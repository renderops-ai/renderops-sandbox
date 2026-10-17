package firecracker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"backend/internal/writabledisk"

	"github.com/google/uuid"
)

type VMState string

const (
	VMStateCreated   VMState = "created"
	VMStateBooting   VMState = "booting"
	VMStateRunning   VMState = "running"
	VMStateStopped   VMState = "stopped"
	VMStateDestroyed VMState = "destroyed"
)

// defaultWritableDiskMB sizes the per-VM writable upper disk when a VMConfig leaves
// both DiskMB and DiskGB unset. The upper layer is disk-backed (not tmpfs) so heavy
// installs land on disk, not RAM. Fixed at build time, uniform across restores.
const defaultWritableDiskMB = 10 * 1024

type VMConfig struct {
	VCPUCount  int
	MemSizeMiB int
	// Per-VM writable upper disk (/dev/vdb) size. DiskMB wins when > 0 (allows sub-GB,
	// e.g. the micro benchmark); otherwise DiskGB*1024; if both 0, defaultWritableDiskMB.
	DiskMB     int
	DiskGB     int // legacy whole-GB size; DiskMB takes precedence when set
	Timeout    time.Duration
	KernelPath string
	RootfsPath string
	InitrdPath string // initramfs for OverlayFS setup; empty = no initramfs
	BootArgs   string
}

type VsockConfig struct {
	GuestCID int    `json:"guest_cid"`
	UDSPath  string `json:"uds_path"`
}

type MicroVM struct {
	ID               string
	Config           VMConfig
	State            VMState
	VsockPath        string
	SocketPath       string
	WritableDiskPath string // per-VM writable upper disk (/dev/vdb) in OverlayFS mode; "" otherwise
	TapName          string // host TAP device name, empty if network setup failed
	Slot             int    // network slot index (>= 0), or -1 for cold-boot VMs
	CreatedAt        time.Time
	Process          *exec.Cmd
	Stdout           *bytes.Buffer
	Stderr           *bytes.Buffer

	// Device names baked into the snapshot this VM was restored from. Firecracker
	// carries them forward into any snapshot taken of this VM, so they can differ
	// from the current template's after a template rebuild. Empty for cold boots.
	SnapshotVsockPath string
	SnapshotTapName   string

	// Guest memory file this VM was restored from, or its last pause snapshot. Restored
	// VMs track dirty pages, so SnapshotMemory writes only what changed since then. Empty
	// when unknown (cold boot, failed snapshot): the next snapshot is a full one.
	MemBasePath   string
	memBaseShared bool // MemBasePath is a template's, shared by many VMs
	trackDirty    bool // restored with Firecracker dirty-page tracking on

	// Process exit tracking (see watch in exit.go). exited is closed once the
	// process has been reaped; exitInfo is valid after that. stopping is set
	// before an intentional kill, so that exit is not reported as a crash.
	exited   chan struct{}
	exitInfo ExitInfo
	stopping atomic.Bool
}

type NetworkInterface struct {
	IfaceID     string `json:"iface_id"`
	GuestMAC    string `json:"guest_mac"`
	HostDevName string `json:"host_dev_name"`
}

type BootSource struct {
	KernelImagePath string `json:"kernel_image_path"`
	BootArgs        string `json:"boot_args"`
	InitrdPath      string `json:"initrd_path,omitempty"`
}

type Drive struct {
	DriveId      string `json:"drive_id"`
	PathOnHost   string `json:"path_on_host"`
	IsRootDevice bool   `json:"is_root_device"`
	IsReadOnly   bool   `json:"is_read_only"`
}

type MachineConfig struct {
	VCPUCount  int `json:"vcpu_count"`
	MemSizeMib int `json:"mem_size_mib"`
}

type FirecrackerConfig struct {
	BootSource    BootSource    `json:"boot-source"`
	Drives        []Drive       `json:"drives"`
	MachineConfig MachineConfig `json:"machine-config"`
}

type SnapshotTemplate struct {
	SnapPath         string // VM + device state file
	MemPath          string // guest RAM file
	RootfsPath       string // shared read-only lower drive, re-pointed on each restore
	WritableDiskPath string // golden writable upper disk at snapshot time; reflink-cloned per restore
	VsockPath        string // vsock UDS path baked into snapshot — renamed after each restore
	TapName          string // host TAP name baked into snapshot — recreated before each restore
}

type SnapshotCreateRequest struct {
	SnapshotType string `json:"snapshot_type"` // "Full"
	SnapshotPath string `json:"snapshot_path"`
	MemFilePath  string `json:"mem_file_path"`
}

type SnapshotLoadRequest struct {
	SnapshotPath        string `json:"snapshot_path"`
	MemFilePath         string `json:"mem_file_path"`
	EnableDiffSnapshots bool   `json:"enable_diff_snapshots"`
	ResumeVM            bool   `json:"resume_vm"`
}

type VMStateChange struct {
	State string `json:"state"`
}

// SlotPool manages pre-created network slots (netns + TAP + veth + nftables)
// set up during worker bootstrap. Each slot is a self-contained network
// environment that a restored VM can use without guest-side reconfiguration.
type SlotPool struct {
	slots chan int
}

func newSlotPool(count int) *SlotPool {
	sp := &SlotPool{slots: make(chan int, count)}
	for i := 0; i < count; i++ {
		sp.slots <- i
	}
	return sp
}

func (s *SlotPool) Acquire(ctx context.Context) (int, error) {
	select {
	case slot := <-s.slots:
		return slot, nil
	case <-ctx.Done():
		return -1, fmt.Errorf("context cancelled waiting for network slot: %w", ctx.Err())
	}
}

func (s *SlotPool) Release(slot int) {
	s.slots <- slot
}

type FireCrackerManager struct {
	SocketDir  string
	AssetsPath string
	BinaryPath string
	Vms        map[string]*MicroVM
	mu         sync.RWMutex
	nextCID    atomic.Uint32
	nextNetIdx atomic.Uint32 // for TAP subnet allocation used by cold-boot VMs
	restoreMu  sync.Mutex    // serializes snapshot restores — prevents vsock path conflicts
	slotPool   *SlotPool     // pre-created network slots from server.sh
	// Provisioning admission gate. There is one host-level budget because all pools share
	// one physical CPU; provisioning (Firecracker spawn + memory page-in + vCPU resume)
	// is CPU-heavy, and too many in parallel saturate the host and push the API-socket
	// bring-up past its 5s wait, forcing cold-boot fallbacks.
	provisionSlots chan struct{}
	// Non-root uid/gid that Firecracker VMM children drop to via setpriv. 0 = run as
	// root (disabled). The backend itself stays root for netns/iptables/cgroup/mounts.
	FCUid         int
	FCGid         int
	writableDisks writabledisk.Store
	memoryBases   memoryBaseCache // reflinkable copies of template memory, see memdiff.go

	exitMu sync.RWMutex
	onExit func(vmID string, info ExitInfo) // see SetExitHandler
}

type VMManager interface {
	Create(ctx context.Context, cfg VMConfig) (*MicroVM, error)
	Boot(ctx context.Context, vmID string) error
	Stop(ctx context.Context, vmID string) error
	Destroy(ctx context.Context, vmID string) error
	Snapshot(ctx context.Context, vmID string, snapPath, memPath string) error
	LoadFromSnapshot(ctx context.Context, cfg VMConfig, tmpl *SnapshotTemplate) (*MicroVM, error)
}

func NewFirecrackerManager(socketDir, assetsPath, binaryPath string, writableDisks writabledisk.Store, slotCount, maxConcurrentProvisions, fcUid, fcGid int) *FireCrackerManager {
	if writableDisks == nil {
		panic("writable disk store is required")
	}
	if maxConcurrentProvisions < 1 {
		maxConcurrentProvisions = 1
	}
	return &FireCrackerManager{
		SocketDir:      socketDir,
		AssetsPath:     assetsPath,
		BinaryPath:     binaryPath,
		Vms:            make(map[string]*MicroVM),
		slotPool:       newSlotPool(slotCount),
		provisionSlots: make(chan struct{}, maxConcurrentProvisions),
		FCUid:          fcUid,
		FCGid:          fcGid,
		writableDisks:  writableDisks,
	}
}

func (f *FireCrackerManager) WritableDiskRoot() string { return f.writableDisks.Root() }

func (f *FireCrackerManager) ListWritableDisks(ctx context.Context) ([]string, error) {
	return f.writableDisks.List(ctx)
}

func (f *FireCrackerManager) DeleteWritableDisk(ctx context.Context, path string) error {
	return f.writableDisks.Delete(ctx, path)
}

func (f *FireCrackerManager) acquireProvision(ctx context.Context) error {
	select {
	case f.provisionSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for provision slot: %w", ctx.Err())
	}
}

func (f *FireCrackerManager) releaseProvision() { <-f.provisionSlots }

// allocateNetwork returns unique TAP/IP values for a new VM.
// Index N → host: 172.16.N.1/30, guest: 172.16.N.2/30, tap: fctapN
// Supports up to 256 concurrent VMs (one /30 subnet each).
func (f *FireCrackerManager) allocateNetwork() (tapName, hostIP, guestIP, mac string) {
	idx := int(f.nextNetIdx.Add(1) - 1)
	tapName = fmt.Sprintf("fctap%d", idx)
	hostIP = fmt.Sprintf("172.16.%d.1", idx)
	guestIP = fmt.Sprintf("172.16.%d.2", idx)
	mac = fmt.Sprintf("AA:FC:00:00:%02X:%02X", idx>>8, idx&0xFF)
	return
}

// createTAP creates a host-side TAP device and assigns it the given /30 host IP.
func createTAP(tapName, hostIP string) error {
	if out, err := exec.Command("ip", "tuntap", "add", tapName, "mode", "tap").CombinedOutput(); err != nil {
		return fmt.Errorf("create TAP %s: %w: %s", tapName, err, out)
	}
	if out, err := exec.Command("ip", "addr", "add", hostIP+"/30", "dev", tapName).CombinedOutput(); err != nil {
		exec.Command("ip", "link", "delete", tapName).Run()
		return fmt.Errorf("assign IP to TAP %s: %w: %s", tapName, err, out)
	}
	if out, err := exec.Command("ip", "link", "set", tapName, "up").CombinedOutput(); err != nil {
		exec.Command("ip", "link", "delete", tapName).Run()
		return fmt.Errorf("bring up TAP %s: %w: %s", tapName, err, out)
	}
	return nil
}

func (f *FireCrackerManager) getSocketPath(vmID string) string {
	return filepath.Join(f.SocketDir, fmt.Sprintf("%s.sock", vmID))
}

// SetSlotEgress controls internet egress for a slot by toggling its host veth in the
// nftables `blocked_veths` set (defined in renderops-start.sh). Present = egress blocked
// (the forward chain drops it in both directions); absent = allowed. vsock host↔guest
// control is unaffected (it's not IP). Atomic, O(1). Idempotent: deleting an absent element
// or adding a present one is fine. No-op for cold-boot VMs (slot < 0, no slot).
func (f *FireCrackerManager) SetSlotEgress(slot int, allow bool) error {
	if slot < 0 {
		return nil
	}
	elem := fmt.Sprintf("{ \"veth-fc-%d\" }", slot)
	if allow {
		// remove from blocked_veths; tolerate "No such" when it wasn't blocked
		if out, err := exec.Command("nft", "delete", "element", "inet", "fc", "blocked_veths", elem).CombinedOutput(); err != nil &&
			!strings.Contains(string(out), "No such") {
			return fmt.Errorf("unblock egress veth-fc-%d: %w: %s", slot, err, out)
		}
		return nil
	}
	if out, err := exec.Command("nft", "add", "element", "inet", "fc", "blocked_veths", elem).CombinedOutput(); err != nil {
		return fmt.Errorf("block egress veth-fc-%d: %w: %s", slot, err, out)
	}
	return nil
}

// firecrackerCmd builds the command that launches Firecracker, optionally entering a
// network namespace and optionally dropping root to a non-root uid/gid via setpriv
// (when FCUid > 0). The backend stays root — only the VMM child is unprivileged, so an
// escaped guest lands as an unprivileged user. --init-groups re-derives the user's
// supplementary groups so the dropped process keeps kvm-group access to /dev/kvm.
func (f *FireCrackerManager) firecrackerCmd(netns string, args ...string) *exec.Cmd {
	var argv []string
	if netns != "" {
		argv = append(argv, "ip", "netns", "exec", netns)
	}
	if f.FCUid > 0 {
		// Private mount namespace: the VMM's mounts are isolated from the host and can't
		// propagate. unshare needs CAP_SYS_ADMIN, so it runs (as root) BEFORE setpriv drops
		// privileges. The restore path already gets a mount ns from `ip netns exec`; this
		// also covers the cold-boot path for consistency. Gated on FCUid so FC_RUN_UID
		// toggles the whole non-root + mount-ns hardening together.
		argv = append(argv, "unshare", "--mount", "--propagation", "private")
		argv = append(argv, "setpriv",
			"--reuid", strconv.Itoa(f.FCUid),
			"--regid", strconv.Itoa(f.FCGid),
			"--init-groups")
	}
	argv = append(argv, f.BinaryPath)
	argv = append(argv, args...)
	return exec.Command(argv[0], argv[1:]...)
}

func (f *FireCrackerManager) putJSON(client *http.Client, url string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API call failed: %s - %s", resp.Status, body)
	}
	return nil
}

func (f *FireCrackerManager) patchJSON(client *http.Client, url string, payload interface{}) error {
	data, _ := json.Marshal(payload)
	req, _ := http.NewRequest("PATCH", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PATCH failed: %s - %s", resp.Status, body)
	}
	return nil
}

func (f *FireCrackerManager) Create(ctx context.Context, cfg VMConfig) (*MicroVM, error) {
	vmID := uuid.New().String()
	socketPath := f.getSocketPath(vmID)
	vsockPath := filepath.Join(f.SocketDir, fmt.Sprintf("vsock-%s.sock", vmID))

	var (
		writableDiskPath string
		err              error
	)
	if cfg.InitrdPath == "" {
		// No initramfs: fall back to full rootfs copy (legacy cold boot)
		rootfsCopy := filepath.Join(f.SocketDir, fmt.Sprintf("rootfs-%s.ext4", vmID))
		if err := copyFile(cfg.RootfsPath, rootfsCopy); err != nil {
			return nil, fmt.Errorf("failed to copy rootfs for VM %s: %w", vmID, err)
		}
		cfg.RootfsPath = rootfsCopy
	} else {
		// OverlayFS mode: the writable upper layer lives on a per-VM disk (/dev/vdb),
		// not tmpfs — so installs and user files land on disk, not RAM. Pre-format an
		// empty ext4 sized by DiskMB/DiskGB; the guest init mounts it as the overlay upperdir.
		diskMB := cfg.DiskMB
		if diskMB <= 0 {
			if cfg.DiskGB > 0 {
				diskMB = cfg.DiskGB * 1024
			} else {
				diskMB = defaultWritableDiskMB
			}
		}
		writableDiskPath, err = f.writableDisks.Create(ctx, vmID, diskMB)
		if err != nil {
			return nil, fmt.Errorf("failed to create writable disk for VM %s: %w", vmID, err)
		}
		// Firecracker drops to an unprivileged user (FCUid) and must open this disk
		// read-write; the file was created here as root, so hand it ownership.
		if err := f.chownForFC(writableDiskPath); err != nil {
			_ = f.writableDisks.Delete(ctx, writableDiskPath)
			return nil, fmt.Errorf("chown writable disk for VM %s: %w", vmID, err)
		}
	}

	vm := &MicroVM{
		ID:               vmID,
		Config:           cfg,
		State:            VMStateCreated,
		SocketPath:       socketPath,
		VsockPath:        vsockPath,
		WritableDiskPath: writableDiskPath,
		Slot:             -1, // cold-boot VM, not using slot pool
		CreatedAt:        time.Now(),
		Process:          nil,
	}

	f.mu.Lock()
	f.Vms[vmID] = vm
	f.mu.Unlock()

	return vm, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

// chownForFC hands a backend-created file to the unprivileged user Firecracker drops to,
// so the VMM can open it. A no-op when privilege drop is disabled (FCUid == 0).
func (f *FireCrackerManager) chownForFC(path string) error {
	if f.FCUid <= 0 {
		return nil
	}
	return os.Chown(path, f.FCUid, f.FCGid)
}

func (f *FireCrackerManager) Boot(ctx context.Context, vmID string) error {
	f.mu.RLock()
	vm, exists := f.Vms[vmID]
	f.mu.RUnlock()

	if !exists {
		return fmt.Errorf("VM %s not found", vmID)
	}

	// Cold boot also spawns Firecracker, so it shares the host provisioning bound.
	gerr := f.acquireProvision(ctx)
	if gerr != nil {
		return fmt.Errorf("provision gate: %w", gerr)
	}
	defer f.releaseProvision()

	// Allocate the host-side TAP + IPs for this VM's network interface. The guest's own
	// IP/gateway is baked into the rootfs (/etc/vm-network.env) at build time, so we no
	// longer mount the rootfs here to write it — that runtime mount was the leak source.
	tapName, hostIP, _, mac := f.allocateNetwork()

	cmd := f.firecrackerCmd("", "--api-sock", vm.SocketPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start firecracker: %w", err)
	}

	// Wait up to 5 seconds for socket
	for range 250 {
		_, err := os.Stat(vm.SocketPath)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond) // tight poll: notice readiness fast (250×20ms = 5s max)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", vm.SocketPath)
			},
		},
	}

	if err := f.putJSON(client, "http://localhost/boot-source", BootSource{
		KernelImagePath: vm.Config.KernelPath,
		BootArgs:        vm.Config.BootArgs,
		InitrdPath:      vm.Config.InitrdPath,
	}); err != nil {
		return fmt.Errorf("failed to set boot source: %w", err)
	}

	if vm.Config.InitrdPath != "" {
		// OverlayFS mode: read-only lower drive (/dev/vda) + per-VM writable disk
		// (/dev/vdb) that backs the overlay upper layer. Order matters: lower is
		// attached first so the guest enumerates it as /dev/vda and writable as /dev/vdb.
		if err := f.putJSON(client, "http://localhost/drives/lower", Drive{
			DriveId:      "lower",
			PathOnHost:   vm.Config.RootfsPath,
			IsRootDevice: true,
			IsReadOnly:   true,
		}); err != nil {
			return fmt.Errorf("failed to set lower drive: %w", err)
		}
		if err := f.putJSON(client, "http://localhost/drives/writable", Drive{
			DriveId:      "writable",
			PathOnHost:   vm.WritableDiskPath,
			IsRootDevice: false,
			IsReadOnly:   false,
		}); err != nil {
			return fmt.Errorf("failed to set writable drive: %w", err)
		}
	} else {
		// Legacy mode: single writable rootfs copy
		if err := f.putJSON(client, "http://localhost/drives/rootfs", Drive{
			DriveId:      "rootfs",
			PathOnHost:   vm.Config.RootfsPath,
			IsRootDevice: true,
			IsReadOnly:   false,
		}); err != nil {
			return fmt.Errorf("failed to set drive: %w", err)
		}
	}

	if err := f.putJSON(client, "http://localhost/machine-config", MachineConfig{
		VCPUCount:  vm.Config.VCPUCount,
		MemSizeMib: vm.Config.MemSizeMiB,
	}); err != nil {
		return fmt.Errorf("failed to set machine config: %w", err)
	}

	guestCID := int(f.nextCID.Add(1)) + 2 // CIDs 0,1,2 are reserved; start from 3
	if err := f.putJSON(client, "http://localhost/vsock", VsockConfig{
		GuestCID: guestCID,
		UDSPath:  vm.VsockPath,
	}); err != nil {
		return fmt.Errorf("failed to set vsock: %w", err)
	}

	// Enable virtio-rng entropy device: feeds host entropy into the guest at boot.
	// Required for kernels < 4.19 (our kernel is 4.14) where random.trust_cpu=on
	// is not supported. Without this, Node.js v20+ blocks indefinitely on getrandom()
	// because the guest CRNG never reaches full initialization in nested-virt environments.
	// The seeded CRNG state is captured in the snapshot so restored VMs start ready.
	if err := f.putJSON(client, "http://localhost/entropy", struct{}{}); err != nil {
		slog.Warn("entropy device setup failed, node/python bridges may hang on getrandom()", "vm_id", vmID, "err", err)
	}

	// Create TAP device on the host and wire it into Firecracker.
	// Non-fatal: VM boots without network if TAP setup fails (e.g. no permissions).
	// NETWORK CARD
	if err := createTAP(tapName, hostIP); err != nil {
		slog.Warn("TAP creation failed, VM will boot without network", "tap", tapName, "err", err)
	} else {
		if err := f.putJSON(client, "http://localhost/network-interfaces/eth0", NetworkInterface{
			IfaceID:     "eth0",
			GuestMAC:    mac,
			HostDevName: tapName,
		}); err != nil {
			slog.Warn("failed to configure Firecracker network interface", "tap", tapName, "err", err)
			exec.Command("ip", "link", "delete", tapName).Run()
		} else {
			vm.TapName = tapName
		}
	}

	if err := f.putJSON(client, "http://localhost/actions", map[string]string{"action_type": "InstanceStart"}); err != nil {
		return fmt.Errorf("failed to start instance: %w", err)
	}

	f.mu.Lock()
	vm.Process = cmd
	vm.Stdout = &stdout
	vm.Stderr = &stderr
	vm.State = VMStateRunning
	f.watch(vm)
	f.mu.Unlock()

	return nil
}

func (f *FireCrackerManager) Stop(ctx context.Context, vmID string) error {
	f.mu.RLock()
	vm, exists := f.Vms[vmID]
	f.mu.RUnlock()

	if !exists {
		return fmt.Errorf("VM %s not found", vmID)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", vm.SocketPath)
			},
		},
	}

	if err := f.putJSON(client, "http://localhost/actions", map[string]string{"action_type": "SendCtrlAltDel"}); err != nil {
		return fmt.Errorf("failed to stop instance: %w", err)
	}

	f.mu.Lock()
	vm.State = VMStateStopped
	f.mu.Unlock()
	return nil
}

func (f *FireCrackerManager) Destroy(ctx context.Context, vmID string) error {
	return f.teardown(ctx, vmID, false)
}

// TeardownKeepDisk tears down a VM exactly like Destroy but PRESERVES its writable
// upper disk. Used by auto-pause: the session's state lives on that disk, so it must
// survive while the VM itself (process + slot + cgroup) is released to free the host.
func (f *FireCrackerManager) TeardownKeepDisk(ctx context.Context, vmID string) error {
	return f.teardown(ctx, vmID, true)
}

// teardown stops a VM and releases its host resources. When keepDisk is true the
// per-VM writable upper disk is left on disk (the caller still owns it — e.g. a paused
// session); otherwise it is removed.
func (f *FireCrackerManager) teardown(ctx context.Context, vmID string, keepDisk bool) error {
	f.mu.Lock()
	vm, exists := f.Vms[vmID]
	if !exists {
		f.mu.Unlock()
		return fmt.Errorf("VM %s not found", vmID)
	}
	vm.State = VMStateDestroyed
	delete(f.Vms, vmID)
	f.mu.Unlock()

	stopProcess(vm)

	os.Remove(vm.SocketPath)
	os.Remove(vm.VsockPath)
	if vm.Config.InitrdPath == "" {
		os.Remove(vm.Config.RootfsPath) // legacy: delete the VM-specific rootfs copy
	}
	// OverlayFS mode: lower is the shared rootfs (not deleted); the per-VM writable
	// upper disk is this VM's alone, so remove it — unless the caller is keeping it
	// (a paused session whose state lives on that disk).
	if !keepDisk && vm.WritableDiskPath != "" {
		if err := f.writableDisks.Delete(ctx, vm.WritableDiskPath); err != nil {
			slog.Warn("failed to delete writable disk", "vm_id", vmID, "path", vm.WritableDiskPath, "err", err)
		}
	}

	// Slot-based VMs (snapshot restores): return the slot to the pool so the next
	// restore can reuse the pre-created netns + TAP. Cold-boot VMs (template creation)
	// have Slot == -1 and own their TAP — delete it directly.
	if vm.Slot >= 0 {
		f.slotPool.Release(vm.Slot)
	} else if vm.TapName != "" {
		if out, err := exec.Command("ip", "link", "delete", vm.TapName).CombinedOutput(); err != nil {
			slog.Warn("failed to delete TAP device", "tap", vm.TapName, "err", err, "output", string(out))
		}
	}

	return nil
}

func (f *FireCrackerManager) Snapshot(ctx context.Context, vmID, snapPath, memPath string) error {
	f.mu.RLock()
	vm, exists := f.Vms[vmID]
	f.mu.RUnlock()
	if !exists {
		return fmt.Errorf("VM %s not found", vmID)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", vm.SocketPath)
			},
		},
	}

	if err := f.patchJSON(client, "http://localhost/vm", VMStateChange{State: "Paused"}); err != nil {
		return fmt.Errorf("failed to pause VM: %w", err)
	}

	return f.putJSON(client, "http://localhost/snapshot/create", SnapshotCreateRequest{
		SnapshotType: "Full",
		SnapshotPath: snapPath,
		MemFilePath:  memPath,
	})
}

// ResumeLive reverses Snapshot's vCPU pause without replacing the Firecracker
// process. It is used when a durable checkpoint upload fails before teardown.
func (f *FireCrackerManager) ResumeLive(ctx context.Context, vmID string) error {
	f.mu.RLock()
	vm, exists := f.Vms[vmID]
	f.mu.RUnlock()
	if !exists {
		return fmt.Errorf("VM %s not found", vmID)
	}
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", vm.SocketPath)
			},
		},
	}
	if err := f.patchJSON(client, "http://localhost/vm", VMStateChange{State: "Resumed"}); err != nil {
		return fmt.Errorf("failed to resume VM: %w", err)
	}
	return nil
}

// LoadFromSnapshot restores a fresh pool VM from the golden template, cloning the
// golden writable disk so every VM starts identical but can diverge.
func (f *FireCrackerManager) LoadFromSnapshot(ctx context.Context, cfg VMConfig, tmpl *SnapshotTemplate) (*MicroVM, error) {
	return f.loadSnapshot(ctx, cfg, tmpl, true)
}

// ResumeFromSnapshot restores a paused session from ITS OWN snapshot, reattaching the
// session's existing writable disk in place (no clone) so its state is preserved.
func (f *FireCrackerManager) ResumeFromSnapshot(ctx context.Context, cfg VMConfig, tmpl *SnapshotTemplate) (*MicroVM, error) {
	return f.loadSnapshot(ctx, cfg, tmpl, false)
}

// loadSnapshot is the shared restore path. cloneDisk=true clones tmpl.WritableDiskPath
// into a fresh per-VM disk (pool boot); cloneDisk=false attaches tmpl.WritableDiskPath
// directly and never deletes it on cleanup (paused-session resume).
func (f *FireCrackerManager) loadSnapshot(ctx context.Context, cfg VMConfig, tmpl *SnapshotTemplate, cloneDisk bool) (*MicroVM, error) {
	t0 := time.Now()
	lastPhase := t0
	vmID := uuid.New().String()
	socketPath := f.getSocketPath(vmID)
	vsockPath := filepath.Join(f.SocketDir, fmt.Sprintf("vsock-%s.sock", vmID))
	// cloneDisk: give this VM its own copy of the golden disk. Otherwise reuse the
	// session's existing disk in place (and don't remove it on cleanup).
	writableDiskPath := tmpl.WritableDiskPath

	// Bound concurrent provisioning so the host is not stampeded. Acquire before the
	// network slot so capacity is not held while waiting for CPU admission.
	gerr := f.acquireProvision(ctx)
	if gerr != nil {
		return nil, fmt.Errorf("provision gate: %w", gerr)
	}
	defer f.releaseProvision()

	// Acquire a pre-created network slot. Each slot has its own netns + TAP + veth
	// + iptables rules set up by server.sh. The guest's baked-in gateway (172.16.0.1)
	// matches the TAP's host IP in the slot, so no guest-side reconfiguration is needed.
	slot, err := f.slotPool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire network slot: %w", err)
	}
	nsName := fmt.Sprintf("fc-ns-%d", slot)
	tapSlotName := fmt.Sprintf("fc-tap-%d", slot)

	// Run Firecracker inside the slot's network namespace so it sees the slot's TAP.
	// Unix sockets (API socket, vsock UDS) are filesystem-based and work across netns.
	var stdout, stderr bytes.Buffer
	cmd := f.firecrackerCmd(nsName, "--api-sock", socketPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		f.slotPool.Release(slot)
		return nil, fmt.Errorf("failed to start firecracker: %w", err)
	}

	// Wait for API socket (up to 5s) — per-VM socket, no lock needed
	for range 250 {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond) // tight poll: notice readiness fast (250×20ms = 5s max)
	}
	socketReadyMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()
	slog.Debug("restore: firecracker socket ready", "vm_id", vmID, "ms", time.Since(t0).Milliseconds())

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
	}

	// Rename slot TAP to the template TAP name inside the slot's netns.
	// Firecracker's snapshot has tmpl.TapName baked in — it must find that name.
	// Safe to do before the lock: each slot has its own namespace, no conflict.
	if tmpl.TapName != "" {
		if out, err := exec.Command("ip", "netns", "exec", nsName,
			"ip", "link", "set", tapSlotName, "name", tmpl.TapName).CombinedOutput(); err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			f.slotPool.Release(slot)
			return nil, fmt.Errorf("TAP pre-rename failed in %s: %w: %s", nsName, err, out)
		}
	}

	tapRenameMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()

	// Serialize on vsock path: tmpl.VsockPath is shared across all concurrent restores.
	// The lock covers only the snapshot load → resume → vsock rename window.
	f.restoreMu.Lock()
	lockWaitMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()
	slog.Debug("restore: lock acquired", "vm_id", vmID, "ms", time.Since(t0).Milliseconds())

	restoreCleanup := func() {
		f.restoreMu.Unlock()
		cmd.Process.Kill()
		cmd.Wait()
		// Rename TAP back so the slot is reusable
		exec.Command("ip", "netns", "exec", nsName, "ip", "link", "set", tmpl.TapName, "name", tapSlotName).Run()
		f.slotPool.Release(slot)
		// Only remove a disk we created for this VM (clone path). A resumed session's
		// disk is the caller's state — never delete it on a failed resume.
		if cloneDisk {
			_ = f.writableDisks.Delete(ctx, writableDiskPath)
		}
	}

	// Clean up stale vsock from a previous failed restore
	os.Remove(tmpl.VsockPath)

	// Clone the golden writable upper disk for this VM (pool boot only). It must be a copy
	// of the disk as it was at snapshot time (not a fresh empty one): the resumed guest
	// already has this ext4 mounted as the overlay upper, so its in-memory state must match
	// the on-disk image. Reflink keeps this near-instant on a CoW host filesystem. On a
	// session resume (cloneDisk=false) the session's existing disk is attached in place.
	if cloneDisk {
		writableDiskPath, err = f.writableDisks.Clone(ctx, vmID, tmpl.WritableDiskPath)
		if err != nil {
			restoreCleanup()
			return nil, fmt.Errorf("clone writable disk failed: %w", err)
		}
		if err := f.chownForFC(writableDiskPath); err != nil {
			restoreCleanup()
			return nil, fmt.Errorf("chown writable disk failed: %w", err)
		}
	}

	// Load snapshot paused — Firecracker (in nsName) opens tmpl.TapName inside the netns
	if err := f.putJSON(client, "http://localhost/snapshot/load", SnapshotLoadRequest{
		SnapshotPath:        tmpl.SnapPath,
		MemFilePath:         tmpl.MemPath,
		EnableDiffSnapshots: true, // pause snapshots write only changed memory (memdiff.go)
		ResumeVM:            false,
	}); err != nil {
		restoreCleanup()
		return nil, fmt.Errorf("snapshot load failed: %w", err)
	}
	snapshotLoadMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()

	// Update lower drive — shared template rootfs (read-only)
	if err := f.patchJSON(client, "http://localhost/drives/lower", map[string]string{
		"drive_id":     "lower",
		"path_on_host": tmpl.RootfsPath,
	}); err != nil {
		restoreCleanup()
		return nil, fmt.Errorf("lower drive update failed: %w", err)
	}
	// Re-point the writable upper drive (/dev/vdb) at this VM's per-VM clone before resume.
	if err := f.patchJSON(client, "http://localhost/drives/writable", map[string]string{
		"drive_id":     "writable",
		"path_on_host": writableDiskPath,
	}); err != nil {
		restoreCleanup()
		return nil, fmt.Errorf("writable drive update failed: %w", err)
	}
	drivePatchMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()

	// Resume — Firecracker binds tmpl.VsockPath; guest wakes up with its baked-in IP
	if err := f.patchJSON(client, "http://localhost/vm", VMStateChange{State: "Resumed"}); err != nil {
		restoreCleanup()
		return nil, fmt.Errorf("resume failed: %w", err)
	}
	resumeMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()

	if err := waitForPath(ctx, tmpl.VsockPath, 2*time.Second); err != nil {
		restoreCleanup()
		return nil, fmt.Errorf("vsock did not become ready after resume: %w", err)
	}
	// Rename vsock → unique per-VM path (fd survives rename, Firecracker keeps listening)
	if err := os.Rename(tmpl.VsockPath, vsockPath); err != nil {
		restoreCleanup()
		return nil, fmt.Errorf("vsock rename failed: %w", err)
	}

	f.restoreMu.Unlock()
	postResumeRenameMs := time.Since(lastPhase).Milliseconds()
	lastPhase = time.Now()
	slog.Debug("restore: lock released", "vm_id", vmID, "ms", time.Since(t0).Milliseconds())

	// Rename tmpl.TapName back to the slot name now that the lock is released.
	// The slot TAP keeps its pre-assigned IP (172.16.0.1/30) — no IP add needed.
	if tmpl.TapName != "" {
		exec.Command("ip", "netns", "exec", nsName, "ip", "link", "set", tmpl.TapName, "name", tapSlotName).Run()
	}

	// No configure_network call: the guest's baked-in IP (172.16.0.2) and gateway
	// (172.16.0.1) match the slot TAP's pre-configured address. No vsock round-trip,
	// no virtio-vsock teardown interrupt, no ~4s stall.

	vm := &MicroVM{
		ID:               vmID,
		Config:           cfg,
		State:            VMStateRunning,
		SocketPath:       socketPath,
		VsockPath:        vsockPath,
		WritableDiskPath: writableDiskPath,
		TapName:          tapSlotName,
		Slot:             slot,
		CreatedAt:        time.Now(),
		Process:          cmd,
		Stdout:           &stdout,
		Stderr:           &stderr,

		SnapshotVsockPath: tmpl.VsockPath,
		SnapshotTapName:   tmpl.TapName,
	}
	// Record the rootfs this VM actually runs on. On resume cfg is the manager's
	// default config, and pause saves Config.RootfsPath for the next resume, so a
	// sandbox on another image would come back on the default rootfs.
	vm.Config.RootfsPath = tmpl.RootfsPath
	vm.MemBasePath, vm.memBaseShared, vm.trackDirty = tmpl.MemPath, cloneDisk, true

	f.mu.Lock()
	f.Vms[vmID] = vm
	f.watch(vm)
	f.mu.Unlock()

	slog.Info("snapshot restore timings",
		"vm_id", vmID,
		"slot", slot,
		"firecracker_socket_ms", socketReadyMs,
		"tap_rename_ms", tapRenameMs,
		"restore_lock_wait_ms", lockWaitMs,
		"snapshot_load_ms", snapshotLoadMs,
		"drive_patch_ms", drivePatchMs,
		"resume_ms", resumeMs,
		"post_resume_rename_ms", postResumeRenameMs,
		"finalize_ms", time.Since(lastPhase).Milliseconds(),
		"total_ms", time.Since(t0).Milliseconds(),
	)

	return vm, nil
}

func waitForPath(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for %s", path)
		case <-ticker.C:
		}
	}
}

// TemplateBuildOptions controls what runs inside the build VM before it is
// snapshotted. Warmup runs the Node+Python warm-up (best-effort). Provision runs
// additional shell commands (e.g. "pip install numpy") and — unlike warm-up — a
// provision failure ABORTS the build, so a template never ships missing a package
// it claims to have.
type TemplateBuildOptions struct {
	Warmup    bool
	Provision []string
}

// CreateTemplate builds a warmed (Node+Python) template with no extra provisioning.
// This is the standard build path used by the worker's legacy startup build.
func (f *FireCrackerManager) CreateTemplate(ctx context.Context, cfg VMConfig, snapDir string) (*SnapshotTemplate, error) {
	return f.CreateTemplateWithProvision(ctx, cfg, snapDir, TemplateBuildOptions{Warmup: true})
}

// CreateTemplateWithProvision boots a build VM, optionally warms the runtimes and
// runs provision commands inside it, then snapshots it into a reusable template.
// The build VM is trusted and short-lived (no cgroup limits). Provision commands
// run over vsock and must exit 0 or the build fails.
func (f *FireCrackerManager) CreateTemplateWithProvision(ctx context.Context, cfg VMConfig, snapDir string, opts TemplateBuildOptions) (*SnapshotTemplate, error) {
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return nil, err
	}
	// Firecracker drops to FCUid and writes the snapshot files itself, so the dir it
	// writes into must be owned by that user (the backend runs as root and made it).
	if f.FCUid > 0 {
		if err := os.Chown(snapDir, f.FCUid, f.FCGid); err != nil {
			return nil, fmt.Errorf("chown snapshot dir %s: %w", snapDir, err)
		}
	}

	// Boot a normal VM
	vm, err := f.Create(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := f.Boot(ctx, vm.ID); err != nil {
		f.Destroy(ctx, vm.ID)
		return nil, err
	}

	// Wait until the guest agent is actually accepting vsock connections.
	vsock := NewVsockClient(vm.VsockPath)
	vsockReady := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ok, _, _ := vsock.Ping(); ok {
			vsockReady = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !vsockReady {
		slog.Error("template VM console output", "stdout", vm.Stdout.String(), "stderr", vm.Stderr.String())
		f.Destroy(ctx, vm.ID)
		return nil, fmt.Errorf("vsock never became ready for template VM")
	}

	if opts.Warmup {
		f.warmupRuntimes(vm, vsock)
	}

	// Provision: run each command inside the build VM. Unlike warm-up, a failure
	// here aborts the build — a template must not ship missing a package it declares.
	if len(opts.Provision) > 0 {
		conn, cerr := vsock.Connect()
		if cerr != nil {
			f.Destroy(ctx, vm.ID)
			return nil, fmt.Errorf("provision: vsock connect for VM %s: %w", vm.ID, cerr)
		}
		for _, cmd := range opts.Provision {
			resp, rerr := vsock.ExecOnConn(conn, cmd, 900)
			if rerr != nil {
				conn.Close()
				f.Destroy(ctx, vm.ID)
				return nil, fmt.Errorf("provision %q transport failed: %w", cmd, rerr)
			}
			if resp.ExitCode != 0 {
				conn.Close()
				f.Destroy(ctx, vm.ID)
				return nil, fmt.Errorf("provision %q failed (exit %d): %s", cmd, resp.ExitCode, strings.TrimSpace(resp.Stderr))
			}
			slog.Info("template provision command ok", "vm_id", vm.ID, "cmd", cmd, "ms", int64(resp.Duration*1000))
		}
		conn.Close()
	}

	// Take snapshot
	snapPath := filepath.Join(snapDir, "template.snap")
	memPath := filepath.Join(snapDir, "template.mem")
	if err := f.Snapshot(ctx, vm.ID, snapPath, memPath); err != nil {
		f.Destroy(ctx, vm.ID)
		return nil, fmt.Errorf("snapshot failed: %w", err)
	}

	// Partial cleanup: kill process and remove sockets but KEEP rootfs copy.
	// Also delete the TAP so it's free to be recreated for each restore.
	rootfsPath := vm.Config.RootfsPath
	writablePath := vm.WritableDiskPath
	tapName := vm.TapName
	vsockPath := vm.VsockPath
	f.mu.Lock()
	vm.State = VMStateDestroyed
	delete(f.Vms, vm.ID)
	f.mu.Unlock()
	stopProcess(vm)
	os.Remove(vm.SocketPath)
	os.Remove(vsockPath)
	if tapName != "" {
		exec.Command("ip", "link", "delete", tapName).Run()
	}
	// rootfsPath and writablePath intentionally NOT removed — restored VMs clone from them

	slog.Info("snapshot template created", "snap", snapPath, "mem", memPath, "writable_disk", writablePath)
	return &SnapshotTemplate{
		SnapPath:         snapPath,
		MemPath:          memPath,
		RootfsPath:       rootfsPath,
		WritableDiskPath: writablePath,
		VsockPath:        vsockPath,
		TapName:          tapName,
	}, nil
}

// warmupRuntimes best-effort warms Node and Python inside a freshly booted build
// VM so restored sandboxes start ready. Failures are logged, not fatal — a runtime
// hiccup should not block a template (mirrors the original CreateTemplate behavior).
func (f *FireCrackerManager) warmupRuntimes(vm *MicroVM, vsock *VsockClient) {
	nodeResp, err := vsock.Execute("1+1", "node", "stateless", 60)
	if err != nil {
		slog.Warn("template node warmup transport failed", "vm_id", vm.ID, "err", err, "vm_console", vm.Stdout.String())
	} else {
		nodeStderr := strings.TrimSpace(nodeResp.Stderr)
		if nodeResp.ExitCode != 0 || strings.Contains(strings.ToLower(nodeStderr), "timeout") {
			slog.Warn("template node warmup failed", "vm_id", vm.ID, "exit_code", nodeResp.ExitCode, "stderr", nodeStderr, "guest_duration_ms", int64(nodeResp.Duration*1000), "vm_console", vm.Stdout.String())
		} else {
			slog.Info("template node warmup succeeded", "vm_id", vm.ID, "guest_duration_ms", int64(nodeResp.Duration*1000))
		}
	}

	pyResp, err := vsock.Execute("pass", "python", "stateful", 60)
	if err != nil {
		slog.Warn("template python warmup transport failed", "vm_id", vm.ID, "err", err, "vm_console", vm.Stdout.String())
	} else {
		pyStderr := strings.TrimSpace(pyResp.Stderr)
		if pyResp.ExitCode != 0 || strings.Contains(strings.ToLower(pyStderr), "timeout") {
			slog.Warn("template python warmup failed", "vm_id", vm.ID, "exit_code", pyResp.ExitCode, "stderr", pyStderr, "guest_duration_ms", int64(pyResp.Duration*1000), "vm_console", vm.Stdout.String())
		} else {
			slog.Info("template python warmup succeeded", "vm_id", vm.ID, "guest_duration_ms", int64(pyResp.Duration*1000))
		}
	}
}
