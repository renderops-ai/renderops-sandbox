package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	DefaultTimeout = 15 * time.Second
	MaxTimeout     = 60 * time.Second
)

// request from host
type ExecutionRequest struct {
	Code     string `json:"code"`
	Language string `json:"language"`
	Timeout  int    `json:"timeout"`
	Mode     string `json:"mode"`
}

// response to host
type ExecutionResponse struct {
	Stdout   string  `json:"stdout"`
	Stderr   string  `json:"stderr"`
	ExitCode int     `json:"exit_code"`
	Duration float64 `json:"duration"`
}

// ─── Kernel Bridge ────────────────────────────────────────────────────────────

// KernelBridge wraps a long-lived kernel_bridge.py process for one language.
// Requests are serialized by the session manager (sess.mu) so no extra
// locking is needed here.
type KernelBridge struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       *bufio.Scanner
	bridgeStderr *bytes.Buffer // captures kernel_bridge.py stderr for error reporting
}

func newKernelBridge(language string) (*KernelBridge, error) {
	cmd := exec.Command("/usr/bin/python3", "/usr/local/bin/kernel_bridge.py", language)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	stderrBuf := new(bytes.Buffer)
	cmd.Stderr = io.MultiWriter(log.Writer(), stderrBuf)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start kernel_bridge.py: %w", err)
	}

	log.Printf("kernel bridge started language=%s pid=%d", language, cmd.Process.Pid)

	return &KernelBridge{
		cmd:          cmd,
		stdin:        stdin,
		stdout:       bufio.NewScanner(stdoutPipe),
		bridgeStderr: stderrBuf,
	}, nil
}

func (kb *KernelBridge) execute(code string, timeoutSec int) (*ExecutionResponse, error) {
	req := map[string]interface{}{
		"code":    code,
		"timeout": timeoutSec,
	}
	data, _ := json.Marshal(req)
	data = append(data, '\n')
	log.Printf("kernel bridge execute begin pid=%d timeout=%ds code_len=%d", kb.cmd.Process.Pid, timeoutSec, len(code))

	if _, err := kb.stdin.Write(data); err != nil {
		log.Printf("kernel bridge stdin write failed pid=%d err=%v", kb.cmd.Process.Pid, err)
		return nil, fmt.Errorf("write to bridge: %w", err)
	}
	log.Printf("kernel bridge stdin write ok pid=%d bytes=%d", kb.cmd.Process.Pid, len(data))

	type scanResult struct {
		resp *ExecutionResponse
		err  error
	}
	ch := make(chan scanResult, 1)
	go func() {
		log.Printf("kernel bridge waiting for stdout pid=%d", kb.cmd.Process.Pid)
		if !kb.stdout.Scan() {
			if err := kb.stdout.Err(); err != nil {
				log.Printf("kernel bridge stdout scan error pid=%d err=%v", kb.cmd.Process.Pid, err)
				ch <- scanResult{nil, fmt.Errorf("bridge stdout: %w", err)}
			} else {
				log.Printf("kernel bridge stdout closed pid=%d stderr=%q", kb.cmd.Process.Pid, kb.bridgeStderr.String())
				ch <- scanResult{nil, fmt.Errorf("bridge process closed: %s", kb.bridgeStderr.String())}
			}
			return
		}
		log.Printf("kernel bridge stdout line received pid=%d bytes=%d", kb.cmd.Process.Pid, len(kb.stdout.Bytes()))
		var resp ExecutionResponse
		if err := json.Unmarshal(kb.stdout.Bytes(), &resp); err != nil {
			log.Printf("kernel bridge decode failed pid=%d payload=%q err=%v", kb.cmd.Process.Pid, string(kb.stdout.Bytes()), err)
			ch <- scanResult{nil, fmt.Errorf("decode bridge response: %w", err)}
			return
		}
		ch <- scanResult{&resp, nil}
	}()

	deadline := time.Duration(timeoutSec+5) * time.Second
	select {
	case r := <-ch:
		if r.err == nil && r.resp != nil {
			log.Printf("kernel bridge execute complete pid=%d exit_code=%d duration=%.3fs", kb.cmd.Process.Pid, r.resp.ExitCode, r.resp.Duration)
		}
		return r.resp, r.err
	case <-time.After(deadline):
		if kb.cmd != nil && kb.cmd.Process != nil {
			log.Printf("kernel bridge timeout pid=%d deadline=%v stderr=%q", kb.cmd.Process.Pid, deadline, kb.bridgeStderr.String())
			kb.cmd.Process.Kill()
		}
		return nil, fmt.Errorf("kernel bridge response timeout after %v", deadline)
	}
}

// ─── Kernel Manager ───────────────────────────────────────────────────────────

// kernelLanguages are routed through kernel_bridge.py for persistent state.
var kernelLanguages = map[string]bool{
	"python": true,
}

var (
	kernelsMu sync.Mutex
	kernels   = map[string]*KernelBridge{}
)

func getOrStartKernel(language string) (*KernelBridge, error) {
	kernelsMu.Lock()
	defer kernelsMu.Unlock()

	if kb, ok := kernels[language]; ok {
		slog.Info("kernel: found existing", "lang", language) // ← add this
		return kb, nil
	}
	slog.Info("kernel: starting new", "lang", language) // ← add this

	kb, err := newKernelBridge(language)
	if err != nil {
		return nil, err
	}

	kernels[language] = kb
	return kb, nil
}

func evictKernel(language string) {
	kernelsMu.Lock()
	delete(kernels, language)
	kernelsMu.Unlock()
}

// resetRuntimes tears down every stateful language runtime (all Python kernels, the Node
// bridge, and the bash shell), killing the processes so the next execution starts fresh.
// Called on resume: a snapshot restore can leave ZMQ-based kernels degraded, and for a
// consistent cross-language contract we reset in-memory interpreter state on every pause
// (the filesystem, installed packages, and re-injected env vars still persist).
func resetRuntimes() {
	totalStarted := time.Now()
	log.Printf("runtime reset phase started phase=terminals")
	phaseStarted := time.Now()
	terminals.CloseAll()
	log.Printf("runtime reset phase completed phase=terminals duration_ms=%d", time.Since(phaseStarted).Milliseconds())

	log.Printf("runtime reset phase started phase=python_kernels stage=waiting_for_lock")
	phaseStarted = time.Now()
	kernelsMu.Lock()
	log.Printf("runtime reset phase progress phase=python_kernels stage=lock_acquired wait_ms=%d", time.Since(phaseStarted).Milliseconds())
	phaseStarted = time.Now()
	for lang, kb := range kernels {
		if kb != nil && kb.cmd != nil && kb.cmd.Process != nil {
			kb.cmd.Process.Kill()
		}
		delete(kernels, lang)
	}
	kernelsMu.Unlock()
	log.Printf("runtime reset phase completed phase=python_kernels duration_ms=%d", time.Since(phaseStarted).Milliseconds())

	log.Printf("runtime reset phase started phase=node_bridge stage=waiting_for_lock")
	phaseStarted = time.Now()
	nodeBridgeMu.Lock()
	log.Printf("runtime reset phase progress phase=node_bridge stage=lock_acquired wait_ms=%d", time.Since(phaseStarted).Milliseconds())
	phaseStarted = time.Now()
	if nodeBridge != nil && nodeBridge.cmd != nil && nodeBridge.cmd.Process != nil {
		nodeBridge.cmd.Process.Kill()
	}
	nodeBridge = nil
	nodeBridgeMu.Unlock()
	log.Printf("runtime reset phase completed phase=node_bridge duration_ms=%d", time.Since(phaseStarted).Milliseconds())

	log.Printf("runtime reset phase started phase=shell")
	phaseStarted = time.Now()
	evictShell()
	log.Printf("runtime reset phase completed phase=shell duration_ms=%d", time.Since(phaseStarted).Milliseconds())
	log.Printf("runtime reset completed duration_ms=%d", time.Since(totalStarted).Milliseconds())
}

// ─── Node Bridge ──────────────────────────────────────────────────────────────

// NodeBridge wraps a long-lived node_bridge.js process.
// Same stdin/stdout JSON protocol as KernelBridge.
type NodeBridge struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       *bufio.Scanner
	bridgeStderr *bytes.Buffer
}

func newNodeBridge() (*NodeBridge, error) {
	cmd := exec.Command("/usr/bin/node", "/usr/local/bin/node_bridge.js")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	stderrBuf := new(bytes.Buffer)
	cmd.Stderr = io.MultiWriter(log.Writer(), stderrBuf)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start node_bridge.js: %w", err)
	}

	log.Printf("node bridge started pid=%d", cmd.Process.Pid)

	return &NodeBridge{
		cmd:          cmd,
		stdin:        stdin,
		stdout:       bufio.NewScanner(stdoutPipe),
		bridgeStderr: stderrBuf,
	}, nil
}

func (nb *NodeBridge) execute(code string, timeoutSec int) (*ExecutionResponse, error) {
	req := map[string]interface{}{
		"code":    code,
		"timeout": timeoutSec,
	}
	data, _ := json.Marshal(req)
	data = append(data, '\n')
	log.Printf("node bridge execute begin pid=%d timeout=%ds code_len=%d", nb.cmd.Process.Pid, timeoutSec, len(code))

	if _, err := nb.stdin.Write(data); err != nil {
		log.Printf("node bridge stdin write failed pid=%d err=%v", nb.cmd.Process.Pid, err)
		return nil, fmt.Errorf("write to node bridge: %w", err)
	}
	log.Printf("node bridge stdin write ok pid=%d bytes=%d", nb.cmd.Process.Pid, len(data))

	type scanResult struct {
		resp *ExecutionResponse
		err  error
	}
	ch := make(chan scanResult, 1)
	go func() {
		log.Printf("node bridge waiting for stdout pid=%d", nb.cmd.Process.Pid)
		if !nb.stdout.Scan() {
			if err := nb.stdout.Err(); err != nil {
				log.Printf("node bridge stdout scan error pid=%d err=%v", nb.cmd.Process.Pid, err)
				ch <- scanResult{nil, fmt.Errorf("node bridge stdout: %w", err)}
			} else {
				log.Printf("node bridge stdout closed pid=%d stderr=%q", nb.cmd.Process.Pid, nb.bridgeStderr.String())
				ch <- scanResult{nil, fmt.Errorf("node bridge process closed: %s", nb.bridgeStderr.String())}
			}
			return
		}
		log.Printf("node bridge stdout line received pid=%d bytes=%d", nb.cmd.Process.Pid, len(nb.stdout.Bytes()))
		var resp ExecutionResponse
		if err := json.Unmarshal(nb.stdout.Bytes(), &resp); err != nil {
			log.Printf("node bridge decode failed pid=%d payload=%q err=%v", nb.cmd.Process.Pid, string(nb.stdout.Bytes()), err)
			ch <- scanResult{nil, fmt.Errorf("decode node bridge response: %w", err)}
			return
		}
		ch <- scanResult{&resp, nil}
	}()

	deadline := time.Duration(timeoutSec+5) * time.Second
	select {
	case r := <-ch:
		if r.err == nil && r.resp != nil {
			log.Printf("node bridge execute complete pid=%d exit_code=%d duration=%.3fs", nb.cmd.Process.Pid, r.resp.ExitCode, r.resp.Duration)
		}
		return r.resp, r.err
	case <-time.After(deadline):
		if nb.cmd != nil && nb.cmd.Process != nil {
			log.Printf("node bridge timeout pid=%d deadline=%v stderr=%q", nb.cmd.Process.Pid, deadline, nb.bridgeStderr.String())
			nb.cmd.Process.Kill()
		}
		return nil, fmt.Errorf("node bridge response timeout after %v", deadline)
	}
}

var (
	nodeBridgeMu sync.Mutex
	nodeBridge   *NodeBridge
)

func getOrStartNodeBridge() (*NodeBridge, error) {
	nodeBridgeMu.Lock()
	defer nodeBridgeMu.Unlock()

	if nodeBridge != nil {
		slog.Info("node bridge: found existing")
		return nodeBridge, nil
	}
	slog.Info("node bridge: starting new")

	nb, err := newNodeBridge()
	if err != nil {
		return nil, err
	}

	nodeBridge = nb
	return nb, nil
}

func evictNodeBridge() {
	nodeBridgeMu.Lock()
	nodeBridge = nil
	nodeBridgeMu.Unlock()
}

// ─── Execution Router ─────────────────────────────────────────────────────────

func executeCode(req ExecutionRequest) ExecutionResponse {
	startTime := time.Now()

	// node   → persistent node_bridge.js (vm module, no ZMQ)
	// python → persistent Jupyter kernel via kernel_bridge.py
	// bash   → direct exec (new process per call, no state)
	if req.Language == "node" {
		log.Printf("executeCode node branch mode=%s", req.Mode)
		return executeViaNodeBridge(req, startTime)
	}
	if req.Language == "python" {
		if req.Mode == "stateful" {
			return executeViaKernel(req, startTime)
		}
	}
	return executeDirect(req, startTime)
}

func executeViaNodeBridge(req ExecutionRequest, startTime time.Time) ExecutionResponse {
	timeout := int(MaxTimeout.Seconds())
	if req.Timeout > 0 {
		timeout = req.Timeout
		if timeout > int(MaxTimeout.Seconds()) {
			timeout = int(MaxTimeout.Seconds())
		}
	}

	nb, err := getOrStartNodeBridge()
	if err != nil {
		log.Printf("node bridge start failed err=%v", err)
		return ExecutionResponse{
			Stderr:   fmt.Sprintf("failed to start node bridge: %v", err),
			ExitCode: -1,
			Duration: time.Since(startTime).Seconds(),
		}
	}

	resp, err := nb.execute(req.Code, timeout)
	if err != nil {
		evictNodeBridge()
		log.Printf("node bridge execution failed err=%v — evicted", err)
		return ExecutionResponse{
			Stderr:   fmt.Sprintf("node bridge error: %v", err),
			ExitCode: -1,
			Duration: time.Since(startTime).Seconds(),
		}
	}

	resp.Duration = time.Since(startTime).Seconds()
	return *resp
}

func executeViaKernel(req ExecutionRequest, startTime time.Time) ExecutionResponse {
	timeout := int(MaxTimeout.Seconds())
	if req.Timeout > 0 {
		timeout = req.Timeout
		if timeout > int(MaxTimeout.Seconds()) {
			timeout = int(MaxTimeout.Seconds())
		}
	}

	kb, err := getOrStartKernel(req.Language)
	if err != nil {
		log.Printf("kernel start failed language=%s err=%v", req.Language, err)
		return ExecutionResponse{
			Stderr:   fmt.Sprintf("failed to start kernel: %v", err),
			ExitCode: -1,
			Duration: time.Since(startTime).Seconds(),
		}
	}

	resp, err := kb.execute(req.Code, timeout)
	if err != nil {
		// kernel died — evict so next request restarts it
		evictKernel(req.Language)
		log.Printf("kernel execution failed language=%s err=%v — evicted", req.Language, err)
		return ExecutionResponse{
			Stderr:   fmt.Sprintf("kernel error: %v", err),
			ExitCode: -1,
			Duration: time.Since(startTime).Seconds(),
		}
	}

	resp.Duration = time.Since(startTime).Seconds()
	return *resp
}

func executeDirect(req ExecutionRequest, startTime time.Time) ExecutionResponse {
	log.Printf("executeDirect invoked language=%s mode=%s", req.Language, req.Mode)
	var cmd *exec.Cmd

	// Use absolute paths since we're running as PID 1 without proper PATH
	switch req.Language {
	case "bash":
		cmd = exec.Command("/bin/bash", "-c", req.Code)
	case "python":
		cmd = exec.Command("/usr/bin/python3", "-c", req.Code)
	case "node":
		cmd = exec.Command("/usr/bin/node", "-e", req.Code)
	default:
		return ExecutionResponse{
			Stderr:   fmt.Sprintf("unsupported language: %s", req.Language),
			ExitCode: 1,
			Duration: time.Since(startTime).Seconds(),
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = io.MultiWriter(&stdout, outputWriter("stdout"))
	cmd.Stderr = io.MultiWriter(&stderr, outputWriter("stderr"))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return ExecutionResponse{
			Stderr:   fmt.Sprintf("failed to start: %v", err),
			ExitCode: -1,
			Duration: time.Since(startTime).Seconds(),
		}
	}
	log.Printf("executeDirect started language=%s pid=%d timeout_req=%d", req.Language, cmd.Process.Pid, req.Timeout)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	timeout := DefaultTimeout
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Second
		if timeout > MaxTimeout {
			timeout = MaxTimeout
		}
	}

	exitCode := 0
	select {
	case err := <-done:
		log.Printf("executeDirect finished language=%s pid=%d duration_ms=%d err=%v", req.Language, cmd.Process.Pid, time.Since(startTime).Milliseconds(), err)
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				exitCode = -1
			}
		}
	case <-time.After(timeout):
		log.Printf("process timeout after %v, killing...", timeout)
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cmd.Wait()
		log.Printf("executeDirect timed out language=%s pid=%d duration_ms=%d", req.Language, cmd.Process.Pid, time.Since(startTime).Milliseconds())
		return ExecutionResponse{
			Stdout:   stdout.String(),
			Stderr:   stderr.String() + fmt.Sprintf("\n[Timeout: Process killed after %v]", timeout),
			ExitCode: -1,
			Duration: time.Since(startTime).Seconds(),
		}
	}

	return ExecutionResponse{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
		Duration: time.Since(startTime).Seconds(),
	}
}

// ─── Entropy Seeding ─────────────────────────────────────────────────────────

// seedCRNG injects entropy into the kernel's random pool via RNDADDENTROPY
// ioctl. Required in nested-KVM environments (Hyper-V → KVM → Firecracker)
// where virtio-rng fails to deliver entropy through DMA, leaving entropy_avail
// permanently below 128 bits. Without this, Node.js v20+ blocks indefinitely
// in getrandom() during V8 init. Data source is nanosecond timing jitter —
// real unpredictability from CPU scheduling and hypervisor overhead variation.
func seedCRNG() {
	f, err := os.OpenFile("/dev/random", os.O_WRONLY, 0)
	if err != nil {
		log.Printf("seedCRNG: open /dev/random: %v", err)
		return
	}
	defer f.Close()

	// Collect 64 bytes of timing jitter across multiple calls.
	var buf [64]byte
	for i := 0; i < 64; i++ {
		ns := time.Now().UnixNano()
		buf[i] = byte(ns >> (uint(i%8) * 8))
	}

	// struct rnd_pool_info { int entropy_count; int buf_size; __u32 buf[]; }
	// RNDADDENTROPY = _IOW('R', 3, int[2]) = 0x40085203
	type randPoolInfo struct {
		entropyCount int32
		size         int32
		buf          [64]byte
	}
	rpi := randPoolInfo{
		entropyCount: 512, // claim 512 bits — well above the 128 needed for crng_init=2
		size:         64,
		buf:          buf,
	}

	const RNDADDENTROPY = 0x40085203
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		f.Fd(),
		RNDADDENTROPY,
		uintptr(unsafe.Pointer(&rpi)),
	)
	if errno != 0 {
		log.Printf("seedCRNG: RNDADDENTROPY ioctl failed: %v", errno)
		return
	}
	log.Printf("seedCRNG: injected 512 bits into entropy pool")
}

// ─── Vsock Listener ───────────────────────────────────────────────────────────

// fdConn wraps a file descriptor to implement io.Reader/Writer
type fdConn struct {
	fd int
}

func (c *fdConn) Read(p []byte) (n int, err error) {
	n, err = unix.Read(c.fd, p)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (c *fdConn) Write(p []byte) (n int, err error) {
	return unix.Write(c.fd, p)
}

// incomingMessage is a union of all message types the host can send.
// Type == "configure_network" → network reconfiguration after snapshot restore.
// Type == "set_env"           → inject env vars into the persistent shell.
// Type == "exec"              → run a shell command in the persistent bash session.
// Type == "" (default)        → code execution request.
type incomingMessage struct {
	LogSandboxID string            `json:"log_sandbox_id"`
	LogEpoch     string            `json:"log_epoch"`
	LogAfter     uint64            `json:"log_after"`
	Type         string            `json:"type"`
	Code         string            `json:"code"`
	Language     string            `json:"language"`
	Timeout      int               `json:"timeout"`
	Mode         string            `json:"mode"`
	GuestIP      string            `json:"guest_ip"`
	GWIP         string            `json:"gw_ip"`
	Command      string            `json:"command"`
	Env          map[string]string `json:"env"`
	TerminalID   string            `json:"terminal_id"`
	Hostname     string            `json:"hostname"`
	Shell        string            `json:"shell"`
	Columns      uint16            `json:"columns"`
	Rows         uint16            `json:"rows"`
	// Quiet (exec only) keeps the command's output out of the output feed, so
	// platform-internal commands (file reads and writes, agent polls, setup)
	// do not reach the sandbox's logs. The output is still returned. Absent
	// means false: hosts that never send it get today's behavior.
	Quiet bool `json:"quiet"`
}

var terminals = newTerminalManager(maxGuestTerminals)

// handleNetworkConfig reconfigures eth0 using netlink directly — no exec.Command,
// no gratuitous ARP. On bare KVM x86_64, ip addr add triggers a gratuitous ARP
// which causes a softirq storm that starves vsock for ~4.4s. Netlink with
// IFA_F_NODAD|IFA_F_NOPREFIXROUTE suppresses the ARP announcement entirely.
func handleNetworkConfig(conn io.Writer, guestIP, gwIP string) {
	t0 := time.Now()
	log.Printf("configure_network start: guest=%s gw=%s", guestIP, gwIP)

	if err := netlinkAddAddr("eth0", guestIP+"/30"); err != nil {
		log.Printf("configure_network: netlinkAddAddr: %v", err)
	}
	log.Printf("configure_network: after addr add, elapsed=%dms", time.Since(t0).Milliseconds())

	if err := netlinkReplaceDefaultRoute(gwIP); err != nil {
		log.Printf("configure_network: netlinkReplaceDefaultRoute: %v", err)
	}
	log.Printf("configure_network: after route replace, elapsed=%dms", time.Since(t0).Milliseconds())

	if err := os.WriteFile("/etc/resolv.conf", []byte("nameserver 8.8.8.8\n"), 0644); err != nil {
		log.Printf("network config: write resolv.conf: %v", err)
	}

	log.Printf("configure_network done: total=%dms", time.Since(t0).Milliseconds())
	json.NewEncoder(conn).Encode(struct {
		Success bool `json:"success"`
	}{Success: true})
}

// parseIPv4 parses a dotted-decimal IPv4 string into 4 bytes.
// Pure string ops — no "net" package, so the binary stays statically linked.
func parseIPv4(s string) ([4]byte, error) {
	var ip [4]byte
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return ip, fmt.Errorf("invalid IPv4: %s", s)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 255 {
			return ip, fmt.Errorf("invalid octet %q in %s", p, s)
		}
		ip[i] = byte(n)
	}
	return ip, nil
}

// netlinkAddAddr assigns cidr (e.g. "172.16.5.2/30") to iface without sending
// a gratuitous ARP. IFA_F_NODAD suppresses Duplicate Address Detection (and the
// associated ARP/ND announcements that would flood the virtio-net device).
func netlinkAddAddr(iface, cidr string) error {
	slash := strings.LastIndex(cidr, "/")
	if slash < 0 {
		return fmt.Errorf("invalid cidr: %s", cidr)
	}
	ipBytes, err := parseIPv4(cidr[:slash])
	if err != nil {
		return fmt.Errorf("parse ip: %w", err)
	}
	prefixLen, err := strconv.Atoi(cidr[slash+1:])
	if err != nil || prefixLen < 0 || prefixLen > 32 {
		return fmt.Errorf("parse prefix len in %s: %w", cidr, err)
	}

	// Read interface index from sysfs — no "net" package needed.
	ifindexRaw, err := os.ReadFile("/sys/class/net/" + iface + "/ifindex")
	if err != nil {
		return fmt.Errorf("read ifindex for %s: %w", iface, err)
	}
	ifindex, err := strconv.Atoi(strings.TrimSpace(string(ifindexRaw)))
	if err != nil {
		return fmt.Errorf("parse ifindex: %w", err)
	}

	sock, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return fmt.Errorf("netlink socket: %w", err)
	}
	defer unix.Close(sock)

	// Build RTM_NEWADDR message
	msg := make([]byte, unix.SizeofNlMsghdr+unix.SizeofIfAddrmsg)
	hdr := (*unix.NlMsghdr)(unsafe.Pointer(&msg[0]))
	hdr.Type = unix.RTM_NEWADDR
	hdr.Flags = unix.NLM_F_REQUEST | unix.NLM_F_CREATE | unix.NLM_F_REPLACE | unix.NLM_F_ACK
	hdr.Seq = 1

	ifa := (*unix.IfAddrmsg)(unsafe.Pointer(&msg[unix.SizeofNlMsghdr]))
	ifa.Family = unix.AF_INET
	ifa.Prefixlen = uint8(prefixLen)
	ifa.Flags = unix.IFA_F_NODAD // suppress ARP/DAD announcements
	ifa.Index = uint32(ifindex)

	// IFA_LOCAL attribute (the IP address)
	attr := make([]byte, unix.SizeofRtAttr+4)
	rta := (*unix.RtAttr)(unsafe.Pointer(&attr[0]))
	rta.Type = unix.IFA_LOCAL
	rta.Len = uint16(unix.SizeofRtAttr + 4)
	copy(attr[unix.SizeofRtAttr:], ipBytes[:])

	msg = append(msg, attr...)
	hdr = (*unix.NlMsghdr)(unsafe.Pointer(&msg[0]))
	hdr.Len = uint32(len(msg))

	sa := &unix.SockaddrNetlink{Family: unix.AF_NETLINK}
	if err := unix.Sendto(sock, msg, 0, sa); err != nil {
		return fmt.Errorf("sendto: %w", err)
	}

	// Read ACK
	buf := make([]byte, 4096)
	n, _, err := unix.Recvfrom(sock, buf, 0)
	if err != nil {
		return fmt.Errorf("recvfrom: %w", err)
	}
	if n < unix.SizeofNlMsghdr {
		return fmt.Errorf("short netlink response: %d bytes", n)
	}
	resp := (*unix.NlMsghdr)(unsafe.Pointer(&buf[0]))
	if resp.Type == unix.NLMSG_ERROR {
		errno := *(*int32)(unsafe.Pointer(&buf[unix.SizeofNlMsghdr]))
		if errno != 0 {
			return fmt.Errorf("netlink error: %w", syscall.Errno(-errno))
		}
	}
	return nil
}

// netlinkReplaceDefaultRoute sets the default route via gwIP using netlink.
func netlinkReplaceDefaultRoute(gwIP string) error {
	gw, err := parseIPv4(gwIP)
	if err != nil {
		return fmt.Errorf("invalid gateway IP: %w", err)
	}

	sock, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return fmt.Errorf("netlink socket: %w", err)
	}
	defer unix.Close(sock)

	msg := make([]byte, unix.SizeofNlMsghdr+unix.SizeofRtMsg)
	hdr := (*unix.NlMsghdr)(unsafe.Pointer(&msg[0]))
	hdr.Type = unix.RTM_NEWROUTE
	hdr.Flags = unix.NLM_F_REQUEST | unix.NLM_F_CREATE | unix.NLM_F_REPLACE | unix.NLM_F_ACK
	hdr.Seq = 1

	rtm := (*unix.RtMsg)(unsafe.Pointer(&msg[unix.SizeofNlMsghdr]))
	rtm.Family = unix.AF_INET
	rtm.Dst_len = 0 // default route
	rtm.Src_len = 0
	rtm.Tos = 0
	rtm.Table = unix.RT_TABLE_MAIN
	rtm.Protocol = unix.RTPROT_BOOT
	rtm.Scope = unix.RT_SCOPE_UNIVERSE
	rtm.Type = unix.RTN_UNICAST

	// RTA_GATEWAY attribute
	attr := make([]byte, unix.SizeofRtAttr+4)
	rta := (*unix.RtAttr)(unsafe.Pointer(&attr[0]))
	rta.Type = unix.RTA_GATEWAY
	rta.Len = uint16(unix.SizeofRtAttr + 4)
	copy(attr[unix.SizeofRtAttr:], gw[:])

	msg = append(msg, attr...)
	hdr = (*unix.NlMsghdr)(unsafe.Pointer(&msg[0]))
	hdr.Len = uint32(len(msg))

	sa := &unix.SockaddrNetlink{Family: unix.AF_NETLINK}
	if err := unix.Sendto(sock, msg, 0, sa); err != nil {
		return fmt.Errorf("sendto: %w", err)
	}

	buf := make([]byte, 4096)
	n, _, err := unix.Recvfrom(sock, buf, 0)
	if err != nil {
		return fmt.Errorf("recvfrom: %w", err)
	}
	if n < unix.SizeofNlMsghdr {
		return fmt.Errorf("short netlink response: %d bytes", n)
	}
	resp := (*unix.NlMsghdr)(unsafe.Pointer(&buf[0]))
	if resp.Type == unix.NLMSG_ERROR {
		errno := *(*int32)(unsafe.Pointer(&buf[unix.SizeofNlMsghdr]))
		if errno != 0 {
			return fmt.Errorf("netlink error: %w", syscall.Errno(-errno))
		}
	}
	return nil
}

// ─── Persistent Shell Session ─────────────────────────────────────────────────

const shellSentinel = "__EXEC_DONE__"

type ShellSession struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
}

var (
	shellMu     sync.Mutex
	globalShell *ShellSession
)

func newShellSession(env map[string]string) (*ShellSession, error) {
	cmd := exec.Command("/bin/bash")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Build env: inherit current env, then apply overrides
	base := os.Environ()
	for k, v := range env {
		base = append(base, k+"="+v)
	}
	cmd.Env = base

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	cmd.Stderr = log.Writer()

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start bash: %w", err)
	}

	sh := &ShellSession{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewScanner(stdoutPipe),
	}

	if token, ok := env["GITHUB_TOKEN"]; ok && token != "" {
		fmt.Fprintf(sh.stdin, "git config --global url.\"https://%s@github.com/\".insteadOf \"https://github.com/\"\n", token)
	}
	// Set default working directory and configure git identity placeholders
	fmt.Fprintf(sh.stdin, "mkdir -p /workspace && cd /workspace\n")
	fmt.Fprintf(sh.stdin, "export GIT_TERMINAL_PROMPT=0\n") // never prompt for credentials — fail fast
	log.Printf("shell session started pid=%d", cmd.Process.Pid)
	return sh, nil
}

func getOrStartShell(env map[string]string) (*ShellSession, error) {
	shellMu.Lock()
	defer shellMu.Unlock()
	if globalShell != nil {
		return globalShell, nil
	}
	sh, err := newShellSession(env)
	if err != nil {
		return nil, err
	}
	globalShell = sh
	return sh, nil
}

func evictShell() {
	shellMu.Lock()
	if globalShell != nil {
		globalShell.cmd.Process.Kill()
		globalShell = nil
	}
	shellMu.Unlock()
}

type shellResult struct {
	lines    []string
	exitCode int
	err      error
}

// execStderrFile receives a persistent-shell command's stderr. A variable so
// tests can move it.
var execStderrFile = "/tmp/.exec_stderr"

// exec runs command in the persistent shell. Its output is copied into the
// output feed unless quiet is set; either way it is returned.
func (s *ShellSession) exec(command string, timeoutSec int, quiet bool) (stdout, stderr string, exitCode int, err error) {
	stderrFile := execStderrFile

	// Wrap command: redirect stderr to tmp file, print sentinel with exit code when done
	wrapped := fmt.Sprintf("{ %s\n} 2>%s\n__ec=$?; printf '\\n%s%%d\\n' $__ec\n",
		command, stderrFile, shellSentinel)

	if _, writeErr := fmt.Fprint(s.stdin, wrapped); writeErr != nil {
		return "", "", -1, fmt.Errorf("write to shell stdin: %w", writeErr)
	}

	ch := make(chan shellResult, 1)
	go func() {
		var lines []string
		for s.stdout.Scan() {
			line := s.stdout.Text()
			if strings.HasPrefix(line, shellSentinel) {
				code, _ := strconv.Atoi(strings.TrimPrefix(line, shellSentinel))
				ch <- shellResult{lines, code, nil}
				return
			}
			lines = append(lines, line)
			if !quiet {
				outputLogs.write("stdout", []byte(line+"\n"))
			}
		}
		ch <- shellResult{nil, -1, fmt.Errorf("shell process died")}
	}()

	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	select {
	case r := <-ch:
		if r.err != nil {
			return "", "", -1, r.err
		}
		if len(r.lines) > 0 {
			stdout = strings.Join(r.lines, "\n") + "\n"
		}
		stderrBytes, _ := os.ReadFile(stderrFile)
		if !quiet {
			outputLogs.write("stderr", stderrBytes)
		}
		return stdout, string(stderrBytes), r.exitCode, nil
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		return "", "", -1, fmt.Errorf("command timed out after %ds", timeoutSec)
	}
}

func handleExec(conn io.Writer, command string, timeoutSec int, quiet bool) {
	if timeoutSec <= 0 {
		timeoutSec = 30
	}
	sh, err := getOrStartShell(nil)
	if err != nil {
		json.NewEncoder(conn).Encode(ExecutionResponse{
			Stderr:   fmt.Sprintf("failed to start shell: %v", err),
			ExitCode: -1,
		})
		return
	}

	stdout, stderr, exitCode, err := sh.exec(command, timeoutSec, quiet)
	if err != nil {
		evictShell()
		json.NewEncoder(conn).Encode(ExecutionResponse{
			Stderr:   fmt.Sprintf("shell exec error: %v", err),
			ExitCode: -1,
		})
		return
	}

	json.NewEncoder(conn).Encode(ExecutionResponse{
		Stdout:   stdout,
		Stderr:   stderr,
		ExitCode: exitCode,
	})
}

func handleSetEnv(conn io.Writer, env map[string]string) {
	shellMu.Lock()
	defer shellMu.Unlock()

	// Start a fresh shell with the provided env vars
	if globalShell != nil {
		globalShell.cmd.Process.Kill()
		globalShell = nil
	}

	sh, err := newShellSession(env)
	if err != nil {
		json.NewEncoder(conn).Encode(struct {
			Success bool   `json:"success"`
			Error   string `json:"error,omitempty"`
		}{false, err.Error()})
		return
	}
	globalShell = sh
	json.NewEncoder(conn).Encode(struct {
		Success bool `json:"success"`
	}{true})
}

func handleConnection(connFd int) {
	conn := &fdConn{fd: connFd}
	log.Printf("connection accepted fd=%d", connFd)

	for {
		var msg incomingMessage
		if err := json.NewDecoder(conn).Decode(&msg); err != nil {
			log.Printf("connection closed fd=%d: %v", connFd, err)
			return
		}

		if msg.Type == "configure_network" {
			handleNetworkConfig(conn, msg.GuestIP, msg.GWIP)
			continue
		}
		if msg.Type == "set_env" {
			handleSetEnv(conn, msg.Env)
			continue
		}
		if msg.Type == "reset_runtimes" {
			log.Printf("resetting guest runtimes")
			resetRuntimes()
			json.NewEncoder(conn).Encode(struct {
				Success bool `json:"success"`
			}{true})
			log.Printf("guest runtimes reset")
			continue
		}
		if msg.Type == "exec" {
			handleExec(conn, msg.Command, msg.Timeout, msg.Quiet)
			continue
		}
		if msg.Type == "logs_follow" {
			_ = outputLogs.follow(conn, msg.LogSandboxID, msg.LogEpoch, msg.LogAfter)
			return
		}
		if msg.Type == "terminal_open" {
			err := terminals.Open(terminalOpenRequest{
				ID: msg.TerminalID, Hostname: msg.Hostname, Shell: msg.Shell, Columns: msg.Columns, Rows: msg.Rows, Env: msg.Env,
			})
			state := ""
			if err == nil {
				state = "ready"
			}
			json.NewEncoder(conn).Encode(struct {
				Success bool   `json:"success"`
				State   string `json:"state,omitempty"`
				Error   string `json:"error,omitempty"`
			}{Success: err == nil, State: state, Error: errorString(err)})
			continue
		}
		if msg.Type == "terminal_close" {
			err := terminals.Close(msg.TerminalID)
			state := ""
			if err == nil {
				state = "closed"
			}
			json.NewEncoder(conn).Encode(struct {
				Success bool   `json:"success"`
				State   string `json:"state,omitempty"`
				Error   string `json:"error,omitempty"`
			}{Success: err == nil, State: state, Error: errorString(err)})
			continue
		}
		if msg.Type == "terminal_close_all" {
			terminals.CloseAll()
			json.NewEncoder(conn).Encode(struct {
				Success bool `json:"success"`
			}{Success: true})
			continue
		}
		if msg.Type == "terminal_attach" {
			if err := terminals.Attach(conn, msg.TerminalID); err != nil {
				log.Printf("terminal attach failed terminal_id=%s: %v", msg.TerminalID, err)
			}
			return
		}

		req := ExecutionRequest{
			Code:     msg.Code,
			Language: msg.Language,
			Timeout:  msg.Timeout,
			Mode:     msg.Mode,
		}
		log.Printf("execute language=%s code_len=%d", req.Language, len(req.Code))
		resp := executeCode(req)
		// Stateful kernel bridges only expose output once a cell finishes.
		if req.Language == "node" || (req.Mode == "stateful" && req.Language == "python") {
			outputLogs.write("stdout", []byte(resp.Stdout))
			outputLogs.write("stderr", []byte(resp.Stderr))
		}
		log.Printf("done exit_code=%d duration=%.2fs", resp.ExitCode, resp.Duration)

		if err := json.NewEncoder(conn).Encode(resp); err != nil {
			log.Printf("encode response: %v", err)
			return
		}
		log.Printf("response encoded fd=%d", connFd)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// listenVsock opens the host-facing listener. It and every accepted connection are
// close-on-exec: otherwise every process the agent starts (shell, kernels, user
// commands) holds the listener and the live host connection, and a connection the
// agent closes stays open in the guest.
func listenVsock() (int, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	addr := &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: 52}
	if err = unix.Bind(fd, addr); err != nil {
		unix.Close(fd)
		return -1, fmt.Errorf("bind: %w", err)
	}
	if err = unix.Listen(fd, 128); err != nil {
		unix.Close(fd)
		return -1, fmt.Errorf("listen: %w", err)
	}
	return fd, nil
}

func ensurePTYFilesystem() error {
	if err := os.MkdirAll("/dev/pts", 0o755); err != nil {
		return fmt.Errorf("create /dev/pts: %w", err)
	}
	if _, err := os.Stat("/dev/pts/ptmx"); os.IsNotExist(err) {
		if err := unix.Mount("devpts", "/dev/pts", "devpts", 0, "newinstance,ptmxmode=0666,mode=0620,gid=5"); err != nil && err != unix.EBUSY {
			return fmt.Errorf("mount devpts: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("stat /dev/pts/ptmx: %w", err)
	}
	if _, err := os.Lstat("/dev/ptmx"); os.IsNotExist(err) {
		if err := os.Symlink("pts/ptmx", "/dev/ptmx"); err != nil {
			return fmt.Errorf("link /dev/ptmx: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("stat /dev/ptmx: %w", err)
	}
	return nil
}

// readNetworkConfig reads guest IP and gateway from /etc/vm-network.env,
// which the host writes into the rootfs copy before, Firecracker starts.
func readNetworkConfig() (guestIP, gwIP string) {
	data, err := os.ReadFile("/etc/vm-network.env")
	if err != nil {
		log.Printf("no /etc/vm-network.env: %v", err)
		return "", ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "GUESTIP":
			guestIP = parts[1]
		case "GWIP":
			gwIP = parts[1]
		}
	}
	return
}

// standardPath is the PATH a normal Linux login gets. As PID 1 the agent inherits
// no PATH from the kernel, so without this commands fall back to a short default
// that lacks /usr/local/bin, where npm -g, pip and the agent CLIs install.
const standardPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// ensurePath gives every command the agent starts (exec, terminals, bridges) a
// PATH that includes /usr/local/bin; they all inherit the agent's environment.
func ensurePath() {
	path := os.Getenv("PATH")
	if strings.Contains(":"+path+":", ":/usr/local/bin:") {
		return
	}
	if path == "" {
		path = standardPath
	} else {
		path = "/usr/local/sbin:/usr/local/bin:" + path
	}
	if err := os.Setenv("PATH", path); err != nil {
		log.Printf("warning: set PATH: %v", err)
	}
}

func main() {
	logToKernel()
	ensurePath()
	// Seed the kernel CRNG immediately. In nested-KVM (Azure/Hyper-V), the
	// virtio-rng device fails to deliver entropy via DMA so entropy_avail
	// stays at ~38 bits forever — below the 128 bits getrandom() requires.
	// This must run before any bridge process starts.
	seedCRNG()
	if err := ensurePTYFilesystem(); err != nil {
		log.Printf("warning: PTY filesystem unavailable: %v", err)
	}

	// bring up loopback interface so 127.0.0.1 works inside the VM
	// required for ZMQ (jupyter kernel ↔ kernel_bridge.py communicate via TCP localhost)
	// 1. assign 127.0.0.1/8 to lo (may already exist, ignore error)
	exec.Command("/sbin/ip", "addr", "add", "127.0.0.1/8", "dev", "lo").Run()
	// 2. bring the interface up
	if out, err := exec.Command("/sbin/ip", "link", "set", "lo", "up").CombinedOutput(); err != nil {
		log.Printf("warning: failed to bring up lo: %v — %s", err, out)
	} else {
		log.Println("loopback interface up (127.0.0.1)")
	}

	// bring up eth0 for outbound internet access (pip install, etc.)
	// IPs are written into /etc/vm-network.env by the host before boot.
	guestIP, gwIP := readNetworkConfig()
	if guestIP != "" && gwIP != "" {
		exec.Command("/sbin/ip", "addr", "add", guestIP+"/30", "dev", "eth0").Run()
		exec.Command("/sbin/ip", "link", "set", "eth0", "up").Run()
		exec.Command("/sbin/ip", "route", "add", "default", "via", gwIP).Run()
		if err := os.WriteFile("/etc/resolv.conf", []byte("nameserver 8.8.8.8\n"), 0644); err != nil {
			log.Printf("warning: failed to write resolv.conf: %v", err)
		}
		log.Printf("network up: guest=%s gateway=%s", guestIP, gwIP)
	} else {
		log.Println("no network config found, eth0 not configured")
	}

	fd, err := listenVsock()
	if err != nil {
		log.Fatalf("failed to listen on vsock: %v", err)
	}

	log.Println("guest agent ready, waiting for connections...")

	for {
		connFd, _, err := unix.Accept4(fd, unix.SOCK_CLOEXEC)
		if err != nil {
			log.Printf("accept failed (%v) — reinitializing vsock listener", err)
			unix.Close(fd)
			for {
				fd, err = listenVsock()
				if err == nil {
					break
				}
				log.Printf("reinit failed: %v — retrying in 20ms", err)
				time.Sleep(20 * time.Millisecond)
			}
			fmt.Println("VSOCK_READY")
			log.Println("vsock listener reinitialized")
			continue
		}

		go func(connectionFD int) {
			handleConnection(connectionFD)
			unix.Close(connectionFD)
		}(connFd)
	}
}
