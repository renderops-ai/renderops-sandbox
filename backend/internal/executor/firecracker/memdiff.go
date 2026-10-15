package firecracker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// Pause snapshots write only the guest memory that changed. Restored VMs run with
// Firecracker's dirty-page tracking on, so a Diff snapshot holds just the pages
// written since the VM was loaded (or since its last snapshot). Firecracker writes
// that diff to a scratch file in RAM, where its fsync costs nothing, and the worker
// builds the full memory file the next resume loads: a copy of the file the VM was
// loaded from (a reflink, so instant, on the pause filesystem) with the changed pages
// written over it. The full file is not fsynced; the durable copy is the Blob
// checkpoint.

// diffScratchDir holds Firecracker's diff files for the second it takes to merge them.
const diffScratchDir = "/dev/shm/renderops-pause"

// memoryCopyChunk is the unit sparseCopy skips when it is all zeros.
const memoryCopyChunk = 4 << 20

// SnapshotMemory pauses the VM and writes a resumable snapshot: VM state to snapPath
// and the full guest memory to memPath. A VM with a known base memory file writes only
// the pages that changed; any other VM, or a diff that fails, gets a full snapshot.
// Afterwards memPath is the VM's base, since Firecracker resets dirty tracking at
// every snapshot.
func (f *FireCrackerManager) SnapshotMemory(ctx context.Context, vmID, snapPath, memPath string) error {
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

	if vm.MemBasePath != "" {
		err := f.diffSnapshot(client, vm, snapPath, memPath)
		if err == nil {
			return nil
		}
		slog.Warn("diff snapshot failed, writing a full one", "vm_id", vmID, "err", err)
	}

	start := time.Now()
	vm.MemBasePath = ""
	if err := f.putJSON(client, "http://localhost/snapshot/create", SnapshotCreateRequest{
		SnapshotType: "Full",
		SnapshotPath: snapPath,
		MemFilePath:  memPath,
	}); err != nil {
		return fmt.Errorf("create full snapshot: %w", err)
	}
	if vm.trackDirty {
		vm.MemBasePath, vm.memBaseShared = memPath, false
	}
	slog.Info("pause snapshot written", "vm_id", vmID, "type", "full", "ms", time.Since(start).Milliseconds())
	return nil
}

// diffSnapshot has Firecracker write the changed pages to a scratch file, then builds
// memPath from the VM's base memory file and those pages.
func (f *FireCrackerManager) diffSnapshot(client *http.Client, vm *MicroVM, snapPath, memPath string) error {
	start := time.Now()
	info, err := os.Stat(vm.MemBasePath)
	if err != nil {
		return fmt.Errorf("stat memory base: %w", err)
	}
	scratch, inRAM := f.diffScratch(info.Size(), filepath.Dir(memPath))
	diffPath := filepath.Join(scratch, vm.ID+"-"+uuid.NewString()+".diff")
	defer os.Remove(diffPath)
	if err := f.putJSON(client, "http://localhost/snapshot/create", SnapshotCreateRequest{
		SnapshotType: "Diff",
		SnapshotPath: snapPath,
		MemFilePath:  diffPath,
	}); err != nil {
		return fmt.Errorf("create diff snapshot: %w", err)
	}
	diffTime := time.Since(start)

	buildStart := time.Now()
	build, err := f.buildMemory(vm.MemBasePath, vm.memBaseShared, diffPath, memPath)
	if err != nil {
		return err
	}
	vm.MemBasePath, vm.memBaseShared = memPath, false
	slog.Info("pause snapshot written", "vm_id", vm.ID, "type", "diff",
		"changed_mb", build.changedBytes>>20, "base", build.clone, "scratch_in_ram", inRAM,
		"diff_ms", diffTime.Milliseconds(), "build_ms", time.Since(buildStart).Milliseconds())
	return nil
}

// diffScratch picks where Firecracker writes a diff: RAM when there is room for a diff
// as large as the whole guest memory, otherwise fallback (on disk, where Firecracker's
// fsync makes it slower).
func (f *FireCrackerManager) diffScratch(guestBytes int64, fallback string) (string, bool) {
	if err := os.MkdirAll(diffScratchDir, 0o755); err == nil {
		if f.FCUid > 0 {
			if err := os.Chown(diffScratchDir, f.FCUid, f.FCGid); err != nil {
				slog.Warn("chown diff scratch dir", "dir", diffScratchDir, "err", err)
				return fallback, false
			}
		}
		var fs unix.Statfs_t
		if unix.Statfs(diffScratchDir, &fs) == nil && int64(fs.Bavail)*int64(fs.Bsize) > guestBytes &&
			memAvailable() > guestBytes+1<<30 {
			return diffScratchDir, true
		}
	}
	return fallback, false
}

// memAvailable returns MemAvailable from /proc/meminfo in bytes, or 0 if unknown.
func memAvailable() int64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0
			}
			return kb << 10
		}
	}
	return 0
}

type memoryBuild struct {
	clone        string // how the base was copied: reflink, cached-reflink or copy
	changedBytes int64  // bytes of changed pages written over it
}

// buildMemory writes dst = base with every data range of diff written over it. dst is
// replaced only once complete. shared marks a base used by many VMs (a template),
// which is worth keeping a reflinkable copy of.
func (f *FireCrackerManager) buildMemory(base string, shared bool, diff, dst string) (memoryBuild, error) {
	baseInfo, err := os.Stat(base)
	if err != nil {
		return memoryBuild{}, fmt.Errorf("stat memory base: %w", err)
	}
	diffInfo, err := os.Stat(diff)
	if err != nil {
		return memoryBuild{}, fmt.Errorf("stat memory diff: %w", err)
	}
	if baseInfo.Size() != diffInfo.Size() {
		return memoryBuild{}, fmt.Errorf("memory diff is %d bytes, base %s is %d", diffInfo.Size(), base, baseInfo.Size())
	}

	tmp := dst + ".tmp"
	_ = os.Remove(tmp) // a leftover from an interrupted build; absent normally
	build := memoryBuild{}
	if build.clone, err = f.cloneMemory(base, shared, tmp); err != nil {
		_ = os.Remove(tmp)
		return memoryBuild{}, fmt.Errorf("copy memory base: %w", err)
	}
	if build.changedBytes, err = copyDataRanges(diff, tmp); err != nil {
		_ = os.Remove(tmp)
		return memoryBuild{}, fmt.Errorf("apply memory diff: %w", err)
	}
	// A filesystem that reports holes as data would have copied zeros over unchanged
	// pages. Copying more than the diff has allocated means exactly that.
	if allocated := allocatedBytes(diffInfo); build.changedBytes > allocated+memoryCopyChunk {
		_ = os.Remove(tmp)
		return memoryBuild{}, fmt.Errorf("memory diff reported %d bytes of data but has %d allocated; holes are not reported", build.changedBytes, allocated)
	}
	if f.FCUid > 0 {
		if err := os.Chown(tmp, f.FCUid, f.FCGid); err != nil {
			_ = os.Remove(tmp)
			return memoryBuild{}, fmt.Errorf("set Firecracker ownership on memory file: %w", err)
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return memoryBuild{}, fmt.Errorf("commit memory file: %w", err)
	}
	return build, nil
}

// cloneMemory copies base to dst, by reflink when base is on dst's filesystem. A shared
// base on another filesystem (templates live in /dev/shm or on the root disk) is
// copied next to dst once and reflinked from there afterwards.
func (f *FireCrackerManager) cloneMemory(base string, shared bool, dst string) (string, error) {
	err := reflinkFile(base, dst)
	if err == nil {
		return "reflink", nil
	}
	if shared && errors.Is(err, unix.EXDEV) && f.writableDisks != nil {
		cached, err := f.memoryBases.copyOf(base, filepath.Join(f.WritableDiskRoot(), "memory-bases"))
		if err != nil {
			return "", err
		}
		if err := reflinkFile(cached, dst); err == nil {
			return "cached-reflink", nil
		}
	}
	return "copy", sparseCopy(base, dst)
}

// reflinkFile makes dst a copy-on-write clone of src.
func reflinkFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	cloneErr := cloneFile(out, in)
	closeErr := out.Close()
	if cloneErr != nil {
		_ = os.Remove(dst)
		return cloneErr
	}
	if closeErr != nil {
		_ = os.Remove(dst)
		return closeErr
	}
	return nil
}

// sparseCopy copies src to dst, leaving all-zero chunks as holes.
func sparseCopy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := out.Truncate(info.Size()); err != nil {
		out.Close()
		return err
	}
	zero := make([]byte, memoryCopyChunk)
	buf := make([]byte, memoryCopyChunk)
	for offset := int64(0); offset < info.Size(); offset += memoryCopyChunk {
		n, err := in.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			out.Close()
			return err
		}
		if bytes.Equal(buf[:n], zero[:n]) {
			continue
		}
		if _, err := out.WriteAt(buf[:n], offset); err != nil {
			out.Close()
			return err
		}
	}
	return out.Close()
}

// copyDataRanges writes every data range of the sparse file src into dst at the same
// offset and returns how many bytes it copied. Zeros inside a data range are copied:
// in a diff they are real page contents.
func copyDataRanges(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return 0, err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY, 0)
	if err != nil {
		return 0, err
	}
	var copied int64
	buf := make([]byte, 1<<20)
	for offset := int64(0); offset < info.Size(); {
		start, err := unix.Seek(int(in.Fd()), offset, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break // no data after offset
		}
		if err != nil {
			out.Close()
			return copied, fmt.Errorf("find data: %w", err)
		}
		end, err := unix.Seek(int(in.Fd()), start, unix.SEEK_HOLE)
		if err != nil {
			out.Close()
			return copied, fmt.Errorf("find hole: %w", err)
		}
		end = min(end, info.Size())
		n, err := io.CopyBuffer(io.NewOffsetWriter(out, start), io.NewSectionReader(in, start, end-start), buf)
		copied += n
		if err != nil {
			out.Close()
			return copied, err
		}
		offset = end
	}
	return copied, out.Close()
}

// allocatedBytes is the disk space a file uses, from its stat block count.
func allocatedBytes(info os.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks) * 512
	}
	return info.Size()
}

// memoryBaseCache keeps sparse copies of shared base memory files (templates) on the
// pause filesystem, so pauses can reflink from them. Copies from an earlier worker run
// are removed on first use: templates are rebuilt at startup.
type memoryBaseCache struct {
	mu      sync.Mutex
	cleaned bool
	copies  map[string]string // base path + size + mtime -> copy
}

func (c *memoryBaseCache) copyOf(base, dir string) (string, error) {
	info, err := os.Stat(base)
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s|%d|%d", base, info.Size(), info.ModTime().UnixNano())

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.cleaned {
		if err := os.RemoveAll(dir); err != nil {
			return "", fmt.Errorf("clear memory base cache: %w", err)
		}
		c.cleaned = true
		c.copies = make(map[string]string)
	}
	if cached, ok := c.copies[key]; ok {
		if _, err := os.Stat(cached); err == nil {
			return cached, nil
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	cached := filepath.Join(dir, uuid.NewString()+".mem")
	if err := sparseCopy(base, cached+".tmp"); err != nil {
		_ = os.Remove(cached + ".tmp")
		return "", fmt.Errorf("copy %s: %w", base, err)
	}
	if err := os.Rename(cached+".tmp", cached); err != nil {
		return "", err
	}
	c.copies[key] = cached
	return cached, nil
}
