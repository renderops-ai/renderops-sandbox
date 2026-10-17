package firecracker

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// ExitInfo describes how a VM's Firecracker process ended.
type ExitInfo struct {
	// Status is the process status as Go prints it: "exit status 1",
	// "signal: killed".
	Status string
	// ExitCode is the exit status, or -1 when a signal ended the process.
	ExitCode int
	// Signaled is true when a signal ended the process (SIGKILL from the
	// kernel OOM killer, for one).
	Signaled bool
	// WaitErr is set when waiting for the process failed for a reason other
	// than its exit status.
	WaitErr string
}

func exitInfoFrom(state *os.ProcessState, waitErr error) ExitInfo {
	info := ExitInfo{ExitCode: -1}
	if state != nil {
		info.Status = state.String()
		info.ExitCode = state.ExitCode()
		if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			info.Signaled = true
		}
	}
	var exitErr *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exitErr) {
		info.WaitErr = waitErr.Error()
		if info.Status == "" {
			info.Status = waitErr.Error()
		}
	}
	return info
}

// SetExitHandler sets the function called when a VM's Firecracker process
// exits while nothing is stopping it (it crashed or was killed, e.g. by the
// OOM killer). It runs on its own goroutine. Set it before VMs are started.
func (f *FireCrackerManager) SetExitHandler(handler func(vmID string, info ExitInfo)) {
	f.exitMu.Lock()
	f.onExit = handler
	f.exitMu.Unlock()
}

func (f *FireCrackerManager) exitHandler() func(string, ExitInfo) {
	f.exitMu.RLock()
	defer f.exitMu.RUnlock()
	return f.onExit
}

// watch waits for the VM's Firecracker process in the background: the one
// place that calls cmd.Wait for a running VM. When the process ends while no
// teardown is stopping it, the exit handler is told.
func (f *FireCrackerManager) watch(vm *MicroVM) {
	if vm.Process == nil || vm.Process.Process == nil {
		return
	}
	cmd := vm.Process
	exited := make(chan struct{})
	vm.exited = exited
	go func() {
		err := cmd.Wait()
		vm.exitInfo = exitInfoFrom(cmd.ProcessState, err)
		close(exited)
		if vm.stopping.Load() {
			return
		}
		slog.Warn("firecracker process exited unexpectedly",
			"vm_id", vm.ID, "status", vm.exitInfo.Status, "wait_err", vm.exitInfo.WaitErr)
		if handler := f.exitHandler(); handler != nil {
			handler(vm.ID, vm.exitInfo)
		}
	}()
}

// stopProcess kills the VM's Firecracker process on purpose and waits for it,
// so the exit is not reported as a crash.
func stopProcess(vm *MicroVM) {
	if vm.Process == nil || vm.Process.Process == nil {
		return
	}
	vm.stopping.Store(true)
	if err := vm.Process.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		slog.Warn("kill firecracker process", "vm_id", vm.ID, "err", err)
	}
	if vm.exited != nil {
		<-vm.exited
		return
	}
	// Not watched (never fully started): reap it here. The error is the
	// process's kill status, which is expected.
	_ = vm.Process.Wait()
}

// WaitExited reports whether the VM's Firecracker process has exited, waiting
// up to timeout for it to be reaped. A VM that is not watched reads as running.
func (vm *MicroVM) WaitExited(timeout time.Duration) (ExitInfo, bool) {
	if vm == nil || vm.exited == nil {
		return ExitInfo{}, false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-vm.exited:
		return vm.exitInfo, true
	case <-timer.C:
		return ExitInfo{}, false
	}
}
