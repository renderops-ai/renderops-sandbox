package firecracker

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func startSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep: %v", err)
	}
	return cmd
}

func TestWatchReportsUnexpectedExit(t *testing.T) {
	manager := NewFirecrackerManager(t.TempDir(), "./assets", "/bin/true", newTestDiskStore(t), 1, 1, 0, 0)
	exits := make(chan ExitInfo, 1)
	manager.SetExitHandler(func(vmID string, info ExitInfo) {
		if vmID != "vm-oom" {
			t.Errorf("exit handler vm = %q", vmID)
		}
		exits <- info
	})
	vm := &MicroVM{ID: "vm-oom", Process: startSleeper(t)}
	manager.watch(vm)

	// What the OOM killer does.
	if err := vm.Process.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case info := <-exits:
		if !info.Signaled || info.ExitCode != -1 || info.Status != "signal: killed" {
			t.Fatalf("exit info = %+v", info)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exit handler not called")
	}
	if info, exited := vm.WaitExited(time.Second); !exited || !info.Signaled {
		t.Fatalf("WaitExited = %+v, %v", info, exited)
	}
}

func TestStoppedVMIsNotReportedAsCrash(t *testing.T) {
	manager := NewFirecrackerManager(t.TempDir(), "./assets", "/bin/true", newTestDiskStore(t), 1, 1, 0, 0)
	called := make(chan struct{}, 1)
	manager.SetExitHandler(func(string, ExitInfo) { called <- struct{}{} })
	vm := &MicroVM{ID: "vm-stopped", Process: startSleeper(t)}
	manager.watch(vm)

	stopProcess(vm)
	if _, exited := vm.WaitExited(time.Second); !exited {
		t.Fatal("stopped VM not reaped")
	}
	select {
	case <-called:
		t.Fatal("intentional stop reported as a crash")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestUnwatchedVMReadsAsRunning(t *testing.T) {
	if _, exited := (&MicroVM{ID: "vm"}).WaitExited(10 * time.Millisecond); exited {
		t.Fatal("unwatched VM reported exited")
	}
}
