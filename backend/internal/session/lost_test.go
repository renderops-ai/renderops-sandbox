package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"backend/internal/cgroup"
	"backend/internal/executor/firecracker"
	"backend/internal/plane"
	"backend/internal/writabledisk"
)

const (
	lostSessionID = "5d8941b1-4f89-5d6c-9354-02809b1bd130"
	lostVMID      = "vm-1"
)

type lostReport struct {
	id, userID, reason string
}

type lostRecorder struct {
	mu      sync.Mutex
	reports []lostReport
	err     error
}

func (r *lostRecorder) hook(id, userID, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, lostReport{id, userID, reason})
	return r.err
}

func (r *lostRecorder) all() []lostReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]lostReport(nil), r.reports...)
}

// writeMemoryEvents writes a cgroup v2 memory.events file with oomKills.
func writeMemoryEvents(t *testing.T, oomKills string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.events")
	content := "low 0\nhigh 0\nmax 1902\noom 1\noom_kill " + oomKills + "\noom_group_kill 0\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// newLostTestManager returns a manager holding one active session on VM
// lostVMID (no real Firecracker process), whose cgroup reads memoryEvents.
func newLostTestManager(t *testing.T, memoryEvents string) (*Manager, *Session, *lostRecorder) {
	t.Helper()
	disks, err := writabledisk.NewFilesystem(t.TempDir(), writabledisk.CloneAuto)
	if err != nil {
		t.Fatal(err)
	}
	vmManager := firecracker.NewFirecrackerManager(t.TempDir(), t.TempDir(), "/bin/true", disks, 4, 4, 0, 0)
	m := &Manager{
		store:       NewStore(10),
		vmManager:   vmManager,
		template:    &firecracker.SnapshotTemplate{},
		pauseDir:    t.TempDir(),
		meterLast:   make(map[string]time.Time),
		meterTotals: make(map[string]MeterSample),
		readOOMKills: func(*Session) (uint64, error) {
			return cgroup.ReadOOMKills(memoryEvents)
		},
	}
	recorder := &lostRecorder{}
	m.SetLostHook(recorder.hook)
	sess := &Session{
		ID:          lostSessionID,
		UserID:      "user-1",
		VM:          &firecracker.MicroVM{ID: lostVMID, Slot: 3},
		State:       StateActive,
		CreatedAt:   time.Now().Add(-time.Hour),
		LastUsed:    time.Now().Add(-time.Hour),
		IdleTimeout: 5 * time.Minute,
		Terminals:   map[string]struct{}{},
	}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}
	m.bindVM(sess.ID, lostVMID)
	return m, sess, recorder
}

func TestVMExitMarksSessionLostWithOOMReason(t *testing.T) {
	m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "1"))
	var revoked []string
	m.AddSlotRevoker(func(id string) { revoked = append(revoked, id) })

	m.handleVMExit(lostVMID, firecracker.ExitInfo{Status: "signal: killed", ExitCode: -1, Signaled: true})

	if _, ok := m.store.Get(sess.ID); ok {
		t.Fatal("lost session is still in the store")
	}
	if sess.State != StateLost || sess.VM != nil {
		t.Fatalf("state=%s vm=%v, want lost without a VM", sess.State, sess.VM)
	}
	if len(revoked) != 1 || revoked[0] != sess.ID {
		t.Fatalf("revoked grants = %v", revoked)
	}
	reports := recorder.all()
	if len(reports) != 1 || reports[0] != (lostReport{sess.ID, "user-1", LostReasonOOM}) {
		t.Fatalf("lost reports = %+v", reports)
	}
	if _, ok := m.sessionForVM(lostVMID); ok {
		t.Fatal("VM still bound to the lost session")
	}

	// Every command now says why, instead of "not found".
	ctx := context.Background()
	for name, err := range map[string]error{
		"pause":  m.Pause(ctx, sess.ID),
		"resume": m.Resume(ctx, sess.ID),
		"exec": func() error {
			_, err := m.Exec(ctx, sess.ID, "true", 5)
			return err
		}(),
	} {
		var lost *plane.LostError
		if !errors.As(err, &lost) || lost.Reason != LostReasonOOM {
			t.Fatalf("%s error = %v, want lost with reason %q", name, err, LostReasonOOM)
		}
	}
	if reason, ok := m.LostReason(sess.ID); !ok || reason != LostReasonOOM {
		t.Fatalf("LostReason = %q, %v", reason, ok)
	}

	// The report went through, so the inventory carries no lost entry.
	held, lost, complete := m.Inventory()
	if len(held) != 0 || len(lost) != 0 || !complete {
		t.Fatalf("inventory held=%v lost=%v complete=%v", held, lost, complete)
	}

	// Destroying a lost sandbox succeeds and forgets it.
	if err := m.Destroy(ctx, sess.ID); err != nil {
		t.Fatalf("destroy lost sandbox: %v", err)
	}
	if _, ok := m.LostReason(sess.ID); ok {
		t.Fatal("lost record kept after destroy")
	}
}

func TestVMExitReasonWithoutOOMKill(t *testing.T) {
	tests := []struct {
		name string
		info firecracker.ExitInfo
		want string
	}{
		{"killed by a signal", firecracker.ExitInfo{Status: "signal: killed", ExitCode: -1, Signaled: true}, "VM crashed (Firecracker signal: killed)"},
		{"non-zero exit", firecracker.ExitInfo{Status: "exit status 1", ExitCode: 1}, "VM crashed (Firecracker exit status 1)"},
		{"clean exit", firecracker.ExitInfo{Status: "exit status 0"}, "VM stopped unexpectedly (guest shut down or rebooted)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "0"))
			m.handleVMExit(lostVMID, tt.info)
			reports := recorder.all()
			if len(reports) != 1 || reports[0].reason != tt.want {
				t.Fatalf("reports = %+v, want reason %q", reports, tt.want)
			}
			if sess.State != StateLost {
				t.Fatalf("state = %s", sess.State)
			}
		})
	}
}

func TestVMExitOfUnknownOrReleasedVMIsIgnored(t *testing.T) {
	m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "1"))
	m.handleVMExit("pool-vm", firecracker.ExitInfo{Status: "signal: killed", Signaled: true})
	if len(recorder.all()) != 0 || sess.State != StateActive {
		t.Fatal("exit of a VM without a session changed the session")
	}
}

// The dev incident: an idle auto-pause found the VM dead. The pause marks the
// session lost (with the OOM reason) instead of failing, and the reaper does
// not try again.
func TestReaperPauseOfDeadVMMarksLostAndStopsRetrying(t *testing.T) {
	m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "1"))
	m.SetAgentBusyGrace(0)
	checks := 0
	m.waitExited = func(vm *firecracker.MicroVM) (firecracker.ExitInfo, bool) {
		checks++
		return firecracker.ExitInfo{Status: "signal: killed", ExitCode: -1, Signaled: true}, true
	}

	now := time.Now()
	m.reapOnce(context.Background(), now)
	if checks != 1 {
		t.Fatalf("death checks after first reap = %d, want 1", checks)
	}
	if sess.State != StateLost {
		t.Fatalf("state = %s, want lost", sess.State)
	}
	reports := recorder.all()
	if len(reports) != 1 || reports[0].reason != LostReasonOOM {
		t.Fatalf("reports = %+v", reports)
	}

	m.reapOnce(context.Background(), now.Add(reaperInterval))
	m.reapOnce(context.Background(), now.Add(2*reaperInterval))
	if checks != 1 || len(recorder.all()) != 1 {
		t.Fatalf("reaper retried the lost sandbox: checks=%d reports=%d", checks, len(recorder.all()))
	}
}

func TestManualPauseOfDeadVMReturnsLostError(t *testing.T) {
	m, sess, _ := newLostTestManager(t, writeMemoryEvents(t, "1"))
	m.waitExited = func(*firecracker.MicroVM) (firecracker.ExitInfo, bool) {
		return firecracker.ExitInfo{Status: "signal: killed", ExitCode: -1, Signaled: true}, true
	}
	err := m.Pause(context.Background(), sess.ID)
	if !errors.Is(err, plane.ErrSandboxLost) {
		t.Fatalf("pause error = %v, want sandbox lost", err)
	}
}

func TestPauseFailureOfLiveVMIsNotLost(t *testing.T) {
	m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "0"))
	m.waitExited = func(*firecracker.MicroVM) (firecracker.ExitInfo, bool) { return firecracker.ExitInfo{}, false }
	err := m.Pause(context.Background(), sess.ID)
	if err == nil || errors.Is(err, plane.ErrSandboxLost) {
		t.Fatalf("pause error = %v, want a plain snapshot failure", err)
	}
	if sess.State != StateActive || len(recorder.all()) != 0 {
		t.Fatalf("state=%s reports=%d", sess.State, len(recorder.all()))
	}
}

func TestFailedLostReportIsCarriedByInventoryUntilAcked(t *testing.T) {
	m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "1"))
	recorder.err = errors.New("orchestrator unreachable")
	m.handleVMExit(lostVMID, firecracker.ExitInfo{Status: "signal: killed", Signaled: true, ExitCode: -1})

	_, lost, _ := m.Inventory()
	if len(lost) != 1 || lost[0] != (LostSandbox{ID: sess.ID, Reason: LostReasonOOM}) {
		t.Fatalf("inventory lost = %+v", lost)
	}
	m.AckLost([]string{sess.ID})
	if _, lost, _ := m.Inventory(); len(lost) != 0 {
		t.Fatalf("acked lost sandbox still in inventory: %+v", lost)
	}
	// Still answered as lost for a while, then forgotten.
	m.pruneLost(time.Now())
	if _, ok := m.LostReason(sess.ID); !ok {
		t.Fatal("lost record pruned right after the ack")
	}
	m.pruneLost(time.Now().Add(lostRetention + time.Second))
	if _, ok := m.LostReason(sess.ID); ok {
		t.Fatal("lost record kept past retention")
	}
}

func TestLoseUnpausedReportsStillRunningSessionsAtShutdown(t *testing.T) {
	m, sess, recorder := newLostTestManager(t, writeMemoryEvents(t, "0"))
	paused := &Session{ID: "paused-1", UserID: "user-1", State: StatePaused}
	if err := m.store.Add(paused); err != nil {
		t.Fatal(err)
	}

	lost := m.LoseUnpaused(context.Background(), LostReasonShutdown)
	if len(lost) != 1 || lost[0] != sess.ID {
		t.Fatalf("lost = %v, want only the active session", lost)
	}
	reports := recorder.all()
	if len(reports) != 1 || reports[0] != (lostReport{sess.ID, "user-1", LostReasonShutdown}) {
		t.Fatalf("reports = %+v", reports)
	}
	if _, ok := m.store.Get(paused.ID); !ok {
		t.Fatal("paused session dropped at shutdown")
	}
}

func TestInventoryIncompleteAfterFailedRecovery(t *testing.T) {
	m := &Manager{store: NewStore(10), recoveryErr: errors.New("unmarshal manifest")}
	if _, _, complete := m.Inventory(); complete {
		t.Fatal("inventory complete although paused sessions were not recovered")
	}
}
