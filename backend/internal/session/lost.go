package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"backend/internal/executor/firecracker"
	"backend/internal/plane"
)

// A sandbox is lost when its Firecracker process exits without the worker
// stopping it (a crash, the kernel OOM killer), or when it is still running
// at shutdown after the drain could not pause it. A lost sandbox cannot be
// resumed: the worker drops it from the store, releases the VM, revokes its
// grants and reports the reason to the orchestrator. Until the orchestrator
// has the report, and for lostRetention after, a lost record answers
// operations on the sandbox with plane.ErrSandboxLost instead of "not found".

const (
	// lostRetention is how long a reported lost record keeps answering
	// operations with the lost error, covering requests that read the
	// sandbox's state before the orchestrator recorded it.
	lostRetention = 10 * time.Minute
	// pauseDeathWait is how long a failed pause waits for the VM's process to
	// be reaped before deciding whether the VM is dead.
	pauseDeathWait = 2 * time.Second

	// LostReasonOOM is the reason for a VM the kernel OOM killer killed.
	LostReasonOOM = "VM crashed (out of memory)"
	// LostReasonShutdown is the reason for a sandbox still running when the
	// worker stopped after failing to pause it.
	LostReasonShutdown = "worker shut down before the sandbox could be paused"
)

// LostHook reports a lost sandbox, with the reason, to the external store.
// It is called once, right after the loss; a failed report is retried through
// the worker inventory (Inventory, AckLost).
type LostHook func(sessionID, userID, reason string) error

type lostRecord struct {
	id         string
	userID     string
	reason     string
	lostAt     time.Time
	reportedAt time.Time // zero until the orchestrator has it
}

// LostSandbox is a sandbox this worker lost, as reported in its inventory.
type LostSandbox struct {
	ID     string
	Reason string
}

// SetLostHook sets the lost-sandbox reporter. Call before serving requests.
func (m *Manager) SetLostHook(hook LostHook) {
	m.lostMu.Lock()
	m.onLost = hook
	m.lostMu.Unlock()
}

// lostErr is the error operations on a lost sandbox return.
func lostErr(sessionID, reason string) error {
	return fmt.Errorf("session %s: %w", sessionID, &plane.LostError{Reason: reason})
}

// missingErr is the error for a session the store does not hold: the lost
// error while a lost record remains, otherwise "not found".
func (m *Manager) missingErr(sessionID string) error {
	if rec, ok := m.lostRecord(sessionID); ok {
		return lostErr(sessionID, rec.reason)
	}
	return fmt.Errorf("session %s not found", sessionID)
}

// LostReason returns why a sandbox was lost, while its lost record remains.
func (m *Manager) LostReason(sessionID string) (string, bool) {
	rec, ok := m.lostRecord(sessionID)
	return rec.reason, ok
}

func (m *Manager) lostRecord(sessionID string) (lostRecord, bool) {
	m.lostMu.Lock()
	defer m.lostMu.Unlock()
	rec, ok := m.lost[sessionID]
	if !ok {
		return lostRecord{}, false
	}
	return *rec, true
}

func (m *Manager) recordLost(sessionID, userID, reason string, at time.Time) {
	m.lostMu.Lock()
	defer m.lostMu.Unlock()
	if m.lost == nil {
		m.lost = make(map[string]*lostRecord)
	}
	if _, exists := m.lost[sessionID]; exists {
		return
	}
	m.lost[sessionID] = &lostRecord{id: sessionID, userID: userID, reason: reason, lostAt: at}
}

// forgetLost drops a lost record (the sandbox was destroyed). It reports
// whether there was one.
func (m *Manager) forgetLost(sessionID string) bool {
	m.lostMu.Lock()
	defer m.lostMu.Unlock()
	_, ok := m.lost[sessionID]
	delete(m.lost, sessionID)
	return ok
}

// reportLost sends a lost record to the LostHook once. A failure leaves the
// record unreported for the inventory to carry.
func (m *Manager) reportLost(sessionID string) {
	m.lostMu.Lock()
	rec, ok := m.lost[sessionID]
	hook := m.onLost
	var id, userID, reason string
	if ok {
		id, userID, reason = rec.id, rec.userID, rec.reason
	}
	m.lostMu.Unlock()
	if !ok {
		return
	}
	if hook != nil {
		if err := hook(id, userID, reason); err != nil {
			slog.Error("report lost sandbox failed; the worker inventory will retry",
				"session_id", id, "reason", reason, "err", err)
			return
		}
	}
	m.AckLost([]string{id})
}

// AckLost marks lost records as received by the orchestrator. They are
// forgotten lostRetention later.
func (m *Manager) AckLost(sessionIDs []string) {
	now := time.Now()
	m.lostMu.Lock()
	defer m.lostMu.Unlock()
	for _, id := range sessionIDs {
		if rec, ok := m.lost[id]; ok && rec.reportedAt.IsZero() {
			rec.reportedAt = now
		}
	}
}

// pruneLost forgets lost records reported more than lostRetention ago.
func (m *Manager) pruneLost(now time.Time) {
	m.lostMu.Lock()
	defer m.lostMu.Unlock()
	for id, rec := range m.lost {
		if !rec.reportedAt.IsZero() && now.Sub(rec.reportedAt) >= lostRetention {
			delete(m.lost, id)
		}
	}
}

// Inventory is what this worker holds, for the orchestrator to reconcile its
// rows against: the ids of every running or paused sandbox, the lost
// sandboxes the orchestrator has not acknowledged, and whether the held set
// is complete (false when paused sessions could not be recovered at start).
func (m *Manager) Inventory() (held []string, lost []LostSandbox, complete bool) {
	for _, sess := range m.store.All() {
		held = append(held, sess.ID)
	}
	sort.Strings(held)
	m.lostMu.Lock()
	for _, rec := range m.lost {
		if rec.reportedAt.IsZero() {
			lost = append(lost, LostSandbox{ID: rec.id, Reason: rec.reason})
		}
	}
	complete = m.recoveryErr == nil
	m.lostMu.Unlock()
	sort.Slice(lost, func(i, j int) bool { return lost[i].ID < lost[j].ID })
	return held, lost, complete
}

// DurableCheckpoints reports whether paused sandboxes are also checkpointed
// to object storage, so their state outlives this worker's local files.
func (m *Manager) DurableCheckpoints() bool {
	return m.checkpoints != nil && m.checkpointReader != nil
}

func (m *Manager) bindVM(sessionID, vmID string) {
	m.vmMu.Lock()
	defer m.vmMu.Unlock()
	if m.vmSessions == nil {
		m.vmSessions = make(map[string]string)
	}
	m.vmSessions[vmID] = sessionID
}

func (m *Manager) unbindVM(vmID string) {
	m.vmMu.Lock()
	delete(m.vmSessions, vmID)
	m.vmMu.Unlock()
}

func (m *Manager) sessionForVM(vmID string) (string, bool) {
	m.vmMu.Lock()
	defer m.vmMu.Unlock()
	id, ok := m.vmSessions[vmID]
	return id, ok
}

// handleVMExit is the Firecracker exit handler: the VM of an active session
// died on its own, so the session is lost.
func (m *Manager) handleVMExit(vmID string, info firecracker.ExitInfo) {
	sessionID, ok := m.sessionForVM(vmID)
	if !ok {
		// A pool VM not yet handed out, or one already released.
		slog.Info("VM without a session exited", "vm_id", vmID, "status", info.Status)
		return
	}
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return
	}
	sess.mu.Lock()
	if sess.State != StateActive || sess.VM == nil || sess.VM.ID != vmID {
		sess.mu.Unlock()
		return
	}
	m.loseLocked(sess, m.deathReason(sess, info))
	sess.mu.Unlock()
	m.reportLost(sessionID)
}

// deathReason explains why the session's VM process ended. The cgroup's
// oom_kill counter tells an OOM kill apart from other signals.
func (m *Manager) deathReason(sess *Session, info firecracker.ExitInfo) string {
	oomKills, err := m.oomKills(sess)
	if err != nil {
		slog.Warn("read VM cgroup memory events", "session_id", sess.ID, "err", err)
	}
	return lostReason(info, oomKills)
}

// vmExited reports whether the VM's process has exited, waiting briefly for
// it to be reaped.
func (m *Manager) vmExited(vm *firecracker.MicroVM) (firecracker.ExitInfo, bool) {
	if m.waitExited != nil {
		return m.waitExited(vm)
	}
	return vm.WaitExited(pauseDeathWait)
}

func (m *Manager) oomKills(sess *Session) (uint64, error) {
	if m.readOOMKills != nil {
		return m.readOOMKills(sess)
	}
	if sess.Cgroup == nil {
		return 0, errors.New("VM has no cgroup")
	}
	return sess.Cgroup.OOMKills()
}

func lostReason(info firecracker.ExitInfo, oomKills uint64) string {
	switch {
	case oomKills > 0:
		return LostReasonOOM
	case info.Status == "":
		return "VM crashed"
	case info.Signaled || info.ExitCode != 0:
		return fmt.Sprintf("VM crashed (Firecracker %s)", info.Status)
	default:
		return "VM stopped unexpectedly (guest shut down or rebooted)"
	}
}

// loseLocked turns an active session into a lost one. Called with sess.mu
// held. The session leaves the store (so the reaper stops acting on it), its
// slot grants are revoked, its VM, cgroup, sockets, disk and checkpoints are
// released, and a lost record takes its place. The caller reports it with
// reportLost after unlocking.
func (m *Manager) loseLocked(sess *Session, reason string) {
	now := time.Now()
	slog.Error("sandbox lost", "session_id", sess.ID, "user_id", sess.UserID, "reason", reason)
	m.recordLost(sess.ID, sess.UserID, reason, now)
	m.stopLogs(sess.ID)
	// Like Destroy: remove and revoke under slotBindMu so a grant cannot be
	// bound to the slot that is about to be released.
	m.slotBindMu.Lock()
	if current, ok := m.store.Get(sess.ID); ok && current == sess {
		m.store.Delete(sess.ID)
	}
	m.revokeSlotGrants(sess.ID)
	m.slotBindMu.Unlock()

	m.meterSession(sess, StateActive, now)
	m.forgetMeter(sess.ID)
	sess.State = StateLost
	sess.Terminals = nil
	if sess.VsockConn != nil {
		if err := sess.VsockConn.Close(); err != nil {
			slog.Debug("close vsock of lost sandbox", "session_id", sess.ID, "err", err)
		}
		sess.VsockConn = nil
	}
	if sess.VM != nil {
		vmID := sess.VM.ID
		if sess.PooledVM != nil && sess.Pool != nil {
			// Destroys the VM (sockets, writable disk, network slot) and its cgroup.
			sess.Pool.Release(sess.PooledVM)
		} else {
			if sess.Cgroup != nil {
				if err := sess.Cgroup.Destroy(); err != nil {
					slog.Warn("destroy cgroup of lost sandbox", "session_id", sess.ID, "err", err)
				}
			}
			if m.vmManager != nil {
				if err := m.vmManager.Destroy(context.Background(), vmID); err != nil {
					slog.Warn("release VM of lost sandbox", "session_id", sess.ID, "vm_id", vmID, "err", err)
				}
			}
		}
		m.unbindVM(vmID)
	}
	sess.VM, sess.PooledVM, sess.Pool, sess.Cgroup = nil, nil, nil, nil
	// Nothing can resume a lost sandbox, and its row leaves this worker, so a
	// later destroy would not reach here: drop its pause files and checkpoints now.
	m.removePauseDirs(sess.ID)
	m.purgeCheckpoints(sess.ID)
}

// LoseUnpaused marks every session still active as lost and reports it. The
// drain calls it after PauseAllActive, before the worker kills the remaining
// VMs, so the orchestrator does not keep those sandboxes active. A session
// whose lock is not free before ctx ends is reported without being torn
// down; Shutdown kills its VM. It returns the ids it reported.
func (m *Manager) LoseUnpaused(ctx context.Context, reason string) []string {
	var lost []string
	for _, sess := range m.store.All() {
		if lockWithin(ctx, &sess.mu) {
			if sess.State != StateActive {
				sess.mu.Unlock()
				continue
			}
			m.loseLocked(sess, reason)
			sess.mu.Unlock()
		} else {
			// Busy past the deadline, so not paused: it dies with the worker.
			m.recordLost(sess.ID, sess.UserID, reason, time.Now())
		}
		m.reportLost(sess.ID)
		lost = append(lost, sess.ID)
	}
	return lost
}

// lockWithin locks mu, giving up when ctx ends.
func lockWithin(ctx context.Context, mu interface{ TryLock() bool }) bool {
	for {
		if mu.TryLock() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
}
