package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"backend/internal/plane"
)

// Lost sandboxes. A sandbox is "lost" when its VM is gone and nothing can
// resume it: the worker saw its Firecracker process die, or the worker no
// longer holds it (a restart killed it). The row moves to state "lost", its
// placement (host_id, and with it the worker's capacity) is released, and an
// audit event (action "sandbox.lost") records the reason. "lost" is terminal
// like "error": the only operation left is destroy.

const (
	// ReasonNotOnWorker is the lost reason for a sandbox its worker no longer
	// holds, found by reconciling the worker's inventory.
	ReasonNotOnWorker = "not found on worker after restart"

	// reconcileGrace skips rows changed this recently: the inventory may have
	// been taken before a create or a lifecycle change reached the worker.
	reconcileGrace = 2 * time.Minute
	// inFlightTimeout bounds provisioning and resuming, which can take as long
	// as the worker's lifecycle timeout (10m). Younger rows are left alone.
	inFlightTimeout = 15 * time.Minute
)

// workerLostFromStates are the row states a worker's lost report applies to:
// the sandbox had a running VM, or was getting one.
var workerLostFromStates = []string{"active", "resuming", "provisioning"}

// reconcileStates are the row states the inventory reconcile looks at.
var reconcileStates = []string{"active", "paused", "resuming", "provisioning"}

// WorkerInventory is what a worker holds, sent on start and periodically.
type WorkerInventory struct {
	// Sandboxes are the ids of every sandbox the worker runs or keeps paused.
	Sandboxes []string `json:"sandboxes"`
	// Lost are sandboxes the worker lost whose report it has not had
	// acknowledged.
	Lost []LostSandbox `json:"lost,omitempty"`
	// DurableCheckpoints is true when the worker checkpoints paused sandboxes
	// to object storage, so a paused sandbox can outlive its local files.
	DurableCheckpoints bool `json:"durable_checkpoints"`
	// Complete is false when the worker could not recover its paused
	// sandboxes at start, so missing paused rows prove nothing.
	Complete bool `json:"complete"`
}

// LostSandbox is a sandbox a worker lost, with the reason.
type LostSandbox struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// PlacedSandbox is a sandbox row placed on a worker.
type PlacedSandbox struct {
	ID        string
	State     string
	UpdatedAt time.Time
}

// ReconcileResult is what a reconcile changed.
type ReconcileResult struct {
	MarkedLost []string `json:"marked_lost"`
}

// ReportWorkerLost records a sandbox the worker lost. A row that is no longer
// running on that worker (destroyed, paused, placed elsewhere) is left alone.
func (s *Service) ReportWorkerLost(ctx context.Context, workerID, sandboxID, reason string) error {
	workerID = strings.TrimSpace(workerID)
	sandboxID = strings.TrimSpace(sandboxID)
	if !workerIDPattern.MatchString(workerID) {
		return fmt.Errorf("invalid worker id %q", workerID)
	}
	if sandboxID == "" {
		return errors.New("sandbox id is required")
	}
	reason = normalizeLostReason(reason)
	marked, err := s.store.MarkPlacementLost(ctx, sandboxID, workerID, workerLostFromStates, time.Time{}, reason)
	if err != nil && !errors.Is(err, ErrSandboxNotFound) {
		logOrchestrator(ctx, slog.LevelWarn, "worker sandbox lost update failed",
			"worker_id", workerID, "sandbox_id", sandboxID, "reason", reason, "err", err)
		return err
	}
	logOrchestrator(ctx, slog.LevelWarn, "worker reported sandbox lost",
		"worker_id", workerID, "sandbox_id", sandboxID, "reason", reason, "marked", marked)
	return nil
}

// ReconcileWorker applies a worker's inventory: it records the sandboxes the
// worker reports lost, then marks lost the rows placed on the worker that it
// does not hold (see reconcileDecision).
func (s *Service) ReconcileWorker(ctx context.Context, workerID string, inventory WorkerInventory) (ReconcileResult, error) {
	workerID = strings.TrimSpace(workerID)
	if !workerIDPattern.MatchString(workerID) {
		return ReconcileResult{}, fmt.Errorf("invalid worker id %q", workerID)
	}
	var result ReconcileResult
	var failures []error
	held := make(map[string]bool, len(inventory.Sandboxes)+len(inventory.Lost))
	for _, id := range inventory.Sandboxes {
		held[strings.ToLower(strings.TrimSpace(id))] = true
	}
	for _, lost := range inventory.Lost {
		id := strings.ToLower(strings.TrimSpace(lost.ID))
		held[id] = true // handled here, with the worker's own reason
		if err := s.ReportWorkerLost(ctx, workerID, id, lost.Reason); err != nil {
			failures = append(failures, fmt.Errorf("record lost sandbox %s: %w", id, err))
		}
	}

	rows, err := s.store.ListWorkerPlacements(ctx, workerID, reconcileStates)
	if err != nil {
		return result, errors.Join(append(failures, fmt.Errorf("list worker placements: %w", err))...)
	}
	now := s.now().UTC()
	for _, row := range rows {
		if held[strings.ToLower(row.ID)] {
			continue
		}
		lose, why := reconcileDecision(row, now, inventory)
		if !lose {
			logOrchestrator(ctx, slog.LevelInfo, "sandbox missing from worker kept",
				"worker_id", workerID, "sandbox_id", row.ID, "state", row.State, "why", why)
			continue
		}
		// Only if the row has not changed since it was read.
		marked, err := s.store.MarkPlacementLost(ctx, row.ID, workerID, []string{row.State}, row.UpdatedAt, ReasonNotOnWorker)
		if err != nil {
			failures = append(failures, fmt.Errorf("mark sandbox %s lost: %w", row.ID, err))
			continue
		}
		if marked {
			result.MarkedLost = append(result.MarkedLost, row.ID)
			logOrchestrator(ctx, slog.LevelWarn, "sandbox missing from worker marked lost",
				"worker_id", workerID, "sandbox_id", row.ID, "state", row.State, "reason", ReasonNotOnWorker)
		}
	}
	return result, errors.Join(failures...)
}

// reconcileDecision decides whether a row placed on a worker that does not
// hold it is lost. It is not when the row changed within reconcileGrace (the
// inventory may predate it), when it is provisioning or resuming within
// inFlightTimeout, or when it is paused and its state may still exist: in a
// durable checkpoint, or on a worker that could not recover its paused
// sandboxes. The second result says why a row is kept.
func reconcileDecision(row PlacedSandbox, now time.Time, inventory WorkerInventory) (bool, string) {
	age := now.Sub(row.UpdatedAt)
	if age < reconcileGrace {
		return false, "changed recently"
	}
	switch row.State {
	case "active":
		return true, ""
	case "provisioning", "resuming":
		if age < inFlightTimeout {
			return false, "in flight"
		}
		return true, ""
	case "paused":
		if !inventory.Complete {
			return false, "worker did not recover its paused sandboxes"
		}
		if inventory.DurableCheckpoints {
			return false, "paused state may be in a durable checkpoint"
		}
		return true, ""
	default:
		return false, "state not reconciled"
	}
}

// markLostAfterWorker records a lost sandbox the worker answered a lifecycle
// command for. Failures are logged: the worker reports it as well.
func (s *Service) markLostAfterWorker(ctx context.Context, sandboxID, workerID string, lost *plane.LostError) {
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := s.store.MarkPlacementLost(storeCtx, sandboxID, workerID, workerLostFromStates, time.Time{}, normalizeLostReason(lost.Reason)); err != nil {
		logOrchestrator(ctx, slog.LevelWarn, "mark lost sandbox failed",
			"sandbox_id", sandboxID, "worker_id", workerID, "err", err)
	}
}

func normalizeLostReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "VM is gone"
	}
	if len(reason) > 500 {
		reason = reason[:500]
	}
	return reason
}
