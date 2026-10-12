package platform

import (
	"context"
	"errors"
	"fmt"
	"time"

	"backend/internal/orchestrator"
	"backend/internal/plane"

	"github.com/jackc/pgx/v5"
)

var _ orchestrator.Store = (*Client)(nil)

// bestOfKSampleSize bounds scheduler work while avoiding the load imbalance of
// selecting a single random worker. This follows the best-of-K pattern with K=3.
const bestOfKSampleSize = 3

// FailStaleUnplacedSandboxes closes scheduling rows left behind when the
// control plane failed before the orchestrator reserved a worker. Rows with a
// host assignment are intentionally excluded because their worker must be
// reconciled before capacity can be released safely.
func (c *Client) FailStaleUnplacedSandboxes(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("stale scheduling threshold must be positive")
	}
	tag, err := c.pool.Exec(ctx, `
		UPDATE sandboxes
		SET state='error',
		    updated_at=now()
		WHERE state='scheduling'
		  AND host_id IS NULL
		  AND updated_at < $1`,
		time.Now().UTC().Add(-olderThan),
	)
	if err != nil {
		return 0, fmt.Errorf("fail stale unplaced sandboxes: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (c *Client) RegisterWorker(ctx context.Context, worker orchestrator.WorkerRegistration, heartbeatAt time.Time) error {
	_, err := c.pool.Exec(ctx, `
		INSERT INTO worker_hosts (
			id, endpoint, pool, status, draining,
			allocatable_vcpus, allocatable_memory_mb, allocatable_disk_gb,
			max_sandboxes, supported_images, last_heartbeat_at, updated_at
		)
		VALUES ($1,$2,$3,'active',false,$4,$5,$6,$7,$8,$9,now())
		ON CONFLICT (id) DO UPDATE SET
			endpoint=EXCLUDED.endpoint,
			pool=EXCLUDED.pool,
			status='active',
			draining=false,
			allocatable_vcpus=EXCLUDED.allocatable_vcpus,
			allocatable_memory_mb=EXCLUDED.allocatable_memory_mb,
			allocatable_disk_gb=EXCLUDED.allocatable_disk_gb,
			max_sandboxes=EXCLUDED.max_sandboxes,
			supported_images=EXCLUDED.supported_images,
			last_heartbeat_at=EXCLUDED.last_heartbeat_at,
			updated_at=now()`,
		worker.ID,
		worker.Endpoint,
		worker.Pool,
		worker.AllocatableVCPUs,
		worker.AllocatableMemoryMB,
		worker.AllocatableDiskGB,
		worker.MaxSandboxes,
		worker.SupportedImages,
		heartbeatAt,
	)
	if err != nil {
		return fmt.Errorf("register worker %s: %w", worker.ID, err)
	}
	return nil
}

func (c *Client) RecordHeartbeat(
	ctx context.Context,
	workerID string,
	capacity plane.Capacity,
	heartbeatAt time.Time,
) error {
	tag, err := c.pool.Exec(ctx, `
		UPDATE worker_hosts
		SET last_heartbeat_at=$2,
		    allocatable_disk_gb=$7,
		    reported_vcpus=$3,
		    reported_memory_mb=$4,
		    reported_disk_gb=$5,
		    reported_sandboxes=$6,
		    capacity_reported_at=$2,
		    status=CASE WHEN draining THEN 'draining' ELSE 'active' END,
		    updated_at=now()
		WHERE id=$1`,
		workerID,
		heartbeatAt,
		capacity.ReservedVCPUs,
		capacity.ReservedMemoryMB,
		capacity.ReservedDiskGB,
		capacity.ReservedSandboxes,
		capacity.AllocatableDiskGB,
	)
	if err != nil {
		return fmt.Errorf("record worker heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return orchestrator.ErrWorkerNotFound
	}
	return nil
}

func (c *Client) SetWorkerDraining(ctx context.Context, workerID string, draining bool) error {
	tag, err := c.pool.Exec(ctx, `
		UPDATE worker_hosts
		SET draining=$2,
		    status=CASE WHEN $2 THEN 'draining' ELSE 'active' END,
		    updated_at=now()
		WHERE id=$1`,
		workerID,
		draining,
	)
	if err != nil {
		return fmt.Errorf("set worker %s draining=%t: %w", workerID, draining, err)
	}
	if tag.RowsAffected() == 0 {
		return orchestrator.ErrWorkerNotFound
	}
	return nil
}

func (c *Client) ReservePlacement(
	ctx context.Context,
	sandboxID string,
	request orchestrator.PlacementRequest,
	policy orchestrator.PlacementPolicy,
	healthyAfter time.Time,
) (orchestrator.Placement, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return orchestrator.Placement{}, fmt.Errorf("begin placement transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var existingWorkerID *string
	var currentState string
	var vcpus, memoryMB, diskGB int
	var image string
	if err := tx.QueryRow(ctx, `
		SELECT host_id, vcpus, memory_mb, disk_gb, COALESCE(image,'alpine'), state
		FROM sandboxes
		WHERE id=$1::uuid
		FOR UPDATE`,
		sandboxID,
	).Scan(&existingWorkerID, &vcpus, &memoryMB, &diskGB, &image, &currentState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orchestrator.Placement{}, orchestrator.ErrSandboxNotFound
		}
		return orchestrator.Placement{}, fmt.Errorf("lock sandbox placement: %w", err)
	}
	if vcpus <= 0 || memoryMB <= 0 || diskGB <= 0 {
		return orchestrator.Placement{}, errors.New("sandbox has invalid resource requirements")
	}

	if existingWorkerID != nil {
		if currentState == "provisioning" || currentState == "resuming" || currentState == "destroying" {
			return orchestrator.Placement{}, orchestrator.ErrPlacementBusy
		}
		if currentState != "active" {
			return orchestrator.Placement{}, orchestrator.ErrInvalidState
		}
		var placement orchestrator.Placement
		placement.SandboxID = sandboxID
		placement.WorkerID = *existingWorkerID
		placement.State = currentState
		if err := tx.QueryRow(ctx, `SELECT endpoint FROM worker_hosts WHERE id=$1`, *existingWorkerID).
			Scan(&placement.Endpoint); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orchestrator.Placement{}, orchestrator.ErrWorkerNotFound
			}
			return orchestrator.Placement{}, fmt.Errorf("read existing placement: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return orchestrator.Placement{}, fmt.Errorf("commit existing placement: %w", err)
		}
		return placement, nil
	}

	var placement orchestrator.Placement
	placement.SandboxID = sandboxID
	placement.State = "provisioning"
	err = tx.QueryRow(ctx, `
		WITH sampled AS (
			SELECT id, endpoint,
			       (reported_vcpus+$3)::double precision /
			         GREATEST(1, FLOOR(allocatable_vcpus*$6)) AS cpu_score,
			       (reported_memory_mb+$4)::double precision /
			         GREATEST(1, FLOOR(allocatable_memory_mb*$7)) AS memory_score
			FROM worker_hosts
			WHERE pool=$1
			  AND status='active'
			  AND NOT draining
			  AND last_heartbeat_at >= $2
			  AND capacity_reported_at >= $2
			  -- Reported usage is a stale-tolerant ranking hint, never a hard
			  -- admission gate. Only exclude workers whose static shape cannot
			  -- ever satisfy this request; the worker makes the atomic decision.
			  AND FLOOR(allocatable_vcpus*$6) >= $3
			  AND FLOOR(allocatable_memory_mb*$7) >= $4
			  AND allocatable_disk_gb >= $5
			  AND $8 = ANY(supported_images)
			  AND max_sandboxes >= 1
			  AND NOT (id = ANY($9::text[]))
			ORDER BY random()
			LIMIT $10
		)
		SELECT id, endpoint
		FROM sampled
		ORDER BY GREATEST(cpu_score,memory_score), random()
		LIMIT 1`,
		request.Pool,
		healthyAfter,
		vcpus,
		memoryMB,
		diskGB,
		policy.CPUOvercommitRatio,
		policy.MemoryOvercommitRatio,
		image,
		request.ExcludedWorkerIDs,
		bestOfKSampleSize,
	).Scan(&placement.WorkerID, &placement.Endpoint)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orchestrator.Placement{}, orchestrator.ErrNoCapacity
		}
		return orchestrator.Placement{}, fmt.Errorf("select worker for placement: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes
		SET host_id=$2, state='provisioning', updated_at=now()
		WHERE id=$1::uuid`,
		sandboxID,
		placement.WorkerID,
	); err != nil {
		return orchestrator.Placement{}, fmt.Errorf("save sandbox placement: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return orchestrator.Placement{}, fmt.Errorf("commit placement: %w", err)
	}
	return placement, nil
}

func (c *Client) GetPlacement(ctx context.Context, sandboxID string) (orchestrator.Placement, bool, error) {
	var placement orchestrator.Placement
	err := c.pool.QueryRow(ctx, `
		SELECT s.id::text, w.id, w.endpoint, s.state
		FROM sandboxes s
		JOIN worker_hosts w ON w.id=s.host_id
		WHERE s.id=$1::uuid`,
		sandboxID,
	).Scan(&placement.SandboxID, &placement.WorkerID, &placement.Endpoint, &placement.State)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orchestrator.Placement{}, false, nil
		}
		return orchestrator.Placement{}, false, fmt.Errorf("get sandbox placement: %w", err)
	}
	return placement, true, nil
}

func (c *Client) UpdatePlacementState(ctx context.Context, sandboxID, workerID string, fromStates []string, toState string) error {
	tag, err := c.pool.Exec(ctx, `
		UPDATE sandboxes
		SET state=$4,
		    paused_at=CASE WHEN $4='active' THEN NULL ELSE paused_at END,
		    updated_at=now()
		WHERE id=$1::uuid
		  AND host_id=$2
		  AND state=ANY($3)`,
		sandboxID,
		workerID,
		fromStates,
		toState,
	)
	if err != nil {
		return fmt.Errorf("update sandbox placement state: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}

	var currentState string
	var currentWorkerID *string
	err = c.pool.QueryRow(ctx, `SELECT state, host_id FROM sandboxes WHERE id=$1::uuid`, sandboxID).
		Scan(&currentState, &currentWorkerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return orchestrator.ErrSandboxNotFound
	}
	if err != nil {
		return fmt.Errorf("read sandbox state after transition conflict: %w", err)
	}
	if currentState == toState && currentWorkerID != nil && *currentWorkerID == workerID {
		return nil
	}
	return orchestrator.ErrInvalidState
}

// PausePlacement persists lifecycle state. The worker atomically releases
// compute while retaining its local writable disk reservation.
func (c *Client) PausePlacement(ctx context.Context, sandboxID, workerID string) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin pause accounting: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentWorkerID *string
	var currentState string
	var vcpus, memoryMB int
	err = tx.QueryRow(ctx, `
		SELECT host_id, state, vcpus, memory_mb
		FROM sandboxes
		WHERE id=$1::uuid
		FOR UPDATE`,
		sandboxID,
	).Scan(&currentWorkerID, &currentState, &vcpus, &memoryMB)
	if errors.Is(err, pgx.ErrNoRows) {
		return orchestrator.ErrSandboxNotFound
	}
	if err != nil {
		return fmt.Errorf("lock sandbox for pause accounting: %w", err)
	}
	if currentWorkerID == nil || *currentWorkerID != workerID {
		return orchestrator.ErrInvalidState
	}
	if currentState == "paused" {
		return tx.Commit(ctx)
	}
	// A worker reporting paused while the row is resuming means the restore never
	// took (e.g. resume cleanup re-pausing a failed restore). The VM is not running,
	// so the row returns to paused instead of being stranded in resuming.
	if currentState != "active" && currentState != "resuming" {
		return orchestrator.ErrInvalidState
	}

	// paused_at keeps the original pause time when a failed resume falls back.
	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes
		SET state='paused',
		    paused_at=CASE WHEN state='active' THEN now() ELSE paused_at END,
		    updated_at=now()
		WHERE id=$1::uuid`,
		sandboxID,
	); err != nil {
		return fmt.Errorf("mark sandbox paused: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit pause accounting: %w", err)
	}
	return nil
}

// ReserveResume claims the durable lifecycle transition on the sandbox's
// existing healthy worker. Worker-local admission is the final capacity check.
func (c *Client) ReserveResume(
	ctx context.Context,
	sandboxID, workerID string,
	policy orchestrator.PlacementPolicy,
	healthyAfter time.Time,
) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin resume reservation: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentWorkerID *string
	var currentState string
	var vcpus, memoryMB int
	err = tx.QueryRow(ctx, `
		SELECT host_id, state, vcpus, memory_mb
		FROM sandboxes
		WHERE id=$1::uuid
		FOR UPDATE`,
		sandboxID,
	).Scan(&currentWorkerID, &currentState, &vcpus, &memoryMB)
	if errors.Is(err, pgx.ErrNoRows) {
		return orchestrator.ErrSandboxNotFound
	}
	if err != nil {
		return fmt.Errorf("lock sandbox for resume reservation: %w", err)
	}
	if currentWorkerID == nil || *currentWorkerID != workerID {
		return orchestrator.ErrInvalidState
	}
	if currentState == "active" || currentState == "resuming" {
		return tx.Commit(ctx)
	}
	if currentState != "paused" {
		return orchestrator.ErrInvalidState
	}

	var workerReady bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM worker_hosts
			WHERE id=$1
			  AND status='active'
			  AND NOT draining
			  AND last_heartbeat_at >= $2
		)`,
		workerID,
		healthyAfter,
	).Scan(&workerReady)
	if err != nil {
		return fmt.Errorf("check worker for resume: %w", err)
	}
	if !workerReady {
		return orchestrator.ErrNoCapacity
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes
		SET state='resuming', updated_at=now()
		WHERE id=$1::uuid`,
		sandboxID,
	); err != nil {
		return fmt.Errorf("mark sandbox resuming: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit resume reservation: %w", err)
	}
	return nil
}

// CancelResume returns a failed restore to paused after the worker confirms it
// has no running VM.
func (c *Client) CancelResume(ctx context.Context, sandboxID, workerID string) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin resume cancellation: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentWorkerID *string
	var currentState string
	var vcpus, memoryMB int
	err = tx.QueryRow(ctx, `
		SELECT host_id, state, vcpus, memory_mb
		FROM sandboxes
		WHERE id=$1::uuid
		FOR UPDATE`,
		sandboxID,
	).Scan(&currentWorkerID, &currentState, &vcpus, &memoryMB)
	if errors.Is(err, pgx.ErrNoRows) {
		return orchestrator.ErrSandboxNotFound
	}
	if err != nil {
		return fmt.Errorf("lock sandbox for resume cancellation: %w", err)
	}
	if currentWorkerID == nil || *currentWorkerID != workerID {
		return orchestrator.ErrInvalidState
	}
	if currentState == "paused" {
		return tx.Commit(ctx)
	}
	if currentState != "resuming" {
		return orchestrator.ErrInvalidState
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes
		SET state='paused', updated_at=now()
		WHERE id=$1::uuid`,
		sandboxID,
	); err != nil {
		return fmt.Errorf("restore paused state after resume failure: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit resume cancellation: %w", err)
	}
	return nil
}

func (c *Client) ReleasePlacement(ctx context.Context, sandboxID, finalState string) error {
	return c.releasePlacement(ctx, sandboxID, "", finalState)
}

func (c *Client) ReleaseWorkerPlacement(ctx context.Context, sandboxID, workerID, finalState string) error {
	return c.releasePlacement(ctx, sandboxID, workerID, finalState)
}

func (c *Client) releasePlacement(ctx context.Context, sandboxID, expectedWorkerID, finalState string) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin placement release: %w", err)
	}
	defer tx.Rollback(ctx)

	var workerID *string
	var vcpus, memoryMB, diskGB int
	var currentState string
	err = tx.QueryRow(ctx, `
		SELECT host_id, vcpus, memory_mb, disk_gb, state
		FROM sandboxes
		WHERE id=$1::uuid
		FOR UPDATE`,
		sandboxID,
	).Scan(&workerID, &vcpus, &memoryMB, &diskGB, &currentState)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orchestrator.ErrSandboxNotFound
		}
		return fmt.Errorf("lock sandbox placement for release: %w", err)
	}
	if workerID == nil {
		if expectedWorkerID != "" {
			if currentState == finalState {
				return tx.Commit(ctx)
			}
			return orchestrator.ErrInvalidState
		}
		if finalState != "" {
			if _, err := tx.Exec(ctx, `
				UPDATE sandboxes
				SET state=$2, updated_at=now()
				WHERE id=$1::uuid`,
				sandboxID,
				finalState,
			); err != nil {
				return fmt.Errorf("finalize unplaced sandbox: %w", err)
			}
		}
		return tx.Commit(ctx)
	}
	if expectedWorkerID != "" && *workerID != expectedWorkerID {
		return orchestrator.ErrInvalidState
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes
		SET host_id=NULL,
		    state=CASE WHEN $2='' THEN state ELSE $2 END,
		    updated_at=now()
		WHERE id=$1::uuid`,
		sandboxID,
		finalState,
	); err != nil {
		return fmt.Errorf("clear sandbox placement: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit placement release: %w", err)
	}
	return nil
}
