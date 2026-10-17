package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"backend/internal/orchestrator"

	"github.com/jackc/pgx/v5"
)

// ListWorkerPlacements returns the sandboxes placed on a worker in the given
// states, for reconciling them with what the worker holds.
func (c *Client) ListWorkerPlacements(ctx context.Context, workerID string, states []string) ([]orchestrator.PlacedSandbox, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT id::text, state, updated_at
		FROM sandboxes
		WHERE host_id=$1
		  AND state=ANY($2)`,
		workerID,
		states,
	)
	if err != nil {
		return nil, fmt.Errorf("list worker placements: %w", err)
	}
	defer rows.Close()
	placed := make([]orchestrator.PlacedSandbox, 0)
	for rows.Next() {
		var row orchestrator.PlacedSandbox
		if err := rows.Scan(&row.ID, &row.State, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan worker placement: %w", err)
		}
		placed = append(placed, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list worker placements: %w", err)
	}
	return placed, nil
}

// MarkPlacementLost moves a sandbox whose VM is gone to the terminal "lost"
// state and releases its placement, in one transaction with a "sandbox.lost"
// audit event carrying the reason (the dashboard and the 410 response read it
// from there). last_used_at is left alone: runtime billing ends at the last
// activity, not when the loss was noticed. A row that is not placed on
// workerID, not in fromStates, or changed after unchangedSince (when set) is
// left alone and false is returned.
func (c *Client) MarkPlacementLost(
	ctx context.Context,
	sandboxID, workerID string,
	fromStates []string,
	unchangedSince time.Time,
	reason string,
) (bool, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin lost sandbox update: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentWorkerID *string
	var currentState, userID string
	var updatedAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT host_id, state, user_id::text, updated_at
		FROM sandboxes
		WHERE id=$1::uuid
		FOR UPDATE`,
		sandboxID,
	).Scan(&currentWorkerID, &currentState, &userID, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, orchestrator.ErrSandboxNotFound
	}
	if err != nil {
		return false, fmt.Errorf("lock sandbox for lost update: %w", err)
	}
	if currentWorkerID == nil || *currentWorkerID != workerID {
		return false, nil
	}
	if !slices.Contains(fromStates, currentState) {
		return false, nil
	}
	if !unchangedSince.IsZero() && updatedAt.After(unchangedSince) {
		return false, nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes
		SET state='lost',
		    host_id=NULL,
		    expires_at=NULL,
		    updated_at=now()
		WHERE id=$1::uuid`,
		sandboxID,
	); err != nil {
		return false, fmt.Errorf("mark sandbox lost: %w", err)
	}
	metadata, err := json.Marshal(map[string]string{
		"reason":         reason,
		"worker_id":      workerID,
		"previous_state": currentState,
	})
	if err != nil {
		return false, fmt.Errorf("marshal lost sandbox audit metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_events (
			scope_type, scope_id, actor_type, action,
			resource_type, resource_id, outcome, metadata
		)
		VALUES ('personal', $1::uuid, 'system', 'sandbox.lost', 'sandbox', $2, 'failure', $3)`,
		userID,
		sandboxID,
		metadata,
	); err != nil {
		return false, fmt.Errorf("record lost sandbox audit event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit lost sandbox update: %w", err)
	}
	return true, nil
}

// SandboxLostReason returns why a lost sandbox was lost, from its latest
// "sandbox.lost" audit event ("" when there is none).
func (c *Client) SandboxLostReason(ctx context.Context, sandboxID string) (string, error) {
	var reason string
	err := c.pool.QueryRow(ctx, `
		SELECT COALESCE(metadata->>'reason','')
		FROM audit_events
		WHERE resource_type='sandbox'
		  AND resource_id=$1
		  AND action='sandbox.lost'
		ORDER BY created_at DESC
		LIMIT 1`,
		sandboxID,
	).Scan(&reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read sandbox lost reason: %w", err)
	}
	return reason, nil
}
