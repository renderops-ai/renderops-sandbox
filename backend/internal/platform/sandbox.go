package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"backend/internal/metering"

	"github.com/jackc/pgx/v5"
)

type Sandbox struct {
	ID            string         `json:"id"`
	UserID        string         `json:"user_id"`
	APIKeyID      string         `json:"api_key_id,omitempty"`
	Image         string         `json:"image"`
	Name          string         `json:"name"`
	State         string         `json:"state"`
	BillingModel  string         `json:"billing_model"`
	VCPUs         int            `json:"vcpus"`
	MemoryMB      int            `json:"memory_mb"`
	DiskGB        int            `json:"disk_gb"`
	Internet      bool           `json:"internet"`
	IdleTimeoutMs int            `json:"idle_timeout_ms"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type SandboxListItem struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	State         string         `json:"state"`
	BillingModel  string         `json:"billing_model"`
	Image         string         `json:"image"`
	VCPUs         int            `json:"vcpus"`
	MemoryMB      int            `json:"memory_mb"`
	DiskGB        int            `json:"disk_gb"`
	IdleTimeoutMs int            `json:"idle_timeout_ms"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Created       string         `json:"created_at,omitempty"`
}

const upsertSandboxQuery = `INSERT INTO sandboxes (id,user_id,api_key_id,name,state,billing_model,image,vcpus,memory_mb,disk_gb,internet,idle_timeout_ms,expires_at,metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::integer,now()+(($12::integer)*interval '1 millisecond'),$13) ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id,api_key_id=EXCLUDED.api_key_id,name=EXCLUDED.name,state=EXCLUDED.state,billing_model=EXCLUDED.billing_model,image=EXCLUDED.image,vcpus=EXCLUDED.vcpus,memory_mb=EXCLUDED.memory_mb,disk_gb=EXCLUDED.disk_gb,internet=EXCLUDED.internet,idle_timeout_ms=EXCLUDED.idle_timeout_ms,expires_at=EXCLUDED.expires_at,metadata=EXCLUDED.metadata,updated_at=now()`

const insertSandboxQuery = `
		INSERT INTO sandboxes (
			id,user_id,api_key_id,name,state,billing_model,image,
			vcpus,memory_mb,disk_gb,internet,idle_timeout_ms,expires_at,metadata
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::integer,now()+(($12::integer)*interval '1 millisecond'),$13)`

func (c *Client) ListSandboxes(ctx context.Context, userID string) ([]SandboxListItem, error) {
	rows, err := c.pool.Query(ctx, `SELECT id::text,name,state,billing_model,COALESCE(image,'alpine'),vcpus,memory_mb,disk_gb,idle_timeout_ms,COALESCE(metadata,'{}'::jsonb),created_at FROM sandboxes WHERE user_id=$1 AND state<>'destroyed' ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("list sandboxes: %w", err)
	}
	defer rows.Close()
	items := make([]SandboxListItem, 0)
	for rows.Next() {
		var item SandboxListItem
		var metadata []byte
		var created time.Time
		if err := rows.Scan(&item.ID, &item.Name, &item.State, &item.BillingModel, &item.Image, &item.VCPUs, &item.MemoryMB, &item.DiskGB, &item.IdleTimeoutMs, &metadata, &created); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(metadata, &item.Metadata)
		item.Created = created.UTC().Format(time.RFC3339)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (c *Client) UpsertSandbox(ctx context.Context, sb Sandbox) {
	metadata, _ := marshalSandboxMetadata(sb.Metadata)
	var apiKeyID any
	if sb.APIKeyID != "" {
		apiKeyID = sb.APIKeyID
	}
	_, err := c.pool.Exec(ctx, upsertSandboxQuery, sb.ID, sb.UserID, apiKeyID, sb.Name, sb.State, sb.BillingModel, sb.Image, sb.VCPUs, sb.MemoryMB, sb.DiskGB, sb.Internet, sb.IdleTimeoutMs, metadata)
	if err != nil {
		slog.Warn("sandbox upsert failed", "err", err)
	}
}

// UpdateSandboxDetails enriches the durable row created by the control plane.
// It deliberately does not update lifecycle or resource fields, which are owned
// by the control-plane/orchestrator provisioning transaction.
func (c *Client) UpdateSandboxDetails(ctx context.Context, sb Sandbox) error {
	metadata, err := marshalSandboxMetadata(sb.Metadata)
	if err != nil {
		return fmt.Errorf("marshal sandbox metadata: %w", err)
	}
	var apiKeyID any
	if sb.APIKeyID != "" {
		apiKeyID = sb.APIKeyID
	}
	tag, err := c.pool.Exec(ctx, `
		UPDATE sandboxes
		SET api_key_id=$2,
		    name=$3,
		    metadata=$4,
		    updated_at=now()
		WHERE id::text=$1`,
		sb.ID,
		apiKeyID,
		sb.Name,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("update sandbox details: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("update sandbox details: sandbox %s not found", sb.ID)
	}
	return nil
}

// InsertSandbox creates the durable scheduling row before any worker capacity
// is reserved or VM boot is attempted.
func (c *Client) InsertSandbox(ctx context.Context, sb Sandbox) error {
	metadata, err := marshalSandboxMetadata(sb.Metadata)
	if err != nil {
		return fmt.Errorf("marshal sandbox metadata: %w", err)
	}
	var apiKeyID any
	if sb.APIKeyID != "" {
		apiKeyID = sb.APIKeyID
	}
	_, err = c.pool.Exec(ctx, insertSandboxQuery,
		sb.ID,
		sb.UserID,
		apiKeyID,
		sb.Name,
		sb.State,
		sb.BillingModel,
		sb.Image,
		sb.VCPUs,
		sb.MemoryMB,
		sb.DiskGB,
		sb.Internet,
		sb.IdleTimeoutMs,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("insert sandbox: %w", err)
	}
	return nil
}

func marshalSandboxMetadata(metadata map[string]any) ([]byte, error) {
	if metadata == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(metadata)
}

func (c *Client) UpdateSandboxState(ctx context.Context, id, state string) {
	var err error
	switch state {
	case "paused":
		// Capture pause time for billing/runtime; keep last_used_at as last activity.
		_, err = c.pool.Exec(ctx, `UPDATE sandboxes SET state=$2,updated_at=now(),paused_at=now(),expires_at=now()+interval '7 days' WHERE id=$1`, id, state)
	case "active":
		_, err = c.pool.Exec(ctx, `UPDATE sandboxes SET state=$2,updated_at=now(),last_used_at=now(),paused_at=NULL,expires_at=now()+(idle_timeout_ms*interval '1 millisecond') WHERE id=$1`, id, state)
	case "destroyed", "destroying", "error":
		// Stamp last_used_at so dashboard duration (created → end) is non-zero.
		_, err = c.pool.Exec(ctx, `UPDATE sandboxes SET state=$2,updated_at=now(),last_used_at=now(),expires_at=NULL WHERE id=$1`, id, state)
	default:
		_, err = c.pool.Exec(ctx, `UPDATE sandboxes SET state=$2,updated_at=now() WHERE id=$1`, id, state)
	}
	if err != nil {
		slog.Warn("sandbox state update failed", "err", err)
	}
}

// TouchSandbox advances the durable activity projection after a successful execution.
func (c *Client) TouchSandbox(ctx context.Context, id string) {
	_, err := c.pool.Exec(ctx, `UPDATE sandboxes SET last_used_at=now(),expires_at=now()+(idle_timeout_ms*interval '1 millisecond'),updated_at=now() WHERE id=$1`, id)
	if err != nil {
		slog.Warn("sandbox activity update failed", "sandbox_id", id, "err", err)
	}
}

type SandboxRun struct {
	ID         string `json:"id"`
	SandboxID  string `json:"sandbox_id"`
	UserID     string `json:"user_id"`
	Kind       string `json:"kind"`
	Language   string `json:"language"`
	Command    string `json:"command"`
	ExitCode   int    `json:"exit_code"`
	Status     string `json:"status"`
	DurationMs int    `json:"duration_ms"`
	StartedAt  string `json:"started_at"`
}

func (c *Client) InsertSandboxRun(ctx context.Context, run SandboxRun) {
	_, err := c.pool.Exec(ctx, `INSERT INTO sandbox_runs (id,sandbox_id,user_id,kind,language,command,exit_code,status,duration_ms,started_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, run.ID, run.SandboxID, run.UserID, run.Kind, run.Language, run.Command, run.ExitCode, run.Status, run.DurationMs, run.StartedAt)
	if err != nil {
		slog.Warn("sandbox run insert failed", "err", err)
	}
}

type SandboxLog struct {
	SandboxID string `json:"sandbox_id"`
	RunID     string `json:"run_id,omitempty"`
	UserID    string `json:"user_id"`
	Stream    string `json:"stream"`
	Level     string `json:"level,omitempty"`
	Language  string `json:"language,omitempty"`
	Content   string `json:"content"`
}

func (c *Client) InsertSandboxLog(ctx context.Context, entry SandboxLog) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		slog.Warn("sandbox log transaction failed", "err", err)
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1397772108)`); err != nil {
		slog.Warn("sandbox log ordering failed", "err", err)
		return
	}
	var runID any
	if entry.RunID != "" {
		runID = entry.RunID
	}
	_, err = tx.Exec(ctx, `INSERT INTO sandbox_logs (sandbox_id,run_id,user_id,stream,level,language,content) VALUES ($1,$2,$3,$4,$5,$6,$7)`, entry.SandboxID, runID, entry.UserID, entry.Stream, entry.Level, entry.Language, entry.Content)
	if err != nil {
		slog.Warn("sandbox log insert failed", "err", err)
		return
	}
	if err = tx.Commit(ctx); err != nil {
		slog.Warn("sandbox log commit failed", "err", err)
	}
}

func (c *Client) AccrueUsageMeters(ctx context.Context, samples []metering.Sample) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin usage meter batch: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, sample := range samples {
		tag, execErr := tx.Exec(ctx, `
			INSERT INTO usage_meters (
				user_id,sandbox_id,billing_model,bucket,
				vcpu_seconds,ram_gb_seconds,disk_gb_seconds
			)
			SELECT
				s.user_id,s.id,s.billing_model,$3,$4,$5,$6
			FROM sandboxes s
			WHERE s.id::text=$1 AND s.host_id=$2
			ON CONFLICT (sandbox_id,bucket) DO UPDATE SET
				vcpu_seconds=GREATEST(usage_meters.vcpu_seconds,EXCLUDED.vcpu_seconds),
				ram_gb_seconds=GREATEST(usage_meters.ram_gb_seconds,EXCLUDED.ram_gb_seconds),
				disk_gb_seconds=GREATEST(usage_meters.disk_gb_seconds,EXCLUDED.disk_gb_seconds)`,
			sample.SandboxID,
			sample.WorkerID,
			sample.Bucket,
			sample.VCPUSeconds,
			sample.RAMGBSeconds,
			sample.DiskGBSeconds,
		)
		if execErr != nil {
			return fmt.Errorf("accrue usage meter for sandbox %s: %w", sample.SandboxID, execErr)
		}
		if tag.RowsAffected() == 0 {
			slog.Warn(
				"rejected usage sample from non-owning worker",
				"sandbox_id", sample.SandboxID,
				"worker_id", sample.WorkerID,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit usage meter batch: %w", err)
	}
	return nil
}

type SandboxRef struct {
	ID            string    `json:"id"`
	State         string    `json:"state"`
	UserID        string    `json:"user_id"`
	BillingModel  string    `json:"billing_model"`
	Image         string    `json:"image"`
	VCPUs         int       `json:"vcpus"`
	MemoryMB      int       `json:"memory_mb"`
	DiskGB        int       `json:"disk_gb"`
	Internet      bool      `json:"internet"`
	IdleTimeoutMs int       `json:"idle_timeout_ms"`
	Created       time.Time `json:"created_at"`
	LastUsed      time.Time `json:"last_used_at"`
}

// GetSandbox fetches one sandbox row by id. ok=false when no row matches
// (the id::text comparison also makes malformed ids a miss, not an error).
func (c *Client) GetSandbox(ctx context.Context, id string) (SandboxRef, bool, error) {
	var ref SandboxRef
	err := c.pool.QueryRow(ctx, `SELECT id::text,state,user_id::text,COALESCE(billing_model,'payg'),COALESCE(image,'alpine'),COALESCE(vcpus,0),COALESCE(memory_mb,0),COALESCE(disk_gb,0),COALESCE(internet,true),idle_timeout_ms,created_at,COALESCE(last_used_at,created_at) FROM sandboxes WHERE id::text=$1`, id).
		Scan(&ref.ID, &ref.State, &ref.UserID, &ref.BillingModel, &ref.Image, &ref.VCPUs, &ref.MemoryMB, &ref.DiskGB, &ref.Internet, &ref.IdleTimeoutMs, &ref.Created, &ref.LastUsed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SandboxRef{}, false, nil
		}
		return SandboxRef{}, false, fmt.Errorf("get sandbox: %w", err)
	}
	return ref, true, nil
}

func (c *Client) ListSandboxesByState(ctx context.Context, states []string) ([]SandboxRef, error) {
	rows, err := c.pool.Query(ctx, `SELECT id::text,state,user_id::text,COALESCE(billing_model,'payg'),COALESCE(image,'alpine'),COALESCE(vcpus,0),COALESCE(memory_mb,0),COALESCE(disk_gb,0),COALESCE(internet,true),idle_timeout_ms,created_at,COALESCE(last_used_at,created_at) FROM sandboxes WHERE state=ANY($1)`, states)
	if err != nil {
		return nil, fmt.Errorf("list sandboxes by state: %w", err)
	}
	defer rows.Close()
	refs := make([]SandboxRef, 0)
	for rows.Next() {
		var ref SandboxRef
		if err := rows.Scan(&ref.ID, &ref.State, &ref.UserID, &ref.BillingModel, &ref.Image, &ref.VCPUs, &ref.MemoryMB, &ref.DiskGB, &ref.Internet, &ref.IdleTimeoutMs, &ref.Created, &ref.LastUsed); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}
