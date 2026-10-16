// Package controlplane holds the control-plane side of the split architecture.
//
// Service is a plane.Service implementation with no local VMs. The
// orchestrator resolves durable placement and the control plane sends execution
// directly to the selected worker over its private HTTP endpoint.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"backend/internal/agent"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/sandboximage"

	"github.com/google/uuid"
)

type Service struct {
	db           *platform.Client
	orchestrator *orchestrator.Client
	workerToken  string
}

var _ plane.Service = (*Service)(nil)

func NewService(db *platform.Client, orchestrationClient *orchestrator.Client, workerToken string) *Service {
	return &Service{db: db, orchestrator: orchestrationClient, workerToken: workerToken}
}

// Create writes the scheduling row first, then asks the orchestrator to reserve
// a healthy worker and boot this exact UUID. The worker never writes Postgres.
func (s *Service) Create(ctx context.Context, userID, billingModel, image string, env map[string]string, vcpus, memoryMB, diskGB int, internet bool, idleTimeout, maxLifetime time.Duration) (*plane.SessionInfo, error) {
	image, err := sandboximage.Normalize(image)
	if err != nil {
		return nil, err
	}
	sandboxID := uuid.NewString()
	if err := s.db.InsertSandbox(ctx, platform.Sandbox{
		ID:            sandboxID,
		UserID:        userID,
		Image:         image,
		Name:          "sandbox",
		State:         "scheduling",
		BillingModel:  billingModel,
		VCPUs:         vcpus,
		MemoryMB:      memoryMB,
		DiskGB:        diskGB,
		Internet:      internet,
		IdleTimeoutMs: int(idleTimeout.Milliseconds()),
	}); err != nil {
		return nil, fmt.Errorf("create sandbox scheduling row: %w", err)
	}

	_, err = s.orchestrator.Provision(ctx, orchestrator.ProvisionRequest{
		CreateRequest: plane.CreateRequest{
			SandboxID:    sandboxID,
			UserID:       userID,
			Image:        image,
			BillingModel: billingModel,
			Env:          env,
			VCPUs:        vcpus,
			MemoryMB:     memoryMB,
			DiskGB:       diskGB,
			Internet:     internet,
			IdleTimeoutS: int(idleTimeout.Seconds()),
			MaxLifetimeS: int(maxLifetime.Seconds()),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("provision sandbox: %w", err)
	}

	now := time.Now()
	return &plane.SessionInfo{
		ID:           sandboxID,
		UserID:       userID,
		Image:        image,
		BillingModel: billingModel,
		Env:          env,
		VCPUs:        vcpus,
		MemoryMB:     memoryMB,
		DiskGB:       diskGB,
		Internet:     internet,
		IdleTimeout:  idleTimeout,
		MaxLifetime:  maxLifetime,
		CreatedAt:    now,
		LastUsed:     now,
		State:        plane.StateActive,
	}, nil
}

var (
	// ErrSandboxIDConflict means the requested sandbox ID belongs to another user.
	ErrSandboxIDConflict = errors.New("sandbox id is already used by another user")
	// ErrSandboxGone means the sandbox with the requested ID was destroyed; the
	// ID cannot be reused.
	ErrSandboxGone = errors.New("sandbox was destroyed")
	// ErrSandboxBusy means the sandbox is being provisioned or resumed; retry later.
	ErrSandboxBusy = errors.New("sandbox is busy")
)

// SandboxSpec is a sandbox with a caller-chosen ID (see CreateWithID).
type SandboxSpec struct {
	ID           string // a UUID
	UserID       string
	Name         string
	BillingModel string
	Image        string
	Env          map[string]string
	VCPUs        int
	MemoryMB     int
	DiskGB       int
	Internet     bool
	IdleTimeout  time.Duration
	MaxLifetime  time.Duration
}

// sandboxRows is the part of the platform database CreateWithID needs.
type sandboxRows interface {
	GetSandbox(ctx context.Context, id string) (platform.SandboxRef, bool, error)
	InsertSandbox(ctx context.Context, sb platform.Sandbox) error
}

// provisioner is the part of the orchestrator client CreateWithID needs.
type provisioner interface {
	Provision(ctx context.Context, request orchestrator.ProvisionRequest) (orchestrator.Placement, error)
}

// CreateWithID creates a sandbox whose ID the caller chose (the agent-session
// runner reserves it before provisioning, so a retry never leaks a second VM).
// It is idempotent: when a row with that ID already exists for the same owner,
// the existing sandbox is returned (active or paused) or its interrupted
// provisioning is retried (scheduling or error). A row of another owner, a
// destroyed row, or one that is provisioning/resuming right now is an error.
func (s *Service) CreateWithID(ctx context.Context, spec SandboxSpec) (*plane.SessionInfo, error) {
	return createWithID(ctx, s.db, s.orchestrator, spec)
}

func createWithID(ctx context.Context, rows sandboxRows, orch provisioner, spec SandboxSpec) (*plane.SessionInfo, error) {
	id, err := uuid.Parse(spec.ID)
	if err != nil {
		return nil, fmt.Errorf("sandbox id %q must be a UUID: %w", spec.ID, err)
	}
	// Rows are looked up by id::text, which Postgres renders in lower case.
	spec.ID = id.String()
	if strings.TrimSpace(spec.UserID) == "" {
		return nil, errors.New("sandbox owner is required")
	}
	if spec.Image, err = sandboximage.Normalize(spec.Image); err != nil {
		return nil, err
	}
	if spec.Name == "" {
		spec.Name = "sandbox"
	}

	ref, found, err := rows.GetSandbox(ctx, spec.ID)
	if err != nil {
		return nil, fmt.Errorf("look up sandbox %s: %w", spec.ID, err)
	}
	if !found {
		insertErr := rows.InsertSandbox(ctx, platform.Sandbox{
			ID:            spec.ID,
			UserID:        spec.UserID,
			Image:         spec.Image,
			Name:          spec.Name,
			State:         "scheduling",
			BillingModel:  spec.BillingModel,
			VCPUs:         spec.VCPUs,
			MemoryMB:      spec.MemoryMB,
			DiskGB:        spec.DiskGB,
			Internet:      spec.Internet,
			IdleTimeoutMs: int(spec.IdleTimeout.Milliseconds()),
		})
		if insertErr == nil {
			return provisionWithID(ctx, orch, spec)
		}
		// A concurrent create of the same ID may have won the insert.
		ref, found, err = rows.GetSandbox(ctx, spec.ID)
		if err != nil || !found {
			return nil, fmt.Errorf("create sandbox scheduling row: %w", insertErr)
		}
	}

	if ref.UserID != spec.UserID {
		return nil, fmt.Errorf("%w: %s", ErrSandboxIDConflict, spec.ID)
	}
	switch ref.State {
	case "active", "paused":
		return sessionInfoFromRef(ref, spec), nil
	case "scheduling", "error":
		// The earlier create stopped before the sandbox was placed (or its
		// placement failed and was released); placing it again is safe.
		slog.Info("retrying provisioning of existing sandbox", "sandbox_id", spec.ID, "state", ref.State)
		return provisionWithID(ctx, orch, spec)
	case "provisioning", "resuming":
		return nil, fmt.Errorf("%w: sandbox %s is %s", ErrSandboxBusy, spec.ID, ref.State)
	default:
		return nil, fmt.Errorf("%w: sandbox %s is %s", ErrSandboxGone, spec.ID, ref.State)
	}
}

func provisionWithID(ctx context.Context, orch provisioner, spec SandboxSpec) (*plane.SessionInfo, error) {
	if _, err := orch.Provision(ctx, orchestrator.ProvisionRequest{
		CreateRequest: plane.CreateRequest{
			SandboxID:    spec.ID,
			UserID:       spec.UserID,
			Image:        spec.Image,
			BillingModel: spec.BillingModel,
			Env:          spec.Env,
			VCPUs:        spec.VCPUs,
			MemoryMB:     spec.MemoryMB,
			DiskGB:       spec.DiskGB,
			Internet:     spec.Internet,
			IdleTimeoutS: int(spec.IdleTimeout.Seconds()),
			MaxLifetimeS: int(spec.MaxLifetime.Seconds()),
		},
	}); err != nil {
		return nil, fmt.Errorf("provision sandbox %s: %w", spec.ID, err)
	}
	now := time.Now()
	return &plane.SessionInfo{
		ID:           spec.ID,
		UserID:       spec.UserID,
		Image:        spec.Image,
		BillingModel: spec.BillingModel,
		Env:          spec.Env,
		VCPUs:        spec.VCPUs,
		MemoryMB:     spec.MemoryMB,
		DiskGB:       spec.DiskGB,
		Internet:     spec.Internet,
		IdleTimeout:  spec.IdleTimeout,
		MaxLifetime:  spec.MaxLifetime,
		CreatedAt:    now,
		LastUsed:     now,
		State:        plane.StateActive,
	}, nil
}

func sessionInfoFromRef(ref platform.SandboxRef, spec SandboxSpec) *plane.SessionInfo {
	state := plane.StateActive
	if ref.State == "paused" {
		state = plane.StatePaused
	}
	return &plane.SessionInfo{
		ID:           ref.ID,
		UserID:       ref.UserID,
		Image:        ref.Image,
		BillingModel: ref.BillingModel,
		VCPUs:        ref.VCPUs,
		MemoryMB:     ref.MemoryMB,
		DiskGB:       ref.DiskGB,
		Internet:     ref.Internet,
		IdleTimeout:  time.Duration(ref.IdleTimeoutMs) * time.Millisecond,
		MaxLifetime:  spec.MaxLifetime,
		CreatedAt:    ref.Created,
		LastUsed:     ref.LastUsed,
		State:        state,
	}
}

func (s *Service) Execute(ctx context.Context, sessionID, code, language string, timeoutSec int) (plane.ExecResult, error) {
	worker, err := s.worker(ctx, sessionID)
	if err != nil {
		return plane.ExecResult{}, err
	}
	res, err := worker.Run(ctx, sessionID, plane.RunRequest{Code: code, Language: language, TimeoutS: timeoutSec})
	return res, s.reconcile(ctx, sessionID, err)
}

func (s *Service) Exec(ctx context.Context, sessionID, command string, timeoutSec int) (plane.ExecResult, error) {
	worker, err := s.worker(ctx, sessionID)
	if err != nil {
		return plane.ExecResult{}, err
	}
	res, err := worker.Exec(ctx, sessionID, plane.ExecRequest{Command: command, TimeoutS: timeoutSec})
	return res, s.reconcile(ctx, sessionID, err)
}

// ExecIfActive runs a shell command only while the sandbox is running. Unlike
// Exec it never resumes a paused sandbox: it fails with plane.ErrSandboxNotActive,
// and the worker does too if the sandbox pauses before the command reaches it.
// A passive command (the dashboard reading files) does not count as activity,
// so the sandbox's idle timer is left alone.
func (s *Service) ExecIfActive(ctx context.Context, sandboxID, command string, timeoutSec int, passive bool) (plane.ExecResult, error) {
	worker := func(endpoint string) workerExecer { return agent.NewClient(endpoint, s.workerToken) }
	res, err := execIfActive(ctx, s.orchestrator, worker, sandboxID, plane.ExecRequest{
		Command: command, TimeoutS: timeoutSec, ActiveOnly: true, Passive: passive,
	})
	if err != nil {
		return res, err
	}
	if !passive {
		s.db.TouchSandbox(ctx, sandboxID)
	}
	return res, nil
}

// placementGetter is the part of the orchestrator client execIfActive needs.
type placementGetter interface {
	Placement(ctx context.Context, sandboxID string) (orchestrator.Placement, error)
}

// workerExecer is the part of the worker client execIfActive needs.
type workerExecer interface {
	Exec(ctx context.Context, sandboxID string, req plane.ExecRequest) (plane.ExecResult, error)
}

func execIfActive(ctx context.Context, orch placementGetter, worker func(endpoint string) workerExecer, sandboxID string, req plane.ExecRequest) (plane.ExecResult, error) {
	placement, err := orch.Placement(ctx, sandboxID)
	if err != nil {
		return plane.ExecResult{}, fmt.Errorf("resolve sandbox worker: %w", err)
	}
	if placement.State != "active" {
		return plane.ExecResult{}, fmt.Errorf("%w: sandbox %s is %s", plane.ErrSandboxNotActive, sandboxID, placement.State)
	}
	if placement.Endpoint == "" {
		return plane.ExecResult{}, fmt.Errorf("sandbox %s has no worker endpoint", sandboxID)
	}
	res, err := worker(placement.Endpoint).Exec(ctx, sandboxID, req)
	if err != nil {
		return plane.ExecResult{}, fmt.Errorf("exec on worker %s: %w", placement.WorkerID, err)
	}
	return res, nil
}

// reconcile self-heals zombie rows: if the agent no longer has the VM (e.g. it
// restarted and lost all in-memory VMs), the DB row is still "active" but dead.
// On a not-found error we mark it destroyed so it drops out of the dashboard,
// and return a clear message instead of the raw agent error.
func (s *Service) reconcile(ctx context.Context, sessionID string, err error) error {
	if err == nil {
		s.db.TouchSandbox(ctx, sessionID)
		return nil
	}
	if !strings.Contains(err.Error(), "not found") {
		return err
	}
	_ = s.orchestrator.Destroy(ctx, sessionID)
	return fmt.Errorf("this sandbox is no longer running (its VM was reclaimed) — create a new one")
}

func (s *Service) Pause(ctx context.Context, sessionID string) error {
	return s.orchestrator.Pause(ctx, sessionID)
}

func (s *Service) Resume(ctx context.Context, sessionID string) error {
	return s.orchestrator.Resume(ctx, sessionID)
}

func (s *Service) Destroy(ctx context.Context, sessionID string) error {
	return s.orchestrator.Destroy(ctx, sessionID)
}

// worker returns a client for the worker running the sandbox, resuming the
// sandbox first if it is paused (see activePlacement).
func (s *Service) worker(ctx context.Context, sandboxID string) (*agent.Client, error) {
	placement, err := activePlacement(ctx, s.orchestrator, sandboxID)
	if err != nil {
		return nil, err
	}
	return agent.NewClient(placement.Endpoint, s.workerToken), nil
}

// placementResumer is the part of the orchestrator client activePlacement needs.
type placementResumer interface {
	Placement(ctx context.Context, sandboxID string) (orchestrator.Placement, error)
	Resume(ctx context.Context, sandboxID string) error
}

// activePlacement resolves the sandbox's worker. A paused sandbox is resumed
// first through the orchestrator, the same path as POST /session/{id}/resume:
// the row moves paused -> resuming -> active with capacity reserved, and a
// failed resume returns it to paused. Sending the command straight to the
// worker instead would let the worker restore the VM on its own, which the
// orchestrator rejects (paused -> active), leaving the VM running while the
// sandbox still reads as paused.
func activePlacement(ctx context.Context, orch placementResumer, sandboxID string) (orchestrator.Placement, error) {
	placement, err := orch.Placement(ctx, sandboxID)
	if err != nil {
		return orchestrator.Placement{}, fmt.Errorf("resolve sandbox worker: %w", err)
	}
	if placement.State != "paused" {
		return placement, nil
	}
	slog.Info("resuming paused sandbox before execution", "sandbox_id", sandboxID, "worker_id", placement.WorkerID)
	if err := orch.Resume(ctx, sandboxID); err != nil {
		return orchestrator.Placement{}, fmt.Errorf("resume paused sandbox %s: %w", sandboxID, err)
	}
	placement.State = "active"
	return placement, nil
}

// GetSession reads the sandbox row; destroyed sandboxes read as gone.
func (s *Service) GetSession(ctx context.Context, id string) (*plane.SessionInfo, bool) {
	ref, ok, err := s.db.GetSandbox(ctx, id)
	if err != nil || !ok {
		return nil, false
	}
	var state plane.SessionState
	switch ref.State {
	case "active":
		state = plane.StateActive
	case "paused":
		state = plane.StatePaused
	default:
		// Scheduling/provisioning/error/destroying rows remain visible in the
		// dashboard, but cannot accept execution or lifecycle commands.
		return nil, false
	}
	return &plane.SessionInfo{
		ID:           ref.ID,
		UserID:       ref.UserID,
		Image:        ref.Image,
		BillingModel: ref.BillingModel,
		VCPUs:        ref.VCPUs,
		MemoryMB:     ref.MemoryMB,
		DiskGB:       ref.DiskGB,
		Internet:     ref.Internet,
		IdleTimeout:  time.Duration(ref.IdleTimeoutMs) * time.Millisecond,
		CreatedAt:    ref.Created,
		LastUsed:     ref.LastUsed,
		State:        state,
	}, true
}
