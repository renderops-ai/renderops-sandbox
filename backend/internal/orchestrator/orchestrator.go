// Package orchestrator owns durable worker registration and sandbox placement.
// It does not proxy execution traffic; the control plane uses the selected
// worker endpoint directly after placement.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"regexp"
	"strings"
	"time"

	"backend/internal/plane"
	"backend/internal/sandboximage"
)

var (
	ErrNoCapacity      = errors.New("no healthy worker has sufficient capacity")
	ErrPlacementBusy   = errors.New("worker placement is temporarily contended")
	ErrWorkerNotFound  = errors.New("worker not found")
	ErrSandboxNotFound = errors.New("sandbox not found")
	ErrInvalidState    = errors.New("sandbox lifecycle transition is no longer valid")
)

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

const (
	maxPlacementAttempts  = 9
	initialContentionWait = 5 * time.Millisecond
	maxContentionWait     = 100 * time.Millisecond
)

type WorkerRegistration struct {
	ID                  string   `json:"id"`
	Endpoint            string   `json:"endpoint"`
	Pool                string   `json:"pool"`
	AllocatableVCPUs    int      `json:"allocatable_vcpus"`
	AllocatableMemoryMB int      `json:"allocatable_memory_mb"`
	AllocatableDiskGB   int      `json:"allocatable_disk_gb"`
	MaxSandboxes        int      `json:"max_sandboxes"`
	SupportedImages     []string `json:"supported_images"`
}

type PlacementRequest struct {
	Pool              string   `json:"pool"`
	Image             string   `json:"image"`
	ExcludedWorkerIDs []string `json:"-"`
}

// PlacementPolicy is owned by the orchestrator and cannot be overridden by an
// API caller. Workers continue to advertise their physical capacity.
type PlacementPolicy struct {
	CPUOvercommitRatio    float64
	MemoryOvercommitRatio float64
}

type Placement struct {
	SandboxID string `json:"sandbox_id"`
	WorkerID  string `json:"worker_id"`
	Endpoint  string `json:"endpoint"`
	State     string `json:"state,omitempty"`
}

type ProvisionRequest struct {
	Pool string `json:"pool,omitempty"`
	plane.CreateRequest
}

type WorkerClient interface {
	Create(context.Context, plane.CreateRequest) (plane.CreateResponse, error)
	Pause(context.Context, string) error
	Resume(context.Context, string) error
	Destroy(context.Context, string) error
}

type WorkerClientFactory func(endpoint string) WorkerClient

type Store interface {
	RegisterWorker(context.Context, WorkerRegistration, time.Time) error
	RecordHeartbeat(context.Context, string, plane.Capacity, time.Time) error
	SetWorkerDraining(context.Context, string, bool) error
	ReservePlacement(context.Context, string, PlacementRequest, PlacementPolicy, time.Time) (Placement, error)
	GetPlacement(context.Context, string) (Placement, bool, error)
	UpdatePlacementState(context.Context, string, string, []string, string) error
	PausePlacement(context.Context, string, string) error
	ReserveResume(context.Context, string, string, PlacementPolicy, time.Time) error
	CancelResume(context.Context, string, string) error
	ReleasePlacement(context.Context, string, string) error
	ReleaseWorkerPlacement(context.Context, string, string, string) error
}

type Service struct {
	store           Store
	heartbeatTTL    time.Duration
	now             func() time.Time
	workerClient    WorkerClientFactory
	placementPolicy PlacementPolicy
	waitForRetry    func(context.Context, time.Duration) error
}

type Option func(*Service)

func WithWorkerClientFactory(factory WorkerClientFactory) Option {
	return func(service *Service) {
		service.workerClient = factory
	}
}

func WithPlacementPolicy(policy PlacementPolicy) Option {
	return func(service *Service) {
		service.placementPolicy = normalizePlacementPolicy(policy)
	}
}

func NewService(store Store, heartbeatTTL time.Duration, options ...Option) *Service {
	if heartbeatTTL <= 0 {
		heartbeatTTL = 30 * time.Second
	}
	service := &Service{
		store:           store,
		heartbeatTTL:    heartbeatTTL,
		now:             time.Now,
		placementPolicy: normalizePlacementPolicy(PlacementPolicy{}),
		waitForRetry:    waitWithJitter,
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) RegisterWorker(ctx context.Context, registration WorkerRegistration) error {
	registration.ID = strings.TrimSpace(registration.ID)
	registration.Endpoint = strings.TrimRight(strings.TrimSpace(registration.Endpoint), "/")
	registration.Pool = strings.TrimSpace(registration.Pool)
	if registration.Pool == "" {
		registration.Pool = "default"
	}
	if len(registration.SupportedImages) == 0 {
		registration.SupportedImages = []string{sandboximage.Default}
	}
	seenImages := make(map[string]struct{}, len(registration.SupportedImages))
	normalizedImages := make([]string, 0, len(registration.SupportedImages))
	for _, value := range registration.SupportedImages {
		image, err := sandboximage.Normalize(value)
		if err != nil {
			return err
		}
		if _, exists := seenImages[image]; !exists {
			seenImages[image] = struct{}{}
			normalizedImages = append(normalizedImages, image)
		}
	}
	registration.SupportedImages = normalizedImages
	if !workerIDPattern.MatchString(registration.ID) {
		return fmt.Errorf("invalid worker id %q", registration.ID)
	}
	if err := validateEndpoint(registration.Endpoint); err != nil {
		return err
	}
	if registration.AllocatableVCPUs <= 0 ||
		registration.AllocatableMemoryMB <= 0 ||
		registration.AllocatableDiskGB <= 0 ||
		registration.MaxSandboxes <= 0 {
		return errors.New("worker allocatable resources and max_sandboxes must be positive")
	}
	err := s.store.RegisterWorker(ctx, registration, s.now().UTC())
	if err != nil {
		logOrchestrator(ctx, slog.LevelWarn, "worker registration failed",
			"worker_id", registration.ID, "err", err)
		return err
	}
	logOrchestrator(ctx, slog.LevelInfo, "worker registered",
		"worker_id", registration.ID,
		"pool", registration.Pool,
		"allocatable_vcpus", registration.AllocatableVCPUs,
		"allocatable_memory_mb", registration.AllocatableMemoryMB,
		"allocatable_disk_gb", registration.AllocatableDiskGB,
		"max_sandboxes", registration.MaxSandboxes)
	return nil
}

func (s *Service) Heartbeat(ctx context.Context, workerID string, capacity plane.Capacity) error {
	workerID = strings.TrimSpace(workerID)
	if !workerIDPattern.MatchString(workerID) {
		return fmt.Errorf("invalid worker id %q", workerID)
	}
	err := s.store.RecordHeartbeat(ctx, workerID, capacity, s.now().UTC())
	if err != nil {
		logOrchestrator(ctx, slog.LevelWarn, "worker heartbeat failed",
			"worker_id", workerID, "err", err)
	}
	return err
}

func (s *Service) SetWorkerDraining(ctx context.Context, workerID string, draining bool) error {
	workerID = strings.TrimSpace(workerID)
	if !workerIDPattern.MatchString(workerID) {
		return fmt.Errorf("invalid worker id %q", workerID)
	}
	err := s.store.SetWorkerDraining(ctx, workerID, draining)
	if err != nil {
		logOrchestrator(ctx, slog.LevelWarn, "worker draining update failed",
			"worker_id", workerID, "draining", draining, "err", err)
		return err
	}
	logOrchestrator(ctx, slog.LevelInfo, "worker draining updated",
		"worker_id", workerID, "draining", draining)
	return nil
}

func (s *Service) Place(ctx context.Context, sandboxID string, request PlacementRequest) (Placement, error) {
	startedAt := time.Now()
	sandboxID = strings.TrimSpace(sandboxID)
	request.Pool = strings.TrimSpace(request.Pool)
	if sandboxID == "" {
		return Placement{}, errors.New("sandbox id is required")
	}
	if request.Pool == "" {
		request.Pool = "default"
	}
	image, err := sandboximage.Normalize(request.Image)
	if err != nil {
		return Placement{}, err
	}
	request.Image = image
	healthyAfter := s.now().UTC().Add(-s.heartbeatTTL)
	for attempt := 0; attempt < maxPlacementAttempts; attempt++ {
		placement, err := s.store.ReservePlacement(ctx, sandboxID, request, s.placementPolicy, healthyAfter)
		if !errors.Is(err, ErrPlacementBusy) {
			if err != nil {
				logOrchestrator(ctx, slog.LevelWarn, "placement failed",
					"sandbox_id", sandboxID,
					"pool", request.Pool,
					"image", request.Image,
					"attempts", attempt+1,
					"duration_ms", time.Since(startedAt).Milliseconds(),
					"err", err)
			} else {
				logOrchestrator(ctx, slog.LevelInfo, "placement reserved",
					"sandbox_id", sandboxID,
					"worker_id", placement.WorkerID,
					"state", placement.State,
					"attempts", attempt+1,
					"duration_ms", time.Since(startedAt).Milliseconds())
			}
			return placement, err
		}
		if attempt == maxPlacementAttempts-1 {
			logOrchestrator(ctx, slog.LevelWarn, "placement contended",
				"sandbox_id", sandboxID,
				"attempts", maxPlacementAttempts,
				"duration_ms", time.Since(startedAt).Milliseconds())
			return Placement{}, ErrPlacementBusy
		}
		if err := s.waitForRetry(ctx, contentionWait(attempt)); err != nil {
			return Placement{}, err
		}
	}
	return Placement{}, ErrPlacementBusy
}

func contentionWait(attempt int) time.Duration {
	wait := initialContentionWait << attempt
	if wait > maxContentionWait {
		return maxContentionWait
	}
	return wait
}

func waitWithJitter(ctx context.Context, ceiling time.Duration) error {
	wait := time.Duration(rand.Int63n(int64(ceiling) + 1))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func normalizePlacementPolicy(policy PlacementPolicy) PlacementPolicy {
	if policy.CPUOvercommitRatio < 1 {
		policy.CPUOvercommitRatio = 1
	}
	if policy.MemoryOvercommitRatio < 1 {
		policy.MemoryOvercommitRatio = 1
	}
	return policy
}

func (s *Service) Placement(ctx context.Context, sandboxID string) (Placement, bool, error) {
	if strings.TrimSpace(sandboxID) == "" {
		return Placement{}, false, errors.New("sandbox id is required")
	}
	return s.store.GetPlacement(ctx, sandboxID)
}

func (s *Service) Release(ctx context.Context, sandboxID string) error {
	if strings.TrimSpace(sandboxID) == "" {
		return errors.New("sandbox id is required")
	}
	return s.store.ReleasePlacement(ctx, sandboxID, "")
}

// Provision reserves capacity before booting and finalizes the sandbox only
// after the worker confirms that its guest agent is ready.
func (s *Service) Provision(ctx context.Context, request ProvisionRequest) (result Placement, resultErr error) {
	startedAt := time.Now()
	selectedWorkerID := ""
	defer func() {
		attributes := []any{
			"sandbox_id", strings.TrimSpace(request.SandboxID),
			"worker_id", selectedWorkerID,
			"duration_ms", time.Since(startedAt).Milliseconds(),
		}
		if resultErr != nil {
			attributes = append(attributes, "err", resultErr)
			logOrchestrator(ctx, slog.LevelError, "sandbox provision failed", attributes...)
			return
		}
		logOrchestrator(ctx, slog.LevelInfo, "sandbox provision completed", attributes...)
	}()
	logOrchestrator(ctx, slog.LevelInfo, "sandbox provision started",
		"sandbox_id", strings.TrimSpace(request.SandboxID),
		"pool", strings.TrimSpace(request.Pool),
		"image", request.Image)
	if s.workerClient == nil {
		return Placement{}, errors.New("worker client is not configured")
	}
	sandboxID := strings.TrimSpace(request.SandboxID)
	if sandboxID == "" {
		return Placement{}, errors.New("sandbox id is required")
	}
	image, err := sandboximage.Normalize(request.Image)
	if err != nil {
		return Placement{}, err
	}
	request.Image = image

	var placement Placement
	var response plane.CreateResponse
	excludedWorkerIDs := make([]string, 0, 3)
	for {
		var err error
		placement, err = s.Place(ctx, sandboxID, PlacementRequest{
			Pool: request.Pool, Image: image, ExcludedWorkerIDs: excludedWorkerIDs,
		})
		if err != nil {
			if !errors.Is(err, ErrSandboxNotFound) {
				if releaseErr := s.store.ReleasePlacement(ctx, sandboxID, "error"); releaseErr != nil {
					return Placement{}, errors.Join(err, fmt.Errorf("mark unscheduled sandbox failed: %w", releaseErr))
				}
			}
			return Placement{}, err
		}
		selectedWorkerID = placement.WorkerID
		if placement.State == "active" {
			return placement, nil
		}

		workerStartedAt := time.Now()
		response, err = s.workerClient(placement.Endpoint).Create(ctx, request.CreateRequest)
		if !errors.Is(err, plane.ErrNoCapacity) {
			if err == nil {
				logOrchestrator(ctx, slog.LevelInfo, "worker sandbox create completed",
					"sandbox_id", sandboxID,
					"worker_id", placement.WorkerID,
					"duration_ms", time.Since(workerStartedAt).Milliseconds())
				break
			}
			// A timeout may happen after the worker completed the boot. Destroy by
			// idempotency key before releasing the durable placement.
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_ = s.workerClient(placement.Endpoint).Destroy(cleanupCtx, sandboxID)
			cancel()
			releaseErr := s.store.ReleasePlacement(ctx, sandboxID, "error")
			if releaseErr != nil {
				return Placement{}, errors.Join(
					fmt.Errorf("provision sandbox on worker: %w", err),
					fmt.Errorf("release failed placement: %w", releaseErr),
				)
			}
			return Placement{}, fmt.Errorf("provision sandbox on worker: %w", err)
		}

		logOrchestrator(ctx, slog.LevelWarn, "worker rejected sandbox capacity",
			"sandbox_id", sandboxID,
			"worker_id", placement.WorkerID,
			"duration_ms", time.Since(workerStartedAt).Milliseconds())
		excludedWorkerIDs = append(excludedWorkerIDs, placement.WorkerID)
		if releaseErr := s.store.ReleasePlacement(ctx, sandboxID, "scheduling"); releaseErr != nil {
			return Placement{}, errors.Join(err, fmt.Errorf("release rejected placement: %w", releaseErr))
		}
	}

	if response.SandboxID != sandboxID {
		releaseErr := s.store.ReleasePlacement(ctx, sandboxID, "error")
		mismatch := fmt.Errorf("worker returned sandbox id %q, expected %q", response.SandboxID, sandboxID)
		if releaseErr != nil {
			return Placement{}, errors.Join(mismatch, fmt.Errorf("release mismatched placement: %w", releaseErr))
		}
		return Placement{}, mismatch
	}
	if err := s.store.UpdatePlacementState(ctx, sandboxID, placement.WorkerID, []string{"provisioning"}, "active"); err != nil {
		return Placement{}, fmt.Errorf("mark sandbox active: %w", err)
	}
	return placement, nil
}

func (s *Service) Pause(ctx context.Context, sandboxID string) (resultErr error) {
	startedAt := time.Now()
	workerID := ""
	logOrchestrator(ctx, slog.LevelInfo, "sandbox lifecycle started",
		"operation", "pause", "sandbox_id", strings.TrimSpace(sandboxID))
	defer func() {
		logLifecycleResult(ctx, "pause", sandboxID, workerID, startedAt, resultErr)
	}()
	if s.workerClient == nil {
		return errors.New("worker client is not configured")
	}
	placement, ok, err := s.Placement(ctx, sandboxID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrSandboxNotFound
	}
	workerID = placement.WorkerID
	if err := s.workerClient(placement.Endpoint).Pause(ctx, sandboxID); err != nil {
		return err
	}
	return s.store.PausePlacement(ctx, sandboxID, placement.WorkerID)
}

func (s *Service) Resume(ctx context.Context, sandboxID string) (resultErr error) {
	startedAt := time.Now()
	workerID := ""
	logOrchestrator(ctx, slog.LevelInfo, "sandbox lifecycle started",
		"operation", "resume", "sandbox_id", strings.TrimSpace(sandboxID))
	defer func() {
		logLifecycleResult(ctx, "resume", sandboxID, workerID, startedAt, resultErr)
	}()
	if s.workerClient == nil {
		return errors.New("worker client is not configured")
	}
	placement, ok, err := s.Placement(ctx, sandboxID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrSandboxNotFound
	}
	workerID = placement.WorkerID

	healthyAfter := s.now().UTC().Add(-s.heartbeatTTL)
	if err := s.store.ReserveResume(
		ctx,
		sandboxID,
		placement.WorkerID,
		s.placementPolicy,
		healthyAfter,
	); err != nil {
		return err
	}

	client := s.workerClient(placement.Endpoint)
	if err := client.Resume(ctx, sandboxID); err != nil {
		// A timed-out restore may still have created a running VM. Pause it before
		// returning the compute reservation to avoid an untracked active sandbox.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		cleanupErr := client.Pause(cleanupCtx, sandboxID)
		cancel()
		if cleanupErr != nil {
			return errors.Join(
				fmt.Errorf("resume sandbox on worker: %w", err),
				fmt.Errorf("resume cleanup failed; capacity remains reserved: %w", cleanupErr),
			)
		}
		// Settle the durable state even if the caller has gone away; a cancelled
		// context here would strand the row in resuming.
		storeCtx, storeCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		cancelErr := s.store.CancelResume(storeCtx, sandboxID, placement.WorkerID)
		storeCancel()
		if cancelErr != nil {
			return errors.Join(
				fmt.Errorf("resume sandbox on worker: %w", err),
				fmt.Errorf("release failed resume reservation: %w", cancelErr),
			)
		}
		return fmt.Errorf("resume sandbox on worker: %w", err)
	}
	storeCtx, storeCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer storeCancel()
	return s.store.UpdatePlacementState(
		storeCtx,
		sandboxID,
		placement.WorkerID,
		[]string{"resuming"},
		"active",
	)
}

func (s *Service) Destroy(ctx context.Context, sandboxID string) (resultErr error) {
	startedAt := time.Now()
	workerID := ""
	logOrchestrator(ctx, slog.LevelInfo, "sandbox lifecycle started",
		"operation", "destroy", "sandbox_id", strings.TrimSpace(sandboxID))
	defer func() {
		logLifecycleResult(ctx, "destroy", sandboxID, workerID, startedAt, resultErr)
	}()
	if s.workerClient == nil {
		return errors.New("worker client is not configured")
	}
	placement, ok, err := s.Placement(ctx, sandboxID)
	if err != nil {
		return err
	}
	if !ok {
		return s.store.ReleasePlacement(ctx, sandboxID, "destroyed")
	}
	workerID = placement.WorkerID
	// A paused sandbox has no compute reservation. Keep that state until the
	// final release so the store knows to release only its retained disk.
	if placement.State != "paused" {
		if err := s.store.UpdatePlacementState(
			ctx,
			sandboxID,
			placement.WorkerID,
			[]string{"active", "error", "provisioning", "resuming"},
			"destroying",
		); err != nil {
			return err
		}
	}
	if err := s.workerClient(placement.Endpoint).Destroy(ctx, sandboxID); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "not found") {
		return fmt.Errorf("destroy sandbox on worker: %w", err)
	}
	return s.store.ReleasePlacement(ctx, sandboxID, "destroyed")
}

func (s *Service) ReportWorkerState(ctx context.Context, workerID, sandboxID, state string) error {
	workerID = strings.TrimSpace(workerID)
	sandboxID = strings.TrimSpace(sandboxID)
	state = strings.TrimSpace(state)
	if !workerIDPattern.MatchString(workerID) {
		return fmt.Errorf("invalid worker id %q", workerID)
	}
	if sandboxID == "" {
		return errors.New("sandbox id is required")
	}

	var err error
	switch state {
	case "destroyed":
		err = s.store.ReleaseWorkerPlacement(ctx, sandboxID, workerID, "destroyed")
	case "paused":
		err = s.store.PausePlacement(ctx, sandboxID, workerID)
	case "active":
		// A paused sandbox cannot become active until Resume has atomically
		// reacquired compute capacity and moved it to resuming.
		err = s.store.UpdatePlacementState(ctx, sandboxID, workerID, []string{"resuming", "provisioning"}, "active")
	default:
		return fmt.Errorf("invalid worker sandbox state %q", state)
	}
	if err != nil {
		logOrchestrator(ctx, slog.LevelWarn, "worker sandbox state update failed",
			"worker_id", workerID, "sandbox_id", sandboxID, "state", state, "err", err)
		return err
	}
	logOrchestrator(ctx, slog.LevelInfo, "worker sandbox state updated",
		"worker_id", workerID, "sandbox_id", sandboxID, "state", state)
	return nil
}

func logLifecycleResult(
	ctx context.Context,
	operation string,
	sandboxID string,
	workerID string,
	startedAt time.Time,
	err error,
) {
	attributes := []any{
		"operation", operation,
		"sandbox_id", strings.TrimSpace(sandboxID),
		"worker_id", workerID,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	}
	if err != nil {
		attributes = append(attributes, "err", err)
		logOrchestrator(ctx, slog.LevelError, "sandbox lifecycle failed", attributes...)
		return
	}
	logOrchestrator(ctx, slog.LevelInfo, "sandbox lifecycle completed", attributes...)
}

func logOrchestrator(ctx context.Context, level slog.Level, message string, attributes ...any) {
	if requestID := requestIDFromContext(ctx); requestID != "" {
		attributes = append(attributes, "request_id", requestID)
	}
	slog.Log(ctx, level, message, attributes...)
}

func validateEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("worker endpoint %q must be an absolute http(s) URL", endpoint)
	}
	return nil
}
