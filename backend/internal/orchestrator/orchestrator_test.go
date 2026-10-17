package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"backend/internal/plane"
)

type fakeStore struct {
	registration   WorkerRegistration
	registeredAt   time.Time
	heartbeatID    string
	heartbeat      plane.Capacity
	heartbeatAt    time.Time
	drainingID     string
	draining       bool
	sandboxID      string
	request        PlacementRequest
	policy         PlacementPolicy
	healthyAfter   time.Time
	placement      Placement
	placements     []Placement
	updatedFrom    []string
	updatedState   string
	paused         bool
	resumeReserved bool
	resumeCanceled bool
	releaseState   string
	releaseWorker  string
	reserveErrors  []error
	reserveCalls   int
	workerRows     []PlacedSandbox
	listedWorker   string
	listedStates   []string
	lostMarks      []lostMark
	err            error
}

func (f *fakeStore) RegisterWorker(_ context.Context, registration WorkerRegistration, at time.Time) error {
	f.registration, f.registeredAt = registration, at
	return f.err
}

func (f *fakeStore) RecordHeartbeat(_ context.Context, id string, capacity plane.Capacity, at time.Time) error {
	f.heartbeatID, f.heartbeat, f.heartbeatAt = id, capacity, at
	return f.err
}

func (f *fakeStore) SetWorkerDraining(_ context.Context, id string, draining bool) error {
	f.drainingID, f.draining = id, draining
	return f.err
}

func (f *fakeStore) ReservePlacement(_ context.Context, sandboxID string, request PlacementRequest, policy PlacementPolicy, healthyAfter time.Time) (Placement, error) {
	f.sandboxID, f.request, f.policy, f.healthyAfter = sandboxID, request, policy, healthyAfter
	f.reserveCalls++
	if len(f.placements) > 0 {
		placement := f.placements[0]
		f.placements = f.placements[1:]
		return placement, nil
	}
	if len(f.reserveErrors) > 0 {
		err := f.reserveErrors[0]
		f.reserveErrors = f.reserveErrors[1:]
		return f.placement, err
	}
	return f.placement, f.err
}

func (f *fakeStore) GetPlacement(_ context.Context, _ string) (Placement, bool, error) {
	return f.placement, f.placement.WorkerID != "", f.err
}

func (f *fakeStore) UpdatePlacementState(_ context.Context, _, _ string, from []string, state string) error {
	f.updatedFrom, f.updatedState = from, state
	return f.err
}

func (f *fakeStore) PausePlacement(_ context.Context, _, _ string) error {
	f.paused = true
	f.updatedState = "paused"
	return f.err
}

func (f *fakeStore) ReserveResume(_ context.Context, _, _ string, policy PlacementPolicy, healthyAfter time.Time) error {
	f.resumeReserved = true
	f.policy = policy
	f.healthyAfter = healthyAfter
	return f.err
}

func (f *fakeStore) CancelResume(_ context.Context, _, _ string) error {
	f.resumeCanceled = true
	return f.err
}

func (f *fakeStore) ReleasePlacement(_ context.Context, _ string, state string) error {
	f.releaseState = state
	return f.err
}

func (f *fakeStore) ReleaseWorkerPlacement(_ context.Context, _, workerID, state string) error {
	f.releaseWorker, f.releaseState = workerID, state
	return f.err
}

func (f *fakeStore) ListWorkerPlacements(_ context.Context, workerID string, states []string) ([]PlacedSandbox, error) {
	f.listedWorker, f.listedStates = workerID, states
	return f.workerRows, f.err
}

func (f *fakeStore) MarkPlacementLost(_ context.Context, sandboxID, workerID string, from []string, unchangedSince time.Time, reason string) (bool, error) {
	f.lostMarks = append(f.lostMarks, lostMark{sandboxID, workerID, from, unchangedSince, reason})
	return true, f.err
}

type lostMark struct {
	sandboxID, workerID string
	from                []string
	unchangedSince      time.Time
	reason              string
}

type fakeWorkerClient struct {
	response  plane.CreateResponse
	createErr error
	created   plane.CreateRequest
	paused    string
	resumed   string
	destroyed string
	pauseErr  error
	resumeErr error
}

func (f *fakeWorkerClient) Create(_ context.Context, request plane.CreateRequest) (plane.CreateResponse, error) {
	f.created = request
	return f.response, f.createErr
}

func (f *fakeWorkerClient) Pause(_ context.Context, id string) error {
	f.paused = id
	return f.pauseErr
}

func (f *fakeWorkerClient) Resume(_ context.Context, id string) error {
	f.resumed = id
	return f.resumeErr
}

func (f *fakeWorkerClient) Destroy(_ context.Context, id string) error {
	f.destroyed = id
	return nil
}

func TestRegisterWorkerNormalizesRegistration(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(store, 30*time.Second)
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	err := svc.RegisterWorker(context.Background(), WorkerRegistration{
		ID:                  " worker-1 ",
		Endpoint:            "http://worker-1.internal:9876/",
		AllocatableVCPUs:    96,
		AllocatableMemoryMB: 180000,
		AllocatableDiskGB:   1800,
		MaxSandboxes:        1200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.registration.ID != "worker-1" {
		t.Fatalf("unexpected worker id %q", store.registration.ID)
	}
	if store.registration.Endpoint != "http://worker-1.internal:9876" {
		t.Fatalf("unexpected endpoint %q", store.registration.Endpoint)
	}
	if store.registration.Pool != "default" {
		t.Fatalf("unexpected pool %q", store.registration.Pool)
	}
	if len(store.registration.SupportedImages) != 1 || store.registration.SupportedImages[0] != "alpine" {
		t.Fatalf("unexpected supported images %#v", store.registration.SupportedImages)
	}
	if !store.registeredAt.Equal(now) {
		t.Fatalf("unexpected registration time %s", store.registeredAt)
	}
}

func TestRegisterWorkerNormalizesAndDeduplicatesImages(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(store, 30*time.Second)
	err := svc.RegisterWorker(context.Background(), WorkerRegistration{
		ID:                  "worker-1",
		Endpoint:            "http://worker-1.internal:9876",
		AllocatableVCPUs:    8,
		AllocatableMemoryMB: 16384,
		AllocatableDiskGB:   100,
		MaxSandboxes:        100,
		SupportedImages:     []string{" Alpine ", "ubuntu", "alpine"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"alpine", "ubuntu"}
	if len(store.registration.SupportedImages) != len(want) {
		t.Fatalf("supported images = %#v, want %#v", store.registration.SupportedImages, want)
	}
	for i := range want {
		if store.registration.SupportedImages[i] != want[i] {
			t.Fatalf("supported images = %#v, want %#v", store.registration.SupportedImages, want)
		}
	}
}

func TestRegisterWorkerRejectsInvalidCapacity(t *testing.T) {
	svc := NewService(&fakeStore{}, 30*time.Second)
	err := svc.RegisterWorker(context.Background(), WorkerRegistration{
		ID:       "worker-1",
		Endpoint: "http://worker-1.internal:9876",
	})
	if err == nil {
		t.Fatal("expected invalid capacity error")
	}
}

func TestSetWorkerDrainingUpdatesStore(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(store, 30*time.Second)

	if err := svc.SetWorkerDraining(context.Background(), "worker-1", true); err != nil {
		t.Fatal(err)
	}
	if store.drainingID != "worker-1" || !store.draining {
		t.Fatalf("worker=%q draining=%v", store.drainingID, store.draining)
	}
}

func TestPlaceUsesHeartbeatCutoff(t *testing.T) {
	store := &fakeStore{placement: Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://worker-1.internal:9876",
	}}
	svc := NewService(
		store,
		45*time.Second,
		WithPlacementPolicy(PlacementPolicy{
			CPUOvercommitRatio:    2,
			MemoryOvercommitRatio: 1,
		}),
	)
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	placement, err := svc.Place(context.Background(), "sandbox-1", PlacementRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if placement.WorkerID != "worker-1" {
		t.Fatalf("unexpected worker %q", placement.WorkerID)
	}
	if store.request.Pool != "default" {
		t.Fatalf("unexpected pool %q", store.request.Pool)
	}
	if store.policy.CPUOvercommitRatio != 2 || store.policy.MemoryOvercommitRatio != 1 {
		t.Fatalf("unexpected placement policy: %+v", store.policy)
	}
	wantCutoff := now.Add(-45 * time.Second)
	if !store.healthyAfter.Equal(wantCutoff) {
		t.Fatalf("cutoff=%s want=%s", store.healthyAfter, wantCutoff)
	}
}

func TestPlacePropagatesNoCapacity(t *testing.T) {
	svc := NewService(&fakeStore{err: ErrNoCapacity}, 30*time.Second)
	_, err := svc.Place(context.Background(), "sandbox-1", PlacementRequest{})
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("error=%v want ErrNoCapacity", err)
	}
}

func TestPlaceRetriesTemporaryContention(t *testing.T) {
	store := &fakeStore{
		placement: Placement{SandboxID: "sandbox-1", WorkerID: "worker-1"},
		reserveErrors: []error{
			ErrPlacementBusy,
			ErrPlacementBusy,
			nil,
		},
	}
	svc := NewService(store, 30*time.Second)
	var waits []time.Duration
	svc.waitForRetry = func(_ context.Context, ceiling time.Duration) error {
		waits = append(waits, ceiling)
		return nil
	}

	placement, err := svc.Place(context.Background(), "sandbox-1", PlacementRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if placement.WorkerID != "worker-1" {
		t.Fatalf("worker=%q", placement.WorkerID)
	}
	if store.reserveCalls != 3 {
		t.Fatalf("reserve calls=%d want=3", store.reserveCalls)
	}
	if len(waits) != 2 || waits[0] != 5*time.Millisecond || waits[1] != 10*time.Millisecond {
		t.Fatalf("wait ceilings=%v", waits)
	}
}

func TestPlaceBoundsPersistentContention(t *testing.T) {
	store := &fakeStore{err: ErrPlacementBusy}
	svc := NewService(store, 30*time.Second)
	svc.waitForRetry = func(_ context.Context, _ time.Duration) error { return nil }

	_, err := svc.Place(context.Background(), "sandbox-1", PlacementRequest{})
	if !errors.Is(err, ErrPlacementBusy) {
		t.Fatalf("error=%v want ErrPlacementBusy", err)
	}
	if store.reserveCalls != maxPlacementAttempts {
		t.Fatalf("reserve calls=%d want=%d", store.reserveCalls, maxPlacementAttempts)
	}
}

func TestProvisionMarksSandboxActiveAfterWorkerReady(t *testing.T) {
	store := &fakeStore{placement: Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://worker-1.internal:9876",
	}}
	worker := &fakeWorkerClient{response: plane.CreateResponse{SandboxID: "sandbox-1"}}
	service := NewService(
		store,
		time.Minute,
		WithWorkerClientFactory(func(string) WorkerClient { return worker }),
	)

	_, err := service.Provision(context.Background(), ProvisionRequest{
		CreateRequest: plane.CreateRequest{SandboxID: "sandbox-1", VCPUs: 2, MemoryMB: 1024, DiskGB: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.created.SandboxID != "sandbox-1" {
		t.Fatalf("worker received sandbox id %q", worker.created.SandboxID)
	}
	if store.updatedState != "active" {
		t.Fatalf("state=%q want active", store.updatedState)
	}
}

func TestProvisionFailureReleasesCapacityAndMarksError(t *testing.T) {
	store := &fakeStore{placement: Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://worker-1.internal:9876",
	}}
	worker := &fakeWorkerClient{createErr: errors.New("guest agent did not become ready")}
	service := NewService(
		store,
		time.Minute,
		WithWorkerClientFactory(func(string) WorkerClient { return worker }),
	)

	_, err := service.Provision(context.Background(), ProvisionRequest{
		CreateRequest: plane.CreateRequest{SandboxID: "sandbox-1"},
	})
	if err == nil {
		t.Fatal("expected provisioning failure")
	}
	if store.releaseState != "error" {
		t.Fatalf("release state=%q want error", store.releaseState)
	}
}

func TestProvisionRetriesAnotherWorkerAfterLocalNoCapacity(t *testing.T) {
	store := &fakeStore{placements: []Placement{
		{SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://worker-1:9876"},
		{SandboxID: "sandbox-1", WorkerID: "worker-2", Endpoint: "http://worker-2:9876"},
	}}
	workers := map[string]*fakeWorkerClient{
		"http://worker-1:9876": {createErr: plane.ErrNoCapacity},
		"http://worker-2:9876": {response: plane.CreateResponse{SandboxID: "sandbox-1"}},
	}
	service := NewService(
		store,
		time.Minute,
		WithWorkerClientFactory(func(endpoint string) WorkerClient { return workers[endpoint] }),
	)

	placement, err := service.Provision(context.Background(), ProvisionRequest{
		CreateRequest: plane.CreateRequest{
			SandboxID: "sandbox-1", VCPUs: 1, MemoryMB: 128, DiskGB: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if placement.WorkerID != "worker-2" {
		t.Fatalf("worker=%q want worker-2", placement.WorkerID)
	}
	if store.reserveCalls != 2 {
		t.Fatalf("placement attempts=%d want=2", store.reserveCalls)
	}
	if len(store.request.ExcludedWorkerIDs) != 1 || store.request.ExcludedWorkerIDs[0] != "worker-1" {
		t.Fatalf("excluded workers=%v", store.request.ExcludedWorkerIDs)
	}
}

func TestDestroyedWorkerEventReleasesOnlyReportedPlacement(t *testing.T) {
	store := &fakeStore{}
	service := NewService(store, time.Minute)

	if err := service.ReportWorkerState(context.Background(), "worker-1", "sandbox-1", "destroyed"); err != nil {
		t.Fatal(err)
	}
	if store.releaseWorker != "worker-1" || store.releaseState != "destroyed" {
		t.Fatalf("worker=%q state=%q", store.releaseWorker, store.releaseState)
	}
}

func TestLifecycleCommandsReachPlacedWorkerAndUpdateState(t *testing.T) {
	newService := func() (*Service, *fakeStore, *fakeWorkerClient) {
		store := &fakeStore{placement: Placement{
			SandboxID: "sandbox-1",
			WorkerID:  "worker-1",
			Endpoint:  "http://worker-1.internal:9876",
		}}
		worker := &fakeWorkerClient{}
		service := NewService(
			store,
			time.Minute,
			WithWorkerClientFactory(func(string) WorkerClient { return worker }),
		)
		return service, store, worker
	}

	pauseService, pauseStore, pauseWorker := newService()
	if err := pauseService.Pause(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if pauseWorker.paused != "sandbox-1" || !pauseStore.paused || pauseStore.updatedState != "paused" {
		t.Fatalf("pause worker=%q state=%q", pauseWorker.paused, pauseStore.updatedState)
	}

	resumeService, resumeStore, resumeWorker := newService()
	if err := resumeService.Resume(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if resumeWorker.resumed != "sandbox-1" || !resumeStore.resumeReserved || resumeStore.updatedState != "active" {
		t.Fatalf("resume worker=%q state=%q", resumeWorker.resumed, resumeStore.updatedState)
	}

	destroyService, destroyStore, destroyWorker := newService()
	if err := destroyService.Destroy(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if destroyWorker.destroyed != "sandbox-1" ||
		destroyStore.updatedState != "destroying" ||
		destroyStore.releaseState != "destroyed" {
		t.Fatalf(
			"destroy worker=%q transition=%q final=%q",
			destroyWorker.destroyed,
			destroyStore.updatedState,
			destroyStore.releaseState,
		)
	}
}

func TestResumeFailurePausesWorkerBeforeReleasingReservation(t *testing.T) {
	store := &fakeStore{placement: Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://worker-1.internal:9876",
		State:     "paused",
	}}
	worker := &fakeWorkerClient{resumeErr: errors.New("restore timed out")}
	service := NewService(
		store,
		time.Minute,
		WithWorkerClientFactory(func(string) WorkerClient { return worker }),
	)

	err := service.Resume(context.Background(), "sandbox-1")
	if err == nil {
		t.Fatal("expected resume failure")
	}
	if !store.resumeReserved || !store.resumeCanceled {
		t.Fatalf("reserved=%v canceled=%v", store.resumeReserved, store.resumeCanceled)
	}
	if worker.resumed != "sandbox-1" || worker.paused != "sandbox-1" {
		t.Fatalf("resume=%q cleanup pause=%q", worker.resumed, worker.paused)
	}
}

func TestResumeCleanupFailureKeepsReservation(t *testing.T) {
	store := &fakeStore{placement: Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://worker-1.internal:9876",
		State:     "paused",
	}}
	worker := &fakeWorkerClient{
		resumeErr: errors.New("restore timed out"),
		pauseErr:  errors.New("worker unreachable"),
	}
	service := NewService(
		store,
		time.Minute,
		WithWorkerClientFactory(func(string) WorkerClient { return worker }),
	)

	err := service.Resume(context.Background(), "sandbox-1")
	if err == nil {
		t.Fatal("expected resume failure")
	}
	if store.resumeCanceled {
		t.Fatal("capacity must remain reserved when cleanup cannot confirm the VM is paused")
	}
}

func TestDestroyPausedPlacementSkipsComputeDestroyingTransition(t *testing.T) {
	store := &fakeStore{placement: Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://worker-1.internal:9876",
		State:     "paused",
	}}
	worker := &fakeWorkerClient{}
	service := NewService(
		store,
		time.Minute,
		WithWorkerClientFactory(func(string) WorkerClient { return worker }),
	)

	if err := service.Destroy(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if store.updatedState == "destroying" {
		t.Fatal("paused placement must stay paused until disk-only release")
	}
	if store.releaseState != "destroyed" {
		t.Fatalf("release state=%q", store.releaseState)
	}
}
