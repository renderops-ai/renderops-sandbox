package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
)

type fakeRows struct {
	rows      map[string]platform.SandboxRef
	inserted  []platform.Sandbox
	insertErr error
	// raceOwner, when set, makes the insert fail as if another create of the
	// same ID won, leaving a row owned by raceOwner.
	raceOwner string
}

func (f *fakeRows) GetSandbox(_ context.Context, id string) (platform.SandboxRef, bool, error) {
	ref, ok := f.rows[id]
	return ref, ok, nil
}

func (f *fakeRows) InsertSandbox(_ context.Context, sb platform.Sandbox) error {
	if f.raceOwner != "" {
		f.rows[sb.ID] = platform.SandboxRef{ID: sb.ID, UserID: f.raceOwner, State: "active"}
		return errors.New("duplicate key value violates unique constraint")
	}
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserted = append(f.inserted, sb)
	f.rows[sb.ID] = platform.SandboxRef{ID: sb.ID, UserID: sb.UserID, State: sb.State}
	return nil
}

type fakeProvisioner struct {
	requests []orchestrator.ProvisionRequest
	err      error
}

func (f *fakeProvisioner) Provision(_ context.Context, request orchestrator.ProvisionRequest) (orchestrator.Placement, error) {
	f.requests = append(f.requests, request)
	return orchestrator.Placement{SandboxID: request.SandboxID, State: "active"}, f.err
}

const testSandboxID = "5be67472-8ab9-4383-9d01-3aa08f97eb42"

func agentSpec() SandboxSpec {
	return SandboxSpec{
		ID: testSandboxID, UserID: "user-1", Name: "agent-session", BillingModel: "payg", Image: "agents",
		VCPUs: 2, MemoryMB: 4096, DiskGB: 20, Internet: true,
		IdleTimeout: 20 * time.Minute, MaxLifetime: 24 * time.Hour,
	}
}

func TestCreateWithIDInsertsAndProvisionsTheChosenID(t *testing.T) {
	rows := &fakeRows{rows: map[string]platform.SandboxRef{}}
	orch := &fakeProvisioner{}
	info, err := createWithID(context.Background(), rows, orch, agentSpec())
	if err != nil {
		t.Fatalf("createWithID: %v", err)
	}
	if len(rows.inserted) != 1 || rows.inserted[0].ID != testSandboxID || rows.inserted[0].State != "scheduling" ||
		rows.inserted[0].Name != "agent-session" || rows.inserted[0].IdleTimeoutMs != 20*60*1000 {
		t.Fatalf("inserted = %+v", rows.inserted)
	}
	if len(orch.requests) != 1 {
		t.Fatalf("provision calls = %d, want 1", len(orch.requests))
	}
	req := orch.requests[0]
	if req.SandboxID != testSandboxID || req.Image != "agents" || req.MemoryMB != 4096 || !req.Internet ||
		req.IdleTimeoutS != 1200 || req.MaxLifetimeS != 86400 {
		t.Fatalf("provision request = %+v", req.CreateRequest)
	}
	if info.ID != testSandboxID || info.State != plane.StateActive {
		t.Fatalf("info = %+v", info)
	}
}

func TestCreateWithIDReturnsExistingSandboxOfTheSameOwner(t *testing.T) {
	for _, state := range []string{"active", "paused"} {
		rows := &fakeRows{rows: map[string]platform.SandboxRef{
			testSandboxID: {ID: testSandboxID, UserID: "user-1", State: state, Image: "agents", MemoryMB: 4096},
		}}
		orch := &fakeProvisioner{}
		info, err := createWithID(context.Background(), rows, orch, agentSpec())
		if err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		if len(rows.inserted) != 0 || len(orch.requests) != 0 {
			t.Fatalf("%s: created again (inserts %d, provisions %d)", state, len(rows.inserted), len(orch.requests))
		}
		want := plane.StateActive
		if state == "paused" {
			want = plane.StatePaused
		}
		if info.State != want || info.ID != testSandboxID {
			t.Fatalf("%s: info = %+v", state, info)
		}
	}
}

func TestCreateWithIDRetriesInterruptedProvisioning(t *testing.T) {
	for _, state := range []string{"scheduling", "error"} {
		rows := &fakeRows{rows: map[string]platform.SandboxRef{
			testSandboxID: {ID: testSandboxID, UserID: "user-1", State: state},
		}}
		orch := &fakeProvisioner{}
		if _, err := createWithID(context.Background(), rows, orch, agentSpec()); err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		if len(rows.inserted) != 0 || len(orch.requests) != 1 {
			t.Fatalf("%s: inserts %d, provisions %d; want 0, 1", state, len(rows.inserted), len(orch.requests))
		}
	}
}

func TestCreateWithIDRefusesUnusableRows(t *testing.T) {
	cases := []struct {
		ref  platform.SandboxRef
		want error
	}{
		{platform.SandboxRef{ID: testSandboxID, UserID: "someone-else", State: "active"}, ErrSandboxIDConflict},
		{platform.SandboxRef{ID: testSandboxID, UserID: "user-1", State: "destroyed"}, ErrSandboxGone},
		{platform.SandboxRef{ID: testSandboxID, UserID: "user-1", State: "destroying"}, ErrSandboxGone},
		{platform.SandboxRef{ID: testSandboxID, UserID: "user-1", State: "provisioning"}, ErrSandboxBusy},
		{platform.SandboxRef{ID: testSandboxID, UserID: "user-1", State: "resuming"}, ErrSandboxBusy},
	}
	for _, c := range cases {
		rows := &fakeRows{rows: map[string]platform.SandboxRef{testSandboxID: c.ref}}
		orch := &fakeProvisioner{}
		_, err := createWithID(context.Background(), rows, orch, agentSpec())
		if !errors.Is(err, c.want) {
			t.Fatalf("%s/%s: error = %v, want %v", c.ref.UserID, c.ref.State, err, c.want)
		}
		if len(orch.requests) != 0 {
			t.Fatalf("%s/%s: provisioned", c.ref.UserID, c.ref.State)
		}
	}
}

func TestCreateWithIDLosingAnInsertRaceChecksTheOwner(t *testing.T) {
	rows := &fakeRows{rows: map[string]platform.SandboxRef{}, raceOwner: "someone-else"}
	orch := &fakeProvisioner{}
	if _, err := createWithID(context.Background(), rows, orch, agentSpec()); !errors.Is(err, ErrSandboxIDConflict) {
		t.Fatalf("error = %v, want ErrSandboxIDConflict", err)
	}
	if len(orch.requests) != 0 {
		t.Fatal("provisioned a sandbox owned by someone else")
	}
}

func TestCreateWithIDValidatesInput(t *testing.T) {
	for name, mutate := range map[string]func(*SandboxSpec){
		"not a uuid": func(s *SandboxSpec) { s.ID = "sandbox-1" },
		"no owner":   func(s *SandboxSpec) { s.UserID = " " },
		"bad image":  func(s *SandboxSpec) { s.Image = "../agents" },
	} {
		spec := agentSpec()
		mutate(&spec)
		rows := &fakeRows{rows: map[string]platform.SandboxRef{}}
		if _, err := createWithID(context.Background(), rows, &fakeProvisioner{}, spec); err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if len(rows.inserted) != 0 {
			t.Fatalf("%s: inserted a row", name)
		}
	}
}

func TestCreateWithIDNormalizesUUIDCase(t *testing.T) {
	rows := &fakeRows{rows: map[string]platform.SandboxRef{
		testSandboxID: {ID: testSandboxID, UserID: "user-1", State: "active"},
	}}
	spec := agentSpec()
	spec.ID = "5BE67472-8AB9-4383-9D01-3AA08F97EB42"
	info, err := createWithID(context.Background(), rows, &fakeProvisioner{}, spec)
	if err != nil || info.ID != testSandboxID || len(rows.inserted) != 0 {
		t.Fatalf("info = %+v, err = %v, inserts = %d", info, err, len(rows.inserted))
	}
}

func TestCreateWithIDReturnsProvisionFailure(t *testing.T) {
	rows := &fakeRows{rows: map[string]platform.SandboxRef{}}
	orch := &fakeProvisioner{err: orchestrator.ErrNoCapacity}
	if _, err := createWithID(context.Background(), rows, orch, agentSpec()); !errors.Is(err, orchestrator.ErrNoCapacity) {
		t.Fatalf("error = %v, want ErrNoCapacity", err)
	}
}
