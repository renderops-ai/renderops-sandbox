package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"backend/internal/orchestrator"
)

type fakeOrchestrator struct {
	placement    orchestrator.Placement
	placementErr error
	resumeErr    error
	calls        []string
}

func (f *fakeOrchestrator) Placement(_ context.Context, sandboxID string) (orchestrator.Placement, error) {
	f.calls = append(f.calls, "placement "+sandboxID)
	return f.placement, f.placementErr
}

func (f *fakeOrchestrator) Resume(_ context.Context, sandboxID string) error {
	f.calls = append(f.calls, "resume "+sandboxID)
	return f.resumeErr
}

func TestActivePlacementResumesPausedSandbox(t *testing.T) {
	orch := &fakeOrchestrator{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://worker-1:19876", State: "paused",
	}}

	placement, err := activePlacement(context.Background(), orch, "sandbox-1")
	if err != nil {
		t.Fatalf("activePlacement: %v", err)
	}
	if got, want := strings.Join(orch.calls, ", "), "placement sandbox-1, resume sandbox-1"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
	if placement.Endpoint != "http://worker-1:19876" || placement.State != "active" {
		t.Fatalf("placement = %+v", placement)
	}
}

func TestActivePlacementDoesNotResumeActiveSandbox(t *testing.T) {
	orch := &fakeOrchestrator{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://worker-1:19876", State: "active",
	}}

	if _, err := activePlacement(context.Background(), orch, "sandbox-1"); err != nil {
		t.Fatalf("activePlacement: %v", err)
	}
	if got, want := strings.Join(orch.calls, ", "), "placement sandbox-1"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}

// A failed resume must stop the command from reaching the worker, which would
// otherwise restore the VM on its own.
func TestActivePlacementReturnsResumeFailure(t *testing.T) {
	orch := &fakeOrchestrator{
		placement: orchestrator.Placement{SandboxID: "sandbox-1", Endpoint: "http://worker-1:19876", State: "paused"},
		resumeErr: fmt.Errorf("%w: worker full", orchestrator.ErrNoCapacity),
	}

	placement, err := activePlacement(context.Background(), orch, "sandbox-1")
	if !errors.Is(err, orchestrator.ErrNoCapacity) {
		t.Fatalf("error = %v, want ErrNoCapacity", err)
	}
	if placement.Endpoint != "" {
		t.Fatalf("placement = %+v, want none", placement)
	}
}

func TestActivePlacementReturnsPlacementFailure(t *testing.T) {
	orch := &fakeOrchestrator{placementErr: orchestrator.ErrSandboxNotFound}

	if _, err := activePlacement(context.Background(), orch, "sandbox-1"); !errors.Is(err, orchestrator.ErrSandboxNotFound) {
		t.Fatalf("error = %v, want ErrSandboxNotFound", err)
	}
	if got, want := strings.Join(orch.calls, ", "), "placement sandbox-1"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}

// TestActivePlacementResumesThroughOrchestratorAPI runs the real orchestrator
// client against a stub orchestrator: the paused state must survive the
// placement JSON and the resume must hit the same route as POST
// /session/{id}/resume before the worker is used.
func TestActivePlacementResumesThroughOrchestratorAPI(t *testing.T) {
	var mu sync.Mutex
	state := "paused"
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/placements/sandbox-1":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(orchestrator.Placement{
				SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://worker-1:19876", State: state,
			}); err != nil {
				t.Errorf("encode placement: %v", err)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/internal/sandboxes/sandbox-1/resume":
			state = "active"
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	placement, err := activePlacement(context.Background(), orchestrator.NewClient(server.URL, "secret"), "sandbox-1")
	if err != nil {
		t.Fatalf("activePlacement: %v", err)
	}
	if placement.Endpoint != "http://worker-1:19876" {
		t.Fatalf("placement = %+v", placement)
	}

	mu.Lock()
	defer mu.Unlock()
	want := "GET /internal/placements/sandbox-1, POST /internal/sandboxes/sandbox-1/resume"
	if got := strings.Join(requests, ", "); got != want {
		t.Fatalf("requests = %q, want %q", got, want)
	}
	if state != "active" {
		t.Fatalf("state = %q, want active", state)
	}
}
