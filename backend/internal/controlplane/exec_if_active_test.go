package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"backend/internal/agent"
	"backend/internal/orchestrator"
	"backend/internal/plane"
)

type fakeWorkerExecer struct {
	endpoint string
	requests []plane.ExecRequest
	err      error
}

func (f *fakeWorkerExecer) Exec(_ context.Context, _ string, req plane.ExecRequest) (plane.ExecResult, error) {
	f.requests = append(f.requests, req)
	return plane.ExecResult{Stdout: "ok\n"}, f.err
}

func TestExecIfActiveNeverResumes(t *testing.T) {
	for _, state := range []string{"paused", "resuming", "provisioning"} {
		orch := &fakeOrchestrator{placement: orchestrator.Placement{
			SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://worker-1:19876", State: state,
		}}
		worker := &fakeWorkerExecer{}
		_, err := execIfActive(context.Background(), orch, func(string) workerExecer { return worker }, "sandbox-1", plane.ExecRequest{Command: "true"})
		if !errors.Is(err, plane.ErrSandboxNotActive) {
			t.Fatalf("%s: error = %v, want ErrSandboxNotActive", state, err)
		}
		if got := strings.Join(orch.calls, ", "); got != "placement sandbox-1" || len(worker.requests) != 0 {
			t.Fatalf("%s: calls = %q, worker requests = %d", state, got, len(worker.requests))
		}
	}
}

func TestExecIfActiveRunsOnTheSandboxWorker(t *testing.T) {
	orch := &fakeOrchestrator{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://worker-1:19876", State: "active",
	}}
	worker := &fakeWorkerExecer{}
	req := plane.ExecRequest{Command: "true", TimeoutS: 30, ActiveOnly: true, Passive: true}
	res, err := execIfActive(context.Background(), orch, func(endpoint string) workerExecer {
		worker.endpoint = endpoint
		return worker
	}, "sandbox-1", req)
	if err != nil || res.Stdout != "ok\n" {
		t.Fatalf("exec = %+v, %v", res, err)
	}
	if worker.endpoint != "http://worker-1:19876" || len(worker.requests) != 1 || worker.requests[0] != req {
		t.Fatalf("worker %q got %+v", worker.endpoint, worker.requests)
	}

	orch.placementErr = orchestrator.ErrSandboxNotFound
	if _, err := execIfActive(context.Background(), orch, func(string) workerExecer { return worker }, "sandbox-1", req); !errors.Is(err, orchestrator.ErrSandboxNotFound) {
		t.Fatalf("placement failure = %v", err)
	}
}

// The worker refuses a sandbox that paused after the placement check; the
// real worker client turns its 409 back into ErrSandboxNotActive.
func TestExecIfActiveReportsAWorkerThatSawThePause(t *testing.T) {
	var got plane.ExecRequest
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode exec request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		if err := json.NewEncoder(w).Encode(plane.ErrorResponse{Code: "sandbox_not_active", Error: "session sandbox-1 is paused"}); err != nil {
			t.Errorf("encode: %v", err)
		}
	}))
	defer worker.Close()
	orch := &fakeOrchestrator{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: worker.URL, State: "active",
	}}
	_, err := execIfActive(context.Background(), orch, func(endpoint string) workerExecer {
		return agent.NewClient(endpoint, "secret")
	}, "sandbox-1", plane.ExecRequest{Command: "true", ActiveOnly: true, Passive: true})
	if !errors.Is(err, plane.ErrSandboxNotActive) {
		t.Fatalf("error = %v, want ErrSandboxNotActive", err)
	}
	if !got.ActiveOnly || !got.Passive {
		t.Fatalf("worker got %+v, want active_only and passive", got)
	}
}
