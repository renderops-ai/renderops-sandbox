package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"backend/internal/orchestrator"
	"backend/internal/plane"
)

// The Service's exec methods send their options to the sandbox's worker. The
// worker answers every command with an error, so no method reaches the
// database (the Service has none here).
func TestServiceSendsExecOptionsToTheWorker(t *testing.T) {
	var mu sync.Mutex
	var got []plane.ExecRequest
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req plane.ExecRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode exec request: %v", err)
		}
		mu.Lock()
		got = append(got, req)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		if err := json.NewEncoder(w).Encode(plane.ErrorResponse{Code: "exec_failed", Error: "guest unavailable"}); err != nil {
			t.Errorf("encode: %v", err)
		}
	}))
	defer worker.Close()
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/placements/sandbox-1" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(orchestrator.Placement{
			SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: worker.URL, State: "active",
		}); err != nil {
			t.Errorf("encode placement: %v", err)
		}
	}))
	defer orch.Close()
	svc := &Service{orchestrator: orchestrator.NewClient(orch.URL, "secret"), workerToken: "secret"}
	ctx := context.Background()

	if _, err := svc.Exec(ctx, "sandbox-1", "ls", 5); err == nil {
		t.Fatal("Exec: want the worker's error")
	}
	if _, err := svc.ExecQuiet(ctx, "sandbox-1", "cat notes.txt", 5); err == nil {
		t.Fatal("ExecQuiet: want the worker's error")
	}
	if _, err := svc.ExecIfActive(ctx, "sandbox-1", "cat notes.txt", 5, plane.ExecOptions{Passive: true, Quiet: true}); err == nil {
		t.Fatal("ExecIfActive: want the worker's error")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []plane.ExecRequest{
		{Command: "ls", TimeoutS: 5},
		{Command: "cat notes.txt", TimeoutS: 5, Quiet: true},
		{Command: "cat notes.txt", TimeoutS: 5, ActiveOnly: true, Passive: true, Quiet: true},
	}
	if len(got) != len(want) {
		t.Fatalf("worker got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("request %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
