package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"backend/internal/plane"
)

func TestReconcileDecision(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	complete := WorkerInventory{Complete: true}
	durable := WorkerInventory{Complete: true, DurableCheckpoints: true}
	incomplete := WorkerInventory{Complete: false}
	tests := []struct {
		name      string
		state     string
		age       time.Duration
		inventory WorkerInventory
		want      bool
	}{
		{"missing active row", "active", time.Hour, complete, true},
		{"missing active row, worker recovery incomplete", "active", time.Hour, incomplete, true},
		{"recently changed active row", "active", time.Minute, complete, false},
		{"young resuming row", "resuming", 5 * time.Minute, complete, false},
		{"stale resuming row", "resuming", time.Hour, complete, true},
		{"young provisioning row", "provisioning", 10 * time.Minute, complete, false},
		{"stale provisioning row", "provisioning", time.Hour, complete, true},
		{"paused row with durable checkpoints", "paused", 24 * time.Hour, durable, false},
		{"paused row, local checkpoints only", "paused", 24 * time.Hour, complete, true},
		{"paused row, worker recovery incomplete", "paused", 24 * time.Hour, incomplete, false},
		{"recently paused row", "paused", 30 * time.Second, complete, false},
		{"destroying row", "destroying", time.Hour, complete, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := PlacedSandbox{ID: "s", State: tt.state, UpdatedAt: now.Add(-tt.age)}
			if got, why := reconcileDecision(row, now, tt.inventory); got != tt.want {
				t.Fatalf("reconcileDecision() = %v (%s), want %v", got, why, tt.want)
			}
		})
	}
}

func TestReconcileWorkerMarksMissingRowsLost(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	ghost := "5d8941b1-4f89-5d6c-9354-02809b1bd130"
	store := &fakeStore{workerRows: []PlacedSandbox{
		{ID: ghost, State: "active", UpdatedAt: now.Add(-14 * time.Hour)},
		{ID: "held-active", State: "active", UpdatedAt: now.Add(-time.Hour)},
		{ID: "held-paused", State: "paused", UpdatedAt: now.Add(-time.Hour)},
		{ID: "creating", State: "provisioning", UpdatedAt: now.Add(-30 * time.Second)},
		{ID: "checkpointed", State: "paused", UpdatedAt: now.Add(-48 * time.Hour)},
		{ID: "crashed", State: "active", UpdatedAt: now.Add(-time.Hour)},
	}}
	service := NewService(store, time.Minute)
	service.now = func() time.Time { return now }

	result, err := service.ReconcileWorker(context.Background(), "worker-2", WorkerInventory{
		Sandboxes:          []string{"held-active", "HELD-PAUSED"},
		Lost:               []LostSandbox{{ID: "crashed", Reason: "VM crashed (out of memory)"}},
		DurableCheckpoints: true,
		Complete:           true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.listedWorker != "worker-2" || !slices.Equal(store.listedStates, reconcileStates) {
		t.Fatalf("listed worker=%q states=%v", store.listedWorker, store.listedStates)
	}
	if !slices.Equal(result.MarkedLost, []string{ghost}) {
		t.Fatalf("marked lost = %v, want only the ghost", result.MarkedLost)
	}
	if len(store.lostMarks) != 2 {
		t.Fatalf("lost marks = %+v", store.lostMarks)
	}
	reported := store.lostMarks[0]
	if reported.sandboxID != "crashed" || reported.reason != "VM crashed (out of memory)" ||
		!slices.Equal(reported.from, workerLostFromStates) || !reported.unchangedSince.IsZero() {
		t.Fatalf("worker-reported lost mark = %+v", reported)
	}
	missing := store.lostMarks[1]
	if missing.sandboxID != ghost || missing.workerID != "worker-2" || missing.reason != ReasonNotOnWorker ||
		!slices.Equal(missing.from, []string{"active"}) || !missing.unchangedSince.Equal(now.Add(-14*time.Hour)) {
		t.Fatalf("reconciled lost mark = %+v", missing)
	}
}

func TestPauseOnLostWorkerSandboxMarksLost(t *testing.T) {
	store := &fakeStore{placement: Placement{SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://w", State: "active"}}
	worker := &fakeWorkerClient{pauseErr: &plane.LostError{Reason: "VM crashed (out of memory)"}}
	service := NewService(store, time.Minute, WithWorkerClientFactory(func(string) WorkerClient { return worker }))

	err := service.Pause(context.Background(), "sandbox-1")
	if !errors.Is(err, plane.ErrSandboxLost) {
		t.Fatalf("pause error = %v", err)
	}
	if store.paused {
		t.Fatal("lost sandbox recorded as paused")
	}
	if len(store.lostMarks) != 1 || store.lostMarks[0].reason != "VM crashed (out of memory)" {
		t.Fatalf("lost marks = %+v", store.lostMarks)
	}
}

func TestResumeOfLostSandboxSkipsCleanupPause(t *testing.T) {
	store := &fakeStore{placement: Placement{SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://w", State: "paused"}}
	worker := &fakeWorkerClient{resumeErr: &plane.LostError{Reason: "VM crashed"}}
	service := NewService(store, time.Minute, WithWorkerClientFactory(func(string) WorkerClient { return worker }))

	err := service.Resume(context.Background(), "sandbox-1")
	if !errors.Is(err, plane.ErrSandboxLost) {
		t.Fatalf("resume error = %v", err)
	}
	if worker.paused != "" || store.resumeCanceled {
		t.Fatalf("cleanup ran for a lost sandbox: paused=%q canceled=%v", worker.paused, store.resumeCanceled)
	}
	if len(store.lostMarks) != 1 || !slices.Contains(store.lostMarks[0].from, "resuming") {
		t.Fatalf("lost marks = %+v", store.lostMarks)
	}
}

func TestHTTPServerRecordsLostSandboxAndMaps410(t *testing.T) {
	store := &fakeStore{}
	server := NewHTTPServer(NewService(store, time.Minute), "control", "worker").Handler()

	request := httptest.NewRequest(http.MethodPost, "/internal/workers/worker-2/sandboxes/sandbox-1/state",
		strings.NewReader(`{"state":"lost","reason":"VM crashed (out of memory)"}`))
	request.Header.Set(AuthHeader, "worker")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if len(store.lostMarks) != 1 || store.lostMarks[0].reason != "VM crashed (out of memory)" || store.lostMarks[0].workerID != "worker-2" {
		t.Fatalf("lost marks = %+v", store.lostMarks)
	}

	inventory := httptest.NewRequest(http.MethodPost, "/internal/workers/worker-2/inventory",
		strings.NewReader(`{"sandboxes":[],"durable_checkpoints":true,"complete":true}`))
	inventory.Header.Set(AuthHeader, "worker")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, inventory)
	if response.Code != http.StatusOK || store.listedWorker != "worker-2" {
		t.Fatalf("inventory status = %d body=%s", response.Code, response.Body.String())
	}

	recorder := httptest.NewRecorder()
	writeServiceError(recorder, &plane.LostError{Reason: "VM crashed (out of memory)"})
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusGone || body["code"] != "sandbox_lost" || body["reason"] != "VM crashed (out of memory)" {
		t.Fatalf("status=%d body=%v", recorder.Code, body)
	}
}

func TestClientMapsSandboxLost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeServiceError(w, &plane.LostError{Reason: "not found on worker after restart"})
	}))
	defer server.Close()

	err := NewClient(server.URL, "token").Pause(context.Background(), "sandbox-1")
	var lost *plane.LostError
	if !errors.As(err, &lost) || lost.Reason != "not found on worker after restart" {
		t.Fatalf("pause error = %v", err)
	}
}
