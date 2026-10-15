package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"backend/internal/plane"
)

func TestExecClientAllowsCommandTimeoutPlusMargin(t *testing.T) {
	client := NewClient("http://worker.internal:19876", "secret")
	for _, tc := range []struct {
		timeoutS int
		want     time.Duration
	}{
		{timeoutS: 0, want: 60 * time.Second},  // worker default (30s) fits the regular client
		{timeoutS: 10, want: 60 * time.Second}, // short commands keep the regular client
		{timeoutS: 90, want: 90*time.Second + ExecMargin},
		{timeoutS: 300, want: 300*time.Second + ExecMargin}, // policy max
		{timeoutS: 900, want: 900*time.Second + ExecMargin}, // git workspace checkout
	} {
		if got := client.execClient(tc.timeoutS).Timeout; got != tc.want {
			t.Errorf("timeout_s=%d: client timeout %v, want %v", tc.timeoutS, got, tc.want)
		}
	}
}

// TestExecOutlastsRegularClientTimeout shortens the regular client timeout so a
// worker that answers after it still completes an exec with a longer timeout,
// while a call on the regular client (run) is cut off.
func TestExecOutlastsRegularClientTimeout(t *testing.T) {
	const workerDelay = 200 * time.Millisecond
	var gotTimeoutS atomic.Int64
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(plane.AuthHeader) != "secret" {
			t.Errorf("missing worker token")
		}
		if r.URL.Path == plane.RouteSandboxPrefix+"sandbox-1/exec" {
			var req plane.ExecRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode exec request: %v", err)
			}
			gotTimeoutS.Store(int64(req.TimeoutS))
		}
		time.Sleep(workerDelay)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(plane.ExecResult{Stdout: "done\n", TerminationReason: "success"}); err != nil {
			// Expected for the run call: its client has already given up.
			t.Logf("encode response: %v", err)
		}
	}))
	defer worker.Close()

	client := NewClient(worker.URL, "secret")
	client.http.Timeout = workerDelay / 4

	res, err := client.Exec(context.Background(), "sandbox-1", plane.ExecRequest{Command: "sleep 90", TimeoutS: 90})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if res.Stdout != "done\n" {
		t.Fatalf("stdout=%q", res.Stdout)
	}
	if got := gotTimeoutS.Load(); got != 90 {
		t.Fatalf("worker got timeout_s=%d, want 90", got)
	}

	if _, err := client.Run(context.Background(), "sandbox-1", plane.RunRequest{Code: "1", Language: "python"}); err == nil {
		t.Fatal("run on the regular client should time out before the worker answers")
	}
}
