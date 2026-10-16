package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/plane"
	"backend/internal/platform"
)

// quietTestServer serves the session API over svc and records usage logs,
// runs and sandbox log rows. It returns the server and an owned session.
func quietTestServer(t *testing.T, svc plane.Service) (http.Handler, *fakePlatformService, string) {
	t.Helper()
	resolver := &fakePlatformService{
		record:       platform.KeyRecord{ID: "key-1", UserID: "tenant-1", IsActive: true, BalanceUSD: 10},
		logCh:        make(chan error, 8),
		runCh:        make(chan platform.SandboxRun, 8),
		sandboxLogCh: make(chan platform.SandboxLog, 8),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/session", handler.SessionHandler(svc, resolver))
	mux.HandleFunc("/session/", handler.SessionHandler(svc, resolver))
	server := middleware.Logging(middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux))

	req := httptest.NewRequest(http.MethodPost, "/session", nil)
	req.Header.Set("Authorization", "Bearer ro_live_pro-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var created handler.CreateSessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	waitUsageLog(t, resolver) // the create's
	return server, resolver, created.Session.SessionID
}

func postSessionExec(server http.Handler, sessionID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/session/"+sessionID+"/exec", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer ro_live_pro-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec
}

// waitUsageLog returns the usage log written next.
func waitUsageLog(t *testing.T, f *fakePlatformService) platform.UsageLog {
	t.Helper()
	select {
	case <-f.logCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a usage log")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logs[len(f.logs)-1]
}

func waitRun(t *testing.T, f *fakePlatformService) platform.SandboxRun {
	t.Helper()
	select {
	case run := <-f.runCh:
		return run
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a run")
		return platform.SandboxRun{}
	}
}

// A quiet exec returns its output but copies it into neither usage_logs nor
// sandbox_logs, even when the worker's log feed is off and the handler would
// otherwise write sandbox_logs itself. The run is still recorded.
func TestSessionExecQuietKeepsOutputOutOfLogs(t *testing.T) {
	svc := newFakeSessionService()
	server, resolver, sessionID := quietTestServer(t, svc)

	rec := postSessionExec(server, sessionID, `{"command":"cat notes.txt","quiet":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("quiet exec: %d %s", rec.Code, rec.Body.String())
	}
	var resp handler.SessionExecuteResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Result == nil || resp.Result.Stdout != "exec output\n" || resp.Result.Stderr != "exec warning\n" {
		t.Fatalf("quiet exec must still return its output: %+v", resp.Result)
	}
	svc.mu.Lock()
	quiet, plain := append([]string(nil), svc.quietCommands...), len(svc.execTimeouts)
	svc.mu.Unlock()
	if len(quiet) != 1 || quiet[0] != "cat notes.txt" || plain != 0 {
		t.Fatalf("quiet commands = %q, plain execs = %d", quiet, plain)
	}
	if usage := waitUsageLog(t, resolver); usage.ExecutionType != "session_exec" || usage.Stdout != "" || usage.Stderr != "" {
		t.Fatalf("quiet exec usage log = %+v, want no output", usage)
	}
	if run := waitRun(t, resolver); run.Command != "cat notes.txt" || run.Kind != "exec" {
		t.Fatalf("quiet exec run = %+v", run)
	}
	// The handler writes a run's sandbox_logs rows right after the run.
	select {
	case row := <-resolver.sandboxLogCh:
		t.Fatalf("quiet exec output reached sandbox_logs: %+v", row)
	case <-time.After(200 * time.Millisecond):
	}

	// The same command without quiet is logged as before.
	if rec := postSessionExec(server, sessionID, `{"command":"cat notes.txt"}`); rec.Code != http.StatusOK {
		t.Fatalf("exec: %d %s", rec.Code, rec.Body.String())
	}
	if usage := waitUsageLog(t, resolver); usage.Stdout != "exec output\n" {
		t.Fatalf("exec usage log = %+v, want its output", usage)
	}
	waitRun(t, resolver)
	select {
	case row := <-resolver.sandboxLogCh:
		if row.Stream != "stdout" || row.Content != "exec output\n" {
			t.Fatalf("exec sandbox log = %+v", row)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exec output did not reach sandbox_logs")
	}
}

// A server whose sandbox service cannot run quiet commands refuses quiet
// rather than running a command whose output would be logged.
func TestSessionExecQuietNeedsSupport(t *testing.T) {
	svc := newFakeSessionService()
	// Only the plane.Service methods: ExecQuiet is hidden.
	server, _, sessionID := quietTestServer(t, struct{ plane.Service }{svc})

	rec := postSessionExec(server, sessionID, `{"command":"cat notes.txt","quiet":true}`)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501: %s", rec.Code, rec.Body.String())
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.quietCommands) != 0 || len(svc.execTimeouts) != 0 {
		t.Fatalf("a refused quiet exec ran: quiet=%q plain=%d", svc.quietCommands, len(svc.execTimeouts))
	}
}
