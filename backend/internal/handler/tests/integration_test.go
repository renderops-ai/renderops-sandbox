package handler_test

import (
	"backend/internal/billing"
	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSessionRequiresAuth(t *testing.T) {
	server := newTestServer(t, testDeps{})

	req := httptest.NewRequest(http.MethodPost, "/session", nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestSessionCreateAcceptsBetterAuthSession(t *testing.T) {
	server := newTestServer(t, testDeps{
		resolver: &fakePlatformService{
			record: platform.KeyRecord{
				ID:         "profile-source",
				BalanceUSD: 10,
			},
			sessionUserID: "better-auth-user",
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/session", nil)
	req.Header.Set("Authorization", "Bearer better-auth-session-token")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp handler.CreateSessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Tenant == nil || resp.Tenant.TenantID != "better-auth-user" {
		t.Fatalf("unexpected tenant payload: %+v", resp.Tenant)
	}
}

func TestSessionCreateReturnsServiceUnavailableWhenCapacityIsExhausted(t *testing.T) {
	server := newTestServer(t, testDeps{
		resolver: &fakePlatformService{
			record: platform.KeyRecord{
				ID:         "key-1",
				UserID:     "tenant-1",
				IsActive:   true,
				BalanceUSD: 10,
			},
		},
		sessionSvc: &fakeSessionService{
			sessions:  map[string]*plane.SessionInfo{},
			createErr: orchestrator.ErrNoCapacity,
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/session", nil)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	var response handler.APIError
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Code != "no_capacity" {
		t.Fatalf("code=%q want no_capacity", response.Code)
	}
}

func TestSessionCreateUsageLogDoesNotUseRequestCancellationContext(t *testing.T) {
	resolver := &fakePlatformService{
		record: platform.KeyRecord{
			ID:         "key-1",
			UserID:     "tenant-1",
			IsActive:   true,
			BalanceUSD: 10,
		},
		logCh:    make(chan error, 1),
		logDelay: 20 * time.Millisecond,
	}
	server := newTestServer(t, testDeps{resolver: resolver})

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/session", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)
	cancel()

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	select {
	case err := <-resolver.logCh:
		if err != nil {
			t.Fatalf("usage log used cancelled request context: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for usage log")
	}
}

func TestSessionLifecycle(t *testing.T) {
	svc := newFakeSessionService()
	server := newTestServer(t, testDeps{
		resolver: &fakePlatformService{
			record: platform.KeyRecord{
				ID:         "key-1",
				UserID:     "tenant-1",
				IsActive:   true,
				BalanceUSD: 10,
			},
		},
		sessionSvc: svc,
	})

	createReq := httptest.NewRequest(http.MethodPost, "/session", nil)
	createReq.Header.Set("Authorization", "Bearer ro_live_pro-key")
	createRec := httptest.NewRecorder()
	server.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRec.Code, createRec.Body.String())
	}

	var createResp handler.CreateSessionResponse
	if err := json.NewDecoder(createRec.Body).Decode(&createResp); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if createResp.Session == nil || createResp.Session.SessionID == "" {
		t.Fatalf("unexpected create response: %+v", createResp)
	}
	sessionID := createResp.Session.SessionID

	runReq := httptest.NewRequest(http.MethodPost, "/session/"+sessionID+"/run", bytes.NewBufferString(`{"code":"print(x)","language":"python"}`))
	runReq.Header.Set("Content-Type", "application/json")
	runReq.Header.Set("Authorization", "Bearer ro_live_pro-key")
	runRec := httptest.NewRecorder()
	server.ServeHTTP(runRec, runReq)

	if runRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from run, got %d: %s", runRec.Code, runRec.Body.String())
	}

	var runResp handler.SessionExecuteResponse
	if err := json.NewDecoder(runRec.Body).Decode(&runResp); err != nil {
		t.Fatalf("decode run response: %v", err)
	}
	if runResp.Result == nil || runResp.Result.Stdout != "session output\n" {
		t.Fatalf("unexpected session run response: %+v", runResp.Result)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/session/"+sessionID, nil)
	getReq.Header.Set("Authorization", "Bearer ro_live_pro-key")
	getRec := httptest.NewRecorder()
	server.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from get, got %d: %s", getRec.Code, getRec.Body.String())
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/session/"+sessionID, nil)
	delReq.Header.Set("Authorization", "Bearer ro_live_pro-key")
	delRec := httptest.NewRecorder()
	server.ServeHTTP(delRec, delReq)

	if delRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 from delete, got %d: %s", delRec.Code, delRec.Body.String())
	}
}

// TestSessionExecResolvesTimeout checks that /exec applies the policy default
// and max (60s / 5m in testExecutionPolicy) like /run, so the worker call's
// timeout is bounded by the advertised limit.
func TestSessionExecResolvesTimeout(t *testing.T) {
	svc := newFakeSessionService()
	server := newTestServer(t, testDeps{sessionSvc: svc})

	createReq := httptest.NewRequest(http.MethodPost, "/session", nil)
	createReq.Header.Set("Authorization", "Bearer ro_live_pro-key")
	createRec := httptest.NewRecorder()
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var createResp handler.CreateSessionResponse
	if err := json.NewDecoder(createRec.Body).Decode(&createResp); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	sessionID := createResp.Session.SessionID

	bodies := []string{
		`{"command":"true"}`,                  // unset: policy default
		`{"command":"sleep 90","timeout":90}`, // within the max: as requested
		`{"command":"true","timeout":3600}`,   // above the max: capped
	}
	for _, body := range bodies {
		req := httptest.NewRequest(http.MethodPost, "/session/"+sessionID+"/exec", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer ro_live_pro-key")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("exec %s: expected 200, got %d: %s", body, rec.Code, rec.Body.String())
		}
	}

	svc.mu.Lock()
	got := append([]int(nil), svc.execTimeouts...)
	svc.mu.Unlock()
	want := []int{60, 90, 300}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("exec timeouts = %v, want %v", got, want)
	}
}

// TestSKPlatformContract replays, verbatim, every call sk-renderops-platform
// makes to the Go backend (src/lib/server/actions/sandboxes.js): create,
// pause, resume, destroy — authenticated the way sk authenticates (a
// better-auth session token as Bearer), with the bodies sk sends and the
// response fields sk parses. If this test fails, the dashboard is broken.
func TestSKPlatformContract(t *testing.T) {
	resolver := &fakePlatformService{
		record:        platform.KeyRecord{ID: "profile-source", BalanceUSD: 10},
		sessionUserID: "dash-user",
		auditCh:       make(chan platform.AuditEvent, 4),
	}
	server := newTestServer(t, testDeps{
		resolver: resolver,
	})

	do := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		var rdr *bytes.Buffer
		if body != "" {
			rdr = bytes.NewBufferString(body)
		} else {
			rdr = &bytes.Buffer{}
		}
		req := httptest.NewRequest(method, path, rdr)
		// backendCall sets both headers on every request, body or not.
		req.Header.Set("Authorization", "Bearer better-auth-session-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		return rec
	}

	// createSandbox: POST /session with the dashboard form's default body.
	rec := do(http.MethodPost, "/session",
		`{"name":"my-box","size":"small","network":{"internet":true},"idle_timeout_s":300,"max_lifetime_s":3600}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	// sk reads data.session.session_id and data.session.state.
	var createResp struct {
		Session *struct {
			SessionID string `json:"session_id"`
			State     string `json:"state"`
		} `json:"session"`
		Limits *struct {
			IdleTimeoutMs int `json:"idle_timeout_ms"`
		} `json:"limits"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&createResp); err != nil {
		t.Fatalf("create: decode: %v", err)
	}
	if createResp.Session == nil || createResp.Session.SessionID == "" {
		t.Fatalf("create: sk needs session.session_id, got %s", rec.Body.String())
	}
	if createResp.Session.State != "active" {
		t.Fatalf("create: sk expects state active, got %q", createResp.Session.State)
	}
	if createResp.Limits == nil || createResp.Limits.IdleTimeoutMs != 300_000 {
		t.Fatalf("create must report requested idle timeout, got %+v", createResp.Limits)
	}
	id := createResp.Session.SessionID

	// pauseSandbox: POST /session/{id}/pause — sk only checks res.ok.
	if rec := do(http.MethodPost, "/session/"+id+"/pause", ""); rec.Code != http.StatusOK {
		t.Fatalf("pause: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// resumeSandbox: POST /session/{id}/resume — sk only checks res.ok.
	if rec := do(http.MethodPost, "/session/"+id+"/resume", ""); rec.Code != http.StatusOK {
		t.Fatalf("resume: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// destroySandbox: DELETE /session/{id} — sk only checks res.ok (204, empty body).
	if rec := do(http.MethodDelete, "/session/"+id, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("destroy: expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	wantActions := map[string]bool{
		"sandbox.created":   false,
		"sandbox.paused":    false,
		"sandbox.resumed":   false,
		"sandbox.destroyed": false,
	}
	for range wantActions {
		select {
		case event := <-resolver.auditCh:
			if _, ok := wantActions[event.Action]; !ok {
				t.Fatalf("unexpected audit action %q", event.Action)
			}
			wantActions[event.Action] = true
			if event.ScopeType != "personal" || event.ScopeID != "dash-user" {
				t.Fatalf("unexpected audit scope: %+v", event)
			}
			if event.ResourceType != "sandbox" || event.ResourceID != id {
				t.Fatalf("unexpected audit resource: %+v", event)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for lifecycle audit events")
		}
	}
	for action, seen := range wantActions {
		if !seen {
			t.Errorf("missing audit action %q", action)
		}
	}
}

// TestSessionCreateAcceptsDashboardBody locks in the exact create body the sk
// dashboard sends (named size + network + timeouts) — it must never 400.
func TestSessionCreateAcceptsDashboardBody(t *testing.T) {
	server := newTestServer(t, testDeps{})
	body := `{"name":"my-box","size":"small","network":{"internet":true},"idle_timeout_s":300,"max_lifetime_s":3600}`
	req := httptest.NewRequest(http.MethodPost, "/session", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 for dashboard create body, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp handler.CreateSessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Session == nil || resp.Session.SessionID == "" {
		t.Fatalf("unexpected create response: %+v", resp)
	}
}

func TestSessionCreateRejectsUnknownSize(t *testing.T) {
	server := newTestServer(t, testDeps{})
	req := httptest.NewRequest(http.MethodPost, "/session", bytes.NewBufferString(`{"size":"mega"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown size, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSessionCreateRejectsClientSelectedTier(t *testing.T) {
	server := newTestServer(t, testDeps{})
	req := httptest.NewRequest(http.MethodPost, "/session", bytes.NewBufferString(`{"tier":"pro"}`))
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for client-selected tier, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSessionLifecycleRejectsForeignTenant(t *testing.T) {
	svc := newFakeSessionService()
	svc.sessions["foreign-session"] = &plane.SessionInfo{
		ID:           "foreign-session",
		UserID:       "tenant-2",
		BillingModel: billing.PAYG,
	}
	server := newTestServer(t, testDeps{
		resolver: &fakePlatformService{
			record: platform.KeyRecord{
				ID:         "key-1",
				UserID:     "tenant-1",
				IsActive:   true,
				BalanceUSD: 10,
			},
		},
		sessionSvc: svc,
	})

	req := httptest.NewRequest(http.MethodDelete, "/session/foreign-session", nil)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	if _, ok := svc.GetSession(context.Background(), "foreign-session"); !ok {
		t.Fatal("foreign tenant session was deleted")
	}
}

type testDeps struct {
	resolver   *fakePlatformService
	sessionSvc *fakeSessionService
}

func newTestServer(t *testing.T, deps testDeps) http.Handler {
	t.Helper()

	resolver := deps.resolver
	if resolver == nil || resolver.record.ID == "" {
		resolver = &fakePlatformService{
			record: platform.KeyRecord{
				ID:         "key-1",
				UserID:     "tenant-1",
				IsActive:   true,
				BalanceUSD: 10,
			},
		}
	}

	sessionSvc := deps.sessionSvc
	if sessionSvc == nil {
		sessionSvc = newFakeSessionService()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/session", handler.SessionHandler(sessionSvc, resolver))
	mux.HandleFunc("/session/", handler.SessionHandler(sessionSvc, resolver))

	return middleware.Logging(middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux))
}

type fakePlatformService struct {
	record        platform.KeyRecord
	sessionUserID string
	mu            sync.Mutex
	logs          []platform.UsageLog
	logCh         chan error
	logDelay      time.Duration
	auditCh       chan platform.AuditEvent
}

func (f *fakePlatformService) ResolveKey(keyHash string) (platform.KeyRecord, error) {
	if f.record.ID == "" {
		return platform.KeyRecord{}, fmt.Errorf("not found")
	}
	return f.record, nil
}

func (f *fakePlatformService) ResolveSession(token string) (platform.SessionRecord, error) {
	if f.sessionUserID == "" {
		return platform.SessionRecord{}, platform.ErrSessionNotFound
	}
	return platform.SessionRecord{UserID: f.sessionUserID}, nil
}

func (f *fakePlatformService) InsertUsageLog(ctx context.Context, log platform.UsageLog) {
	if f.logDelay > 0 {
		time.Sleep(f.logDelay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, log)
	if f.logCh != nil {
		f.logCh <- ctx.Err()
	}
}

func (f *fakePlatformService) InsertAuditEvent(_ context.Context, event platform.AuditEvent) error {
	if f.auditCh != nil {
		f.auditCh <- event
	}
	return nil
}

func (f *fakePlatformService) UpsertSandbox(ctx context.Context, sb platform.Sandbox) {}
func (f *fakePlatformService) UpdateSandboxDetails(ctx context.Context, sb platform.Sandbox) error {
	return nil
}
func (f *fakePlatformService) UpdateSandboxState(ctx context.Context, id, state string)      {}
func (f *fakePlatformService) InsertSandboxLog(ctx context.Context, l platform.SandboxLog)   {}
func (f *fakePlatformService) InsertSandboxRun(ctx context.Context, run platform.SandboxRun) {}
func (f *fakePlatformService) BillSandboxRuntime(ctx context.Context, sandboxID string, ratePerSec float64) {
}
func (f *fakePlatformService) ListSandboxes(ctx context.Context, userID string) ([]platform.SandboxListItem, error) {
	return nil, nil
}

func (f *fakePlatformService) GetProfile(userID string) (platform.Profile, error) {
	return platform.Profile{BalanceUSD: f.record.BalanceUSD}, nil
}

type fakeSessionService struct {
	mu           sync.Mutex
	sessions     map[string]*plane.SessionInfo
	createErr    error
	execTimeouts []int // timeoutSec of each Exec call
}

func newFakeSessionService() *fakeSessionService {
	return &fakeSessionService{sessions: map[string]*plane.SessionInfo{}}
}

func (f *fakeSessionService) Exec(ctx context.Context, sessionID, command string, timeoutSec int) (plane.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sessions[sessionID]; !ok {
		return plane.ExecResult{}, fmt.Errorf("session %s not found", sessionID)
	}
	f.execTimeouts = append(f.execTimeouts, timeoutSec)
	return plane.ExecResult{
		Stdout:            "exec output\n",
		ExitCode:          0,
		TerminationReason: "success",
	}, nil
}

func (f *fakeSessionService) OpenTerminal(context.Context, string, string, string, uint16, uint16) error {
	return nil
}

func (f *fakeSessionService) CloseTerminal(context.Context, string, string) error { return nil }

func (f *fakeSessionService) Create(ctx context.Context, userID, billingModel, image string, env map[string]string, vcpus, memoryMB, diskGB int, internet bool, idleTimeout, maxLifetime time.Duration) (*plane.SessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}

	id := fmt.Sprintf("sess-%d", len(f.sessions)+1)
	sess := &plane.SessionInfo{
		ID:           id,
		UserID:       userID,
		BillingModel: billingModel,
		Image:        image,
		IdleTimeout:  idleTimeout,
		MaxLifetime:  maxLifetime,
		CreatedAt:    time.Now().UTC(),
		LastUsed:     time.Now().UTC(),
		State:        plane.StateActive,
	}
	f.sessions[id] = sess
	return sess, nil
}

func (f *fakeSessionService) Execute(ctx context.Context, sessionID, code, language string, timeoutSec int) (plane.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sess, ok := f.sessions[sessionID]
	if !ok {
		return plane.ExecResult{}, fmt.Errorf("session %s not found", sessionID)
	}
	sess.LastUsed = time.Now().UTC()
	sess.RunCount++
	sess.TotalExecutionMs += 7
	exitCode := 0
	sess.LastExitCode = &exitCode
	return plane.ExecResult{
		Stdout:            "session output\n",
		ExitCode:          0,
		TerminationReason: "success",
		GuestDuration:     0.007,
	}, nil
}

func (f *fakeSessionService) Pause(ctx context.Context, sessionID string) error  { return nil }
func (f *fakeSessionService) Resume(ctx context.Context, sessionID string) error { return nil }

func (f *fakeSessionService) Destroy(ctx context.Context, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sessions[sessionID]; !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	delete(f.sessions, sessionID)
	return nil
}

func (f *fakeSessionService) GetSession(ctx context.Context, id string) (*plane.SessionInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sess, ok := f.sessions[id]
	return sess, ok
}
