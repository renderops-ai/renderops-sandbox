package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"backend/internal/handler"
	"backend/internal/ide"
	"backend/internal/ideauth"
	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
)

type fakeIDEWorker struct {
	instance plane.IDEInstance
	err      error
	calls    int
}

func (f *fakeIDEWorker) StartIDE(context.Context, string, string) (plane.IDEInstance, error) {
	f.calls++
	return f.instance, f.err
}

func TestCreateIDESessionStartsWorkerAndReturnsHandoffURL(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions[previewSandboxID] = &plane.SessionInfo{
		ID: previewSandboxID, UserID: "tenant-1", State: plane.StateActive,
	}
	placements := &fakePlacementResolver{placement: orchestrator.Placement{
		SandboxID: previewSandboxID, WorkerID: "worker-1",
		Endpoint: "http://10.0.0.4:9876", State: "active",
	}}
	workers := &fakeIDEWorker{instance: plane.IDEInstance{Port: ide.DefaultPort, State: "ready"}}
	signer, _ := ideauth.NewSigner(ideauth.DeriveSigningSecret("worker-secret"))
	server := newIDETestServer(sessions, placements, workers, signer)

	request := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/"+previewSandboxID+"/ide/sessions", nil)
	request.Header.Set("Authorization", "Bearer ro_live_test-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var created handler.CreateIDESessionResponse
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(created.URL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Host != "3001-"+previewSandboxID+".dev-sandbox.renderops.com" || workers.calls != 1 {
		t.Fatalf("url=%q worker_calls=%d", created.URL, workers.calls)
	}
	if payload := parsed.Query().Get("payload"); payload != `[["skipWelcome","true"]]` {
		t.Fatalf("payload=%q, want the skipWelcome workbench payload", payload)
	}
	claimsToken := parsed.Query().Get("ro_auth")
	nonces := ideauth.NewMemoryNonceStore()
	if _, _, err := signer.Redeem(context.Background(), claimsToken, previewSandboxID, ide.DefaultPort, nonces); err != nil {
		t.Fatalf("redeem handoff: %v", err)
	}
	if created.ExpiresAt.Before(time.Now().Add(50 * time.Second)) {
		t.Fatalf("handoff expiry=%s", created.ExpiresAt)
	}
}

func TestCreateIDESessionHidesForeignSandbox(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions[previewSandboxID] = &plane.SessionInfo{
		ID: previewSandboxID, UserID: "tenant-2", State: plane.StateActive,
	}
	workers := &fakeIDEWorker{instance: plane.IDEInstance{Port: ide.DefaultPort, State: "ready"}}
	signer, _ := ideauth.NewSigner(ideauth.DeriveSigningSecret("worker-secret"))
	server := newIDETestServer(sessions, &fakePlacementResolver{}, workers, signer)

	request := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/"+previewSandboxID+"/ide/sessions", nil)
	request.Header.Set("Authorization", "Bearer ro_live_test-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || workers.calls != 0 {
		t.Fatalf("status=%d worker_calls=%d", response.Code, workers.calls)
	}
}

func newIDETestServer(sessions plane.Service, placements handler.IDEPlacementResolver, workers handler.IDEWorker, signer *ideauth.Signer) http.Handler {
	resolver := &fakePlatformService{record: platform.KeyRecord{
		ID: "key-1", UserID: "tenant-1", IsActive: true, BalanceUSD: 10,
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sandboxes/{sandboxID}/ide/sessions", handler.CreateIDESessionHandler(
		sessions, placements, workers, signer, "dev-sandbox.renderops.com",
	))
	return middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux)
}
