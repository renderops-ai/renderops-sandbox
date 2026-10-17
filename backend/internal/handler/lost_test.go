package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"backend/internal/plane"
)

// lostService is a plane.Service whose only sandbox is lost.
type lostService struct {
	plane.Service
	userID, reason string
}

func (s lostService) GetSession(context.Context, string) (*plane.SessionInfo, bool) {
	return nil, false // a lost row is not a live session
}

func (s lostService) LostSandbox(_ context.Context, _ string) (string, string, bool) {
	return s.userID, s.reason, true
}

func decodeLost(t *testing.T, recorder *httptest.ResponseRecorder) SandboxLostResponse {
	t.Helper()
	var body SandboxLostResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body.String(), err)
	}
	return body
}

func TestCommandOnLostSandboxReturns410(t *testing.T) {
	mgr := lostService{userID: "user-1", reason: "VM crashed (out of memory)"}
	request := httptest.NewRequest(http.MethodPost, "/session/sandbox-1/pause", nil)
	recorder := httptest.NewRecorder()

	if ownsLiveSession(recorder, request, mgr, "sandbox-1", "user-1", "req-1") {
		t.Fatal("lost sandbox accepted as live")
	}
	if recorder.Code != http.StatusGone {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := decodeLost(t, recorder)
	if body.Code != "sandbox_lost" || body.Reason != "VM crashed (out of memory)" ||
		!strings.Contains(body.Hint, "DELETE /session/sandbox-1") || body.RequestID != "req-1" {
		t.Fatalf("body = %+v", body)
	}
}

func TestLostSandboxOfAnotherTenantIsNotFound(t *testing.T) {
	mgr := lostService{userID: "user-2", reason: "VM crashed"}
	request := httptest.NewRequest(http.MethodPost, "/session/sandbox-1/exec", nil)
	recorder := httptest.NewRecorder()
	if ownsLiveSession(recorder, request, mgr, "sandbox-1", "user-1", "req-1") {
		t.Fatal("foreign sandbox accepted")
	}
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (no id leak)", recorder.Code)
	}
}

func TestLostErrorFromWorkerMapsTo410(t *testing.T) {
	recorder := httptest.NewRecorder()
	err := fmt.Errorf("pause sandbox: %w", &plane.LostError{Reason: "VM crashed (Firecracker signal: killed)"})
	if !writeIfLost(recorder, "req-1", "sandbox-1", err) {
		t.Fatal("lost error not mapped")
	}
	if recorder.Code != http.StatusGone || decodeLost(t, recorder).Reason != "VM crashed (Firecracker signal: killed)" {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if writeIfLost(httptest.NewRecorder(), "req-1", "sandbox-1", errors.New("session x not found")) {
		t.Fatal("plain error mapped as lost")
	}
}
