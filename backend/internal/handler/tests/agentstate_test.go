package handler_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/plane"
	"backend/internal/platform"
)

func newAgentStateTestServer(t *testing.T, state plane.SessionState, owner string, execer handler.FileExecer) http.Handler {
	t.Helper()
	sessions := newFakeSessionService()
	sessions.sessions[filesSandboxID] = &plane.SessionInfo{ID: filesSandboxID, UserID: owner, State: state}
	resolver := &fakePlatformService{record: platform.KeyRecord{
		ID: "key-1", UserID: "tenant-1", IsActive: true, BalanceUSD: 10,
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{sandboxID}/agent-state", handler.AgentStateHandler(sessions, execer))
	return middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux)
}

func getAgentState(t *testing.T, server http.Handler) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/v1/sandboxes/"+filesSandboxID+"/agent-state", nil)
	request.Header.Set("Authorization", "Bearer ro_live_test-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	var decoded map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("body is not JSON: %q", response.Body.String())
	}
	return response, decoded
}

func TestAgentStateReturnsTheHooksState(t *testing.T) {
	// The sandbox's clock runs behind (it was resumed): age_seconds uses it.
	guestNow := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	updatedAt := guestNow.Add(-90 * time.Second)
	execer := &fakeFileExecer{answers: []string{
		fmt.Sprintf("%d\n{\"state\":\"working\",\"updated_at\":%q}\n", guestNow.Unix(), updatedAt.Format(time.RFC3339)),
	}}
	server := newAgentStateTestServer(t, plane.StateActive, "tenant-1", execer)

	response, body := getAgentState(t, server)
	if response.Code != http.StatusOK || body["state"] != "working" || body["age_seconds"] != float64(90) {
		t.Fatalf("%d %v", response.Code, body)
	}
	if got, err := time.Parse(time.RFC3339, fmt.Sprint(body["updated_at"])); err != nil || !got.Equal(updatedAt) {
		t.Fatalf("updated_at = %v (%v), want %v", body["updated_at"], err, updatedAt)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", response.Header().Get("Cache-Control"))
	}
	// Polling must not keep the sandbox awake (passive) or fill its logs
	// (quiet: fakeFileExecer refuses anything else).
	if len(execer.calls) != 1 || !execer.calls[0].passive {
		t.Fatalf("calls = %+v, want one passive exec", execer.calls)
	}
}

func TestAgentStateUnknownWithoutAValidFile(t *testing.T) {
	for name, file := range map[string]string{
		"missing":       "",
		"malformed":     `{"state":`,
		"unknown state": `{"state":"thinking","updated_at":"2026-10-17T12:00:00Z"}`,
		"bad timestamp": `{"state":"done","updated_at":"noon"}`,
	} {
		execer := &fakeFileExecer{answers: []string{"1760702400\n" + file}}
		response, body := getAgentState(t, newAgentStateTestServer(t, plane.StateActive, "tenant-1", execer))
		if response.Code != http.StatusOK || len(body) != 1 || body["state"] != "unknown" {
			t.Errorf("%s: %d %v, want 200 {\"state\":\"unknown\"}", name, response.Code, body)
		}
	}
}

func TestAgentStateOwnerAndActiveOnly(t *testing.T) {
	execer := &fakeFileExecer{}
	response, body := getAgentState(t, newAgentStateTestServer(t, plane.StateActive, "tenant-2", execer))
	if response.Code != http.StatusNotFound || body["code"] != "sandbox_not_found" {
		t.Fatalf("foreign sandbox: %d %v", response.Code, body)
	}
	response, body = getAgentState(t, newAgentStateTestServer(t, plane.StatePaused, "tenant-1", execer))
	if response.Code != http.StatusConflict || body["code"] != "sandbox_not_active" {
		t.Fatalf("paused sandbox: %d %v", response.Code, body)
	}
	if len(execer.calls) != 0 {
		t.Fatal("a foreign or paused sandbox reached the worker")
	}

	// The sandbox pauses after the check: the worker refuses (ActiveOnly).
	racing := &fakeFileExecer{err: fmt.Errorf("exec on worker w1: %w: session is paused", plane.ErrSandboxNotActive)}
	response, body = getAgentState(t, newAgentStateTestServer(t, plane.StateActive, "tenant-1", racing))
	if response.Code != http.StatusConflict || body["code"] != "sandbox_not_active" {
		t.Fatalf("paused during the request: %d %v", response.Code, body)
	}

	failing := &fakeFileExecer{err: errors.New("worker unreachable")}
	response, body = getAgentState(t, newAgentStateTestServer(t, plane.StateActive, "tenant-1", failing))
	if response.Code != http.StatusBadGateway || body["code"] != "agent_state_failed" {
		t.Fatalf("worker failure: %d %v", response.Code, body)
	}
}
