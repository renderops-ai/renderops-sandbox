package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"backend/internal/executor"
	"backend/internal/plane"
	"backend/internal/session"
)

// optionsSessionService records which exec path the worker took.
type optionsSessionService struct {
	fakeSessionService
	plainExecs int
	options    []session.ExecOptions
	optionsErr error
}

func (f *optionsSessionService) Exec(context.Context, string, string, int) (executor.ExecutionResult, error) {
	f.plainExecs++
	return executor.ExecutionResult{Stdout: "plain\n"}, nil
}

func (f *optionsSessionService) ExecWithOptions(_ context.Context, _, _ string, _ int, opts session.ExecOptions) (executor.ExecutionResult, error) {
	f.options = append(f.options, opts)
	return executor.ExecutionResult{Stdout: "options\n"}, f.optionsErr
}

func postExec(t *testing.T, server *Server, req plane.ExecRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, plane.RouteSandboxPrefix+"sandbox-1/exec", bytes.NewReader(body))
	request.Header.Set(plane.AuthHeader, "worker-secret")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	return response
}

func TestExecWithLifecycleOptions(t *testing.T) {
	service := &optionsSessionService{}
	server := NewServer(service, "worker-secret", 10)

	if response := postExec(t, server, plane.ExecRequest{Command: "true"}); response.Code != http.StatusOK || service.plainExecs != 1 || len(service.options) != 0 {
		t.Fatalf("plain exec: %d, plain=%d options=%v", response.Code, service.plainExecs, service.options)
	}
	response := postExec(t, server, plane.ExecRequest{Command: "true", ActiveOnly: true, Passive: true})
	if response.Code != http.StatusOK || len(service.options) != 1 || service.options[0] != (session.ExecOptions{ActiveOnly: true, Passive: true}) {
		t.Fatalf("exec with options: %d %v", response.Code, service.options)
	}

	service.optionsErr = fmt.Errorf("%w: session sandbox-1 is paused", session.ErrNotActive)
	response = postExec(t, server, plane.ExecRequest{Command: "true", ActiveOnly: true})
	var body plane.ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusConflict || body.Code != "sandbox_not_active" {
		t.Fatalf("paused sandbox: %d %+v", response.Code, body)
	}
}

func TestExecWithOptionsNeedsSupport(t *testing.T) {
	server := NewServer(&fakeSessionService{}, "worker-secret", 10)
	if response := postExec(t, server, plane.ExecRequest{Command: "true", ActiveOnly: true}); response.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501 rather than an exec that may resume", response.Code)
	}
}
