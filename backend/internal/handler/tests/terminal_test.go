package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/terminal"

	"github.com/coder/websocket"
)

const terminalTestSecret = "0123456789abcdef0123456789abcdef"

type fakePlacementResolver struct {
	placement orchestrator.Placement
	err       error
	calls     int
}

type fakeTerminalWorker struct {
	openCalls  int
	closeCalls int
	openErr    error
	starts     []terminal.Command
}

type fakeTerminalAttachmentWorker struct {
	stream     *fakeTerminalStream
	closeCalls int
	mu         sync.Mutex
}

type fakeTerminalStream struct {
	toWorker   chan terminal.Frame
	fromWorker chan terminal.Frame
}

func newFakeTerminalStream() *fakeTerminalStream {
	return &fakeTerminalStream{toWorker: make(chan terminal.Frame, 8), fromWorker: make(chan terminal.Frame, 8)}
}

func (s *fakeTerminalStream) Send(frame terminal.Frame) error {
	s.toWorker <- frame
	return nil
}

func (s *fakeTerminalStream) Receive() (terminal.Frame, error) {
	return <-s.fromWorker, nil
}

func (f *fakeTerminalAttachmentWorker) AttachTerminal(context.Context, string, string, string) (terminal.Stream, error) {
	return f.stream, nil
}

func (f *fakeTerminalAttachmentWorker) CloseTerminal(context.Context, string, string, string) error {
	f.mu.Lock()
	f.closeCalls++
	f.mu.Unlock()
	return nil
}

func (f *fakeTerminalWorker) OpenTerminal(_ context.Context, _, _, _, _ string, _, _ uint32, start terminal.Command) error {
	f.openCalls++
	f.starts = append(f.starts, start)
	return f.openErr
}

func (f *fakeTerminalWorker) CloseTerminal(context.Context, string, string, string) error {
	f.closeCalls++
	return nil
}

func (f *fakePlacementResolver) Placement(context.Context, string) (orchestrator.Placement, error) {
	f.calls++
	return f.placement, f.err
}

func TestCreateTerminalAuthorizesMultipleFreshTerminals(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions["sandbox-1"] = &plane.SessionInfo{
		ID:     "sandbox-1",
		UserID: "tenant-1",
		State:  plane.StateActive,
	}
	placements := &fakePlacementResolver{placement: orchestrator.Placement{
		SandboxID: "sandbox-1",
		WorkerID:  "worker-1",
		Endpoint:  "http://10.0.0.4:9876",
	}}
	manager, err := terminal.NewManager(terminalTestSecret, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	workers := &fakeTerminalWorker{}
	server := newTerminalTestServer(t, sessions, placements, workers, manager)

	first := createTerminal(t, server, "sandbox-1", "ro_live_test-key")
	second := createTerminal(t, server, "sandbox-1", "ro_live_test-key")
	if first.Terminal.ID == second.Terminal.ID {
		t.Fatal("multiple terminals for one sandbox received the same terminal id")
	}
	if first.Terminal.State != terminal.StateReady {
		t.Fatalf("state=%q want ready", first.Terminal.State)
	}
	if first.WebSocketPath != "/v1/terminals/"+first.Terminal.ID {
		t.Fatalf("unexpected WebSocket path %q", first.WebSocketPath)
	}
	if first.AttachmentToken == "" || first.ExpiresIn <= 0 || first.ExpiresIn > 60 {
		t.Fatalf("unexpected attachment token metadata: %+v", first)
	}

	consumed, err := manager.Consume(first.AttachmentToken)
	if err != nil {
		t.Fatalf("consume issued attachment token: %v", err)
	}
	if consumed.ID != first.Terminal.ID || consumed.WorkerID != "worker-1" {
		t.Fatalf("unexpected terminal target: %+v", consumed)
	}
	if workers.openCalls != 2 {
		t.Fatalf("worker open calls=%d want 2", workers.openCalls)
	}
}

func TestCreateTerminalRejectsForeignSandboxWithoutResolvingPlacement(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions["sandbox-foreign"] = &plane.SessionInfo{
		ID:     "sandbox-foreign",
		UserID: "tenant-2",
		State:  plane.StateActive,
	}
	placements := &fakePlacementResolver{}
	manager, _ := terminal.NewManager(terminalTestSecret, time.Minute)
	server := newTerminalTestServer(t, sessions, placements, &fakeTerminalWorker{}, manager)

	req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/sandbox-foreign/terminals", nil)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404: %s", rec.Code, rec.Body.String())
	}
	if placements.calls != 0 {
		t.Fatalf("placement was resolved %d times for a foreign sandbox", placements.calls)
	}
}

func TestCreateTerminalRequiresActiveSandbox(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions["sandbox-paused"] = &plane.SessionInfo{
		ID:     "sandbox-paused",
		UserID: "tenant-1",
		State:  plane.StatePaused,
	}
	placements := &fakePlacementResolver{}
	manager, _ := terminal.NewManager(terminalTestSecret, time.Minute)
	server := newTerminalTestServer(t, sessions, placements, &fakeTerminalWorker{}, manager)

	req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/sandbox-paused/terminals", nil)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want 409: %s", rec.Code, rec.Body.String())
	}
	var response handler.APIError
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Code != "sandbox_not_active" {
		t.Fatalf("code=%q want sandbox_not_active", response.Code)
	}
}

func TestCreateTerminalHandlesUnavailablePlacement(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions["sandbox-1"] = &plane.SessionInfo{
		ID:     "sandbox-1",
		UserID: "tenant-1",
		State:  plane.StateActive,
	}
	placements := &fakePlacementResolver{err: errors.New("orchestrator unavailable")}
	manager, _ := terminal.NewManager(terminalTestSecret, time.Minute)
	server := newTerminalTestServer(t, sessions, placements, &fakeTerminalWorker{}, manager)

	req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/sandbox-1/terminals", nil)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateTerminalDoesNotIssueTokenWhenWorkerFails(t *testing.T) {
	sessions := newFakeSessionService()
	sessions.sessions["sandbox-1"] = &plane.SessionInfo{ID: "sandbox-1", UserID: "tenant-1", State: plane.StateActive}
	placements := &fakePlacementResolver{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://10.0.0.4:9876",
	}}
	workers := &fakeTerminalWorker{openErr: errors.New("guest PTY unavailable")}
	manager, _ := terminal.NewManager(terminalTestSecret, time.Minute)
	server := newTerminalTestServer(t, sessions, placements, workers, manager)

	req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/sandbox-1/terminals", nil)
	req.Header.Set("Authorization", "Bearer ro_live_test-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "attachment_token") {
		t.Fatalf("worker failure returned a terminal token: %s", rec.Body.String())
	}
}

func TestAttachTerminalWebSocketStreamsInputOutputAndResize(t *testing.T) {
	manager, _ := terminal.NewManager(terminalTestSecret, time.Minute)
	session, _ := manager.Reserve("sandbox-1", "tenant-1", "worker-1")
	_, token, _ := manager.Authorize(session.ID)
	placements := &fakePlacementResolver{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://10.0.0.4:9876",
	}}
	stream := newFakeTerminalStream()
	workers := &fakeTerminalAttachmentWorker{stream: stream}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/terminals/{terminalID}", handler.AttachTerminalHandler(placements, workers, manager, nil))
	server := httptest.NewServer(middleware.Logging(mux))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/terminals/" + session.ID + "?token=" + token
	connection, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial terminal WebSocket: %v", err)
	}
	defer connection.CloseNow()

	messageType, payload, err := connection.Read(ctx)
	if err != nil || messageType != websocket.MessageText || !strings.Contains(string(payload), `"type":"ready"`) {
		t.Fatalf("ready message type=%v payload=%s err=%v", messageType, payload, err)
	}

	stream.fromWorker <- terminal.Frame{Type: terminal.FrameOutput, Data: []byte("prompt$ ")}
	messageType, payload, err = connection.Read(ctx)
	if err != nil || messageType != websocket.MessageBinary || string(payload) != "prompt$ " {
		t.Fatalf("output message type=%v payload=%q err=%v", messageType, payload, err)
	}

	if err := connection.Write(ctx, websocket.MessageBinary, []byte("pwd\n")); err != nil {
		t.Fatal(err)
	}
	if frame := <-stream.toWorker; frame.Type != terminal.FrameInput || string(frame.Data) != "pwd\n" {
		t.Fatalf("unexpected input frame: %+v", frame)
	}
	if err := connection.Write(ctx, websocket.MessageText, []byte(`{"type":"resize","columns":140,"rows":40}`)); err != nil {
		t.Fatal(err)
	}
	if frame := <-stream.toWorker; frame.Type != terminal.FrameResize || frame.Columns != 140 || frame.Rows != 40 {
		t.Fatalf("unexpected resize frame: %+v", frame)
	}

	stream.fromWorker <- terminal.Frame{Type: terminal.FrameExit, ExitCode: 7}
	messageType, payload, err = connection.Read(ctx)
	if err != nil || messageType != websocket.MessageText || !strings.Contains(string(payload), `"exit_code":7`) {
		t.Fatalf("exit message type=%v payload=%s err=%v", messageType, payload, err)
	}
}

func newTerminalTestServer(t *testing.T, sessions plane.Service, placements handler.TerminalPlacementResolver, workers handler.TerminalWorker, manager *terminal.Manager) http.Handler {
	t.Helper()
	resolver := &fakePlatformService{record: platform.KeyRecord{
		ID:         "key-1",
		UserID:     "tenant-1",
		IsActive:   true,
		BalanceUSD: 10,
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sandboxes/{sandboxID}/terminals", handler.CreateTerminalHandler(sessions, placements, workers, manager))
	return middleware.Logging(middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux))
}

func createTerminal(t *testing.T, server http.Handler, sandboxID, apiKey string) handler.CreateTerminalResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/"+sandboxID+"/terminals", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want 201: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", rec.Header().Get("Cache-Control"))
	}
	if strings.Contains(rec.Body.String(), "worker-1") || strings.Contains(rec.Body.String(), "10.0.0.4") {
		t.Fatalf("response leaked private worker routing: %s", rec.Body.String())
	}
	var response handler.CreateTerminalResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode create terminal response: %v", err)
	}
	return response
}
