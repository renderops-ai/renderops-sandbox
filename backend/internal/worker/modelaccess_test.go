package worker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"backend/internal/modelproxy"
	"backend/internal/plane"
)

type recordingModelProxy struct {
	bound   []string
	slot    int
	grant   modelproxy.Grant
	unbound []string
}

func (r *recordingModelProxy) Bind(id string, slot int, grant modelproxy.Grant) error {
	if err := grant.Validate(time.Now()); err != nil {
		return err
	}
	r.bound, r.slot, r.grant = append(r.bound, id), slot, grant
	return nil
}

func (r *recordingModelProxy) Unbind(id string) { r.unbound = append(r.unbound, id) }

func modelAccessRequest(t *testing.T, server *Server, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/worker/sandbox/sbx-1/model-access", bytes.NewBufferString(body))
	request.Header.Set("X-Worker-Token", "worker-secret")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	return response
}

func modelGrantBody(t *testing.T, token string) string {
	t.Helper()
	body, _ := json.Marshal(plane.ModelAccessRequest{
		Provider: "anthropic", Model: "claude-opus-5-5", Token: token,
		ExpiresAt: time.Now().Add(time.Hour).UTC().Truncate(time.Second),
	})
	return string(body)
}

func TestGrantModelAccessBindsTheLiveSlot(t *testing.T) {
	proxy := &recordingModelProxy{}
	server := NewServer(gitSessionService{fakeSessionService: &fakeSessionService{}, slot: 7}, "worker-secret", 10)
	server.model = proxy
	token := strings.Repeat("a", 40)

	response := modelAccessRequest(t, server, http.MethodPut, modelGrantBody(t, token))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var out plane.ModelAccessResponse
	if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.BaseURL != "http://172.16.0.1:7081" || proxy.slot != 7 || proxy.grant.Token != token || len(proxy.bound) != 1 {
		t.Fatalf("out=%+v proxy=%+v", out, proxy)
	}
	if strings.Contains(response.Body.String(), token) {
		t.Fatal("response echoed the proxy token")
	}
}

func TestGrantModelAccessErrors(t *testing.T) {
	short := modelAccessRequest(t, func() *Server {
		s := NewServer(gitSessionService{fakeSessionService: &fakeSessionService{}, slot: 7}, "worker-secret", 10)
		s.model = &recordingModelProxy{}
		return s
	}(), http.MethodPut, modelGrantBody(t, "short"))
	if short.Code != http.StatusBadRequest {
		t.Errorf("invalid grant: status=%d", short.Code)
	}

	paused := NewServer(gitSessionService{fakeSessionService: &fakeSessionService{}, slot: -1}, "worker-secret", 10)
	paused.model = &recordingModelProxy{}
	if got := modelAccessRequest(t, paused, http.MethodPut, modelGrantBody(t, strings.Repeat("a", 40))); got.Code != http.StatusConflict {
		t.Errorf("paused sandbox: status=%d", got.Code)
	}

	disabled := NewServer(gitSessionService{fakeSessionService: &fakeSessionService{}, slot: 7}, "worker-secret", 10)
	if got := modelAccessRequest(t, disabled, http.MethodPut, modelGrantBody(t, strings.Repeat("a", 40))); got.Code != http.StatusNotImplemented {
		t.Errorf("no proxy: status=%d", got.Code)
	}
}

func TestRevokeModelAccessUnbinds(t *testing.T) {
	proxy := &recordingModelProxy{}
	server := NewServer(gitSessionService{fakeSessionService: &fakeSessionService{}, slot: 7}, "worker-secret", 10)
	server.model = proxy
	if got := modelAccessRequest(t, server, http.MethodDelete, ""); got.Code != http.StatusNoContent || len(proxy.unbound) != 1 {
		t.Fatalf("status=%d unbound=%v", got.Code, proxy.unbound)
	}
}
