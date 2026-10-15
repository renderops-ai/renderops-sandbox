package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"backend/internal/gitproxy"
	"backend/internal/plane"
	"backend/internal/session"
)

type gitSessionService struct {
	*fakeSessionService
	slot int
}

func (g gitSessionService) BindGitAccess(id string, bind func(int) error) error {
	if g.slot < 0 {
		return fmt.Errorf("session %s is not active", id)
	}
	return bind(g.slot)
}

type recordingGitProxy struct {
	bound    []string
	slot     int
	grant    gitproxy.Grant
	unbound  []string
	listenEr error
}

func (r *recordingGitProxy) Bind(id string, slot int, grant gitproxy.Grant) error {
	if err := grant.Validate(time.Now()); err != nil {
		return err
	}
	if r.listenEr != nil {
		return r.listenEr
	}
	r.bound, r.slot, r.grant = append(r.bound, id), slot, grant
	return nil
}

func (r *recordingGitProxy) Unbind(id string) { r.unbound = append(r.unbound, id) }

func gitAccessRequest(t *testing.T, server *Server, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/worker/sandbox/sbx-1/git-access", bytes.NewBufferString(body))
	request.Header.Set("X-Worker-Token", "worker-secret")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	return response
}

func TestGrantGitAccessBindsLiveSlotAndKeepsTokenOnWorker(t *testing.T) {
	proxy := &recordingGitProxy{}
	server := NewServer(gitSessionService{fakeSessionService: &fakeSessionService{}, slot: 7}, "worker-secret", 10)
	server.git = proxy
	expires := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	body, _ := json.Marshal(plane.GitAccessRequest{Repository: "acme/widgets", Token: "ghs_secret", ExpiresAt: expires})

	response := gitAccessRequest(t, server, http.MethodPut, string(body))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var out plane.GitAccessResponse
	if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.RemoteURL != "http://172.16.0.1:7080/acme/widgets.git" || !out.ExpiresAt.Equal(expires) {
		t.Fatalf("response = %+v", out)
	}
	if strings.Contains(response.Body.String(), "ghs_secret") {
		t.Fatal("token echoed back in the response")
	}
	if proxy.slot != 7 || proxy.grant.Token != "ghs_secret" || len(proxy.bound) != 1 {
		t.Fatalf("proxy bind = slot %d grant %+v", proxy.slot, proxy.grant)
	}

	if response := gitAccessRequest(t, server, http.MethodDelete, ""); response.Code != http.StatusNoContent || len(proxy.unbound) != 1 {
		t.Fatalf("revoke status=%d unbound=%v", response.Code, proxy.unbound)
	}
}

func TestGrantGitAccessRejections(t *testing.T) {
	valid, _ := json.Marshal(plane.GitAccessRequest{Repository: "acme/widgets", Token: "ghs_secret", ExpiresAt: time.Now().Add(time.Hour)})
	expired, _ := json.Marshal(plane.GitAccessRequest{Repository: "acme/widgets", Token: "ghs_secret", ExpiresAt: time.Now().Add(-time.Minute)})
	for name, tc := range map[string]struct {
		svc    session.Service
		proxy  *recordingGitProxy
		body   string
		status int
	}{
		"no proxy":        {svc: gitSessionService{&fakeSessionService{}, 7}, body: string(valid), status: http.StatusNotImplemented},
		"no slot binder":  {svc: &fakeSessionService{}, proxy: &recordingGitProxy{}, body: string(valid), status: http.StatusNotImplemented},
		"unknown field":   {svc: gitSessionService{&fakeSessionService{}, 7}, proxy: &recordingGitProxy{}, body: `{"repository":"acme/widgets","scope":"write"}`, status: http.StatusBadRequest},
		"expired grant":   {svc: gitSessionService{&fakeSessionService{}, 7}, proxy: &recordingGitProxy{}, body: string(expired), status: http.StatusBadRequest},
		"not active":      {svc: gitSessionService{&fakeSessionService{}, -1}, proxy: &recordingGitProxy{}, body: string(valid), status: http.StatusConflict},
		"listener failed": {svc: gitSessionService{&fakeSessionService{}, 7}, proxy: &recordingGitProxy{listenEr: errors.New("netns gone")}, body: string(valid), status: http.StatusInternalServerError},
	} {
		server := NewServer(tc.svc, "worker-secret", 10)
		if tc.proxy != nil {
			server.git = tc.proxy
		}
		response := gitAccessRequest(t, server, http.MethodPut, tc.body)
		if response.Code != tc.status {
			t.Errorf("%s: status=%d want %d body=%s", name, response.Code, tc.status, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "ghs_secret") {
			t.Errorf("%s: token in error body", name)
		}
		if tc.proxy != nil && tc.status != http.StatusOK && len(tc.proxy.bound) != 0 {
			t.Errorf("%s: grant bound despite rejection", name)
		}
	}
}

func TestGitAccessRequiresWorkerToken(t *testing.T) {
	server := NewServer(gitSessionService{&fakeSessionService{}, 7}, "worker-secret", 10)
	server.git = &recordingGitProxy{}
	request := httptest.NewRequest(http.MethodPut, "/worker/sandbox/sbx-1/git-access", bytes.NewBufferString(`{}`))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", response.Code)
	}
}
