package modelproxy

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/plane"
)

const proxyToken = "proxy-token-0123456789abcdef0123456789"

type relayCall struct {
	path, auth, sandbox, apiKey, cookie, body string
}

type harness struct {
	t     *testing.T
	proxy *Proxy
	relay *httptest.Server
	now   time.Time

	mu    sync.Mutex
	calls []relayCall
	addrs map[int]string // slot -> listener address
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, now: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC), addrs: map[int]string{}}
	h.relay = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.calls = append(h.calls, relayCall{
			path: r.URL.Path, auth: r.Header.Get("Authorization"), sandbox: r.Header.Get(plane.SandboxIDHeader),
			apiKey: r.Header.Get("X-Api-Key"), cookie: r.Header.Get("Cookie"), body: string(body),
		})
		h.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {}\n\n")
	}))
	t.Cleanup(h.relay.Close)
	proxy, err := New(Config{
		RelayURL:    h.relay.URL,
		WorkerToken: "worker-token",
		Now:         func() time.Time { return h.now },
		Listen: func(slot int, address string) (net.Listener, error) {
			if address != "172.16.0.1:7081" {
				t.Fatalf("listen address = %s", address)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			h.mu.Lock()
			h.addrs[slot] = listener.Addr().String()
			h.mu.Unlock()
			return listener, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	h.proxy = proxy
	return h
}

func (h *harness) grant() Grant {
	return Grant{Provider: "anthropic", Model: "claude-opus-5-5", Token: proxyToken, ExpiresAt: h.now.Add(time.Hour)}
}

func (h *harness) post(slot int, path string, header http.Header) (*http.Response, error) {
	h.mu.Lock()
	addr := h.addrs[slot]
	h.mu.Unlock()
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+path, strings.NewReader(`{"model":"claude-opus-5-5"}`))
	for k, v := range header {
		req.Header[k] = v
	}
	return http.DefaultClient.Do(req)
}

func bearer(token string) http.Header {
	return http.Header{"Authorization": {"Bearer " + token}, "Anthropic-Version": {"2023-06-01"}, "Cookie": {"guest=1"}}
}

func TestProxyForwardsToTheRelayWithWorkerAuth(t *testing.T) {
	h := newHarness(t)
	if err := h.proxy.Bind("sbx-1", 3, h.grant()); err != nil {
		t.Fatal(err)
	}
	resp, err := h.post(3, "/v1/messages", bearer(proxyToken))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "message_start") {
		t.Fatalf("status %d body %q", resp.StatusCode, body)
	}
	if len(h.calls) != 1 {
		t.Fatalf("relay calls = %d", len(h.calls))
	}
	call := h.calls[0]
	if call.path != "/internal/model/anthropic/v1/messages" || call.auth != "Bearer worker-token" || call.sandbox != "sbx-1" {
		t.Errorf("relay call = %+v", call)
	}
	if call.apiKey != "" || call.cookie != "" || strings.Contains(call.auth, proxyToken) {
		t.Errorf("guest credentials reached the relay: %+v", call)
	}
	if call.body != `{"model":"claude-opus-5-5"}` {
		t.Errorf("body = %q", call.body)
	}
}

func TestProxyAcceptsTheTokenAsXAPIKey(t *testing.T) {
	h := newHarness(t)
	if err := h.proxy.Bind("sbx-1", 3, h.grant()); err != nil {
		t.Fatal(err)
	}
	resp, err := h.post(3, "/v1/messages/count_tokens", http.Header{"X-Api-Key": {proxyToken}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestProxyRefuses(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		header http.Header
		status int
	}{
		{"no token", "/v1/messages", http.Header{}, http.StatusUnauthorized},
		{"wrong token", "/v1/messages", bearer("proxy-token-wrong-wrong-wrong-wrong-wr"), http.StatusUnauthorized},
		{"other path", "/v1/models", bearer(proxyToken), http.StatusNotFound},
		{"other query", "/v1/messages?debug=1", bearer(proxyToken), http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			if err := h.proxy.Bind("sbx-1", 3, h.grant()); err != nil {
				t.Fatal(err)
			}
			resp, err := h.post(3, c.path, c.header)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != c.status || len(h.calls) != 0 {
				t.Fatalf("status %d (want %d), relay calls %d", resp.StatusCode, c.status, len(h.calls))
			}
		})
	}
}

func TestUnbindClosesTheListener(t *testing.T) {
	h := newHarness(t)
	if err := h.proxy.Bind("sbx-1", 3, h.grant()); err != nil {
		t.Fatal(err)
	}
	h.proxy.Unbind("sbx-1")
	if resp, err := h.post(3, "/v1/messages", bearer(proxyToken)); err == nil {
		resp.Body.Close()
		t.Fatalf("request after unbind got %d", resp.StatusCode)
	}
}

func TestExpiredGrantIsRefused(t *testing.T) {
	h := newHarness(t)
	if err := h.proxy.Bind("sbx-1", 3, h.grant()); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(2 * time.Hour)
	resp, err := h.post(3, "/v1/messages", bearer(proxyToken))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || len(h.calls) != 0 {
		t.Fatalf("status %d, relay calls %d", resp.StatusCode, len(h.calls))
	}
}

func TestBindReclaimsASlotFromAnotherSandbox(t *testing.T) {
	h := newHarness(t)
	if err := h.proxy.Bind("old", 3, h.grant()); err != nil {
		t.Fatal(err)
	}
	if err := h.proxy.Bind("new", 3, h.grant()); err != nil {
		t.Fatal(err)
	}
	h.proxy.mu.Lock()
	_, oldStillBound := h.proxy.bindings["old"]
	h.proxy.mu.Unlock()
	if oldStillBound {
		t.Fatal("old sandbox kept its binding on a reused slot")
	}
}

func TestGrantValidation(t *testing.T) {
	now := time.Now()
	good := Grant{Provider: "anthropic", Model: "m", Token: proxyToken, ExpiresAt: now.Add(time.Hour)}
	if err := good.Validate(now); err != nil {
		t.Fatal(err)
	}
	for name, g := range map[string]Grant{
		"openai":      {Provider: "openai", Model: "m", Token: proxyToken, ExpiresAt: now.Add(time.Hour)},
		"short token": {Provider: "anthropic", Model: "m", Token: "short", ExpiresAt: now.Add(time.Hour)},
		"no model":    {Provider: "anthropic", Token: proxyToken, ExpiresAt: now.Add(time.Hour)},
		"expired":     {Provider: "anthropic", Model: "m", Token: proxyToken, ExpiresAt: now.Add(-time.Minute)},
		"too long":    {Provider: "anthropic", Model: "m", Token: proxyToken, ExpiresAt: now.Add(48 * time.Hour)},
	} {
		if err := g.Validate(now); !errors.Is(err, ErrInvalidGrant) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
