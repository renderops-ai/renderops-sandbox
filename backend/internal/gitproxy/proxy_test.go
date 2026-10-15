package gitproxy

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type upstreamCall struct {
	method, path, query, auth, protocol, cookie, body string
}

type harness struct {
	t        *testing.T
	proxy    *Proxy
	upstream *httptest.Server
	now      time.Time

	mu    sync.Mutex
	calls []upstreamCall
	addrs map[int]string // slot -> listener address
}

func newHarness(t *testing.T, handler http.HandlerFunc) *harness {
	t.Helper()
	h := &harness{t: t, now: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC), addrs: map[int]string{}}
	if handler == nil {
		handler = func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "pack-data") }
	}
	h.upstream = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.calls = append(h.calls, upstreamCall{
			method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, auth: r.Header.Get("Authorization"),
			protocol: r.Header.Get("Git-Protocol"), cookie: r.Header.Get("Cookie"), body: string(body),
		})
		h.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(h.upstream.Close)

	proxy, err := New(Config{
		Upstream:  h.upstream.URL,
		Transport: h.upstream.Client().Transport,
		Now:       func() time.Time { return h.now },
		Listen: func(slot int, address string) (net.Listener, error) {
			if address != "172.16.0.1:7080" {
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

func (h *harness) grant(repository string) Grant {
	return Grant{Repository: repository, Token: "ghs_secret", ExpiresAt: h.now.Add(time.Hour)}
}

func (h *harness) do(slot int, method, path, body string, header http.Header) *http.Response {
	h.t.Helper()
	h.mu.Lock()
	addr := h.addrs[slot]
	h.mu.Unlock()
	request, err := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	for name, values := range header {
		request.Header[name] = values
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	h.t.Cleanup(func() { response.Body.Close() })
	return response
}

func (h *harness) upstreamCalls() []upstreamCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]upstreamCall(nil), h.calls...)
}

func TestFetchIsForwardedWithInjectedTokenOnly(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.proxy.Bind("sbx-1", 3, h.grant("acme/widgets")); err != nil {
		t.Fatal(err)
	}

	refs := h.do(3, http.MethodGet, "/acme/widgets.git/info/refs?service=git-upload-pack", "", http.Header{
		"Authorization": {"Basic Z3Vlc3Q6Z3Vlc3Q="}, "Cookie": {"session=guest"}, "Git-Protocol": {"version=2"},
	})
	if refs.StatusCode != http.StatusOK {
		t.Fatalf("info/refs status = %d", refs.StatusCode)
	}
	pack := h.do(3, http.MethodPost, "/acme/widgets.git/git-upload-pack", "0014command=fetch", http.Header{
		"Content-Type": {"application/x-git-upload-pack-request"}, "Git-Protocol": {"version=2"},
	})
	if body, _ := io.ReadAll(pack.Body); pack.StatusCode != http.StatusOK || string(body) != "pack-data" {
		t.Fatalf("upload-pack status=%d body=%q", pack.StatusCode, body)
	}

	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ghs_secret"))
	calls := h.upstreamCalls()
	if len(calls) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(calls))
	}
	for _, call := range calls {
		if call.auth != wantAuth {
			t.Errorf("%s %s auth = %q, want the injected installation token", call.method, call.path, call.auth)
		}
		if call.cookie != "" || call.protocol != "version=2" {
			t.Errorf("%s forwarded cookie=%q protocol=%q", call.path, call.cookie, call.protocol)
		}
	}
	if calls[0].path != "/acme/widgets.git/info/refs" || calls[0].query != "service=git-upload-pack" {
		t.Errorf("info/refs forwarded as %s?%s", calls[0].path, calls[0].query)
	}
	if calls[1].path != "/acme/widgets.git/git-upload-pack" || calls[1].body != "0014command=fetch" {
		t.Errorf("upload-pack forwarded as %s body=%q", calls[1].path, calls[1].body)
	}
}

func TestOnlyReadOnlyFetchOfTheGrantedRepositoryIsAllowed(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.proxy.Bind("sbx-1", 1, h.grant("acme/widgets")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/acme/widgets.git/info/refs?service=git-receive-pack"},
		{http.MethodPost, "/acme/widgets.git/git-receive-pack"},
		{http.MethodGet, "/acme/other.git/info/refs?service=git-upload-pack"},
		{http.MethodPost, "/acme/other.git/git-upload-pack"},
		{http.MethodGet, "/acme/widgets.git/info/lfs/objects/batch"},
		{http.MethodPost, "/acme/widgets.git/info/lfs/objects/batch"},
		{http.MethodGet, "/acme/widgets.git/git-upload-pack"},
		{http.MethodPost, "/acme/widgets.git/info/refs?service=git-upload-pack"},
		{http.MethodGet, "/acme/widgets.git/info/refs?service=git-upload-pack&x=1"},
		{http.MethodGet, "/acme/widgets.git/../other.git/info/refs?service=git-upload-pack"},
		{http.MethodGet, "/"},
	} {
		if response := h.do(1, tc.method, tc.path, "", nil); response.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s status = %d, want 403", tc.method, tc.path, response.StatusCode)
		}
	}
	if calls := h.upstreamCalls(); len(calls) != 0 {
		t.Fatalf("refused requests reached upstream: %+v", calls)
	}
}

func TestExpiredAndRevokedGrantsAreRefused(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.proxy.Bind("sbx-1", 2, h.grant("acme/widgets")); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(2 * time.Hour)
	if response := h.do(2, http.MethodGet, "/acme/widgets.git/info/refs?service=git-upload-pack", "", nil); response.StatusCode != http.StatusForbidden {
		t.Fatalf("expired grant status = %d, want 403", response.StatusCode)
	}

	h.now = h.now.Add(-2 * time.Hour)
	h.proxy.Unbind("sbx-1")
	h.proxy.Unbind("sbx-1") // idempotent
	h.mu.Lock()
	addr := h.addrs[2]
	h.mu.Unlock()
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		t.Fatal("listener still accepting after Unbind")
	}
	if calls := h.upstreamCalls(); len(calls) != 0 {
		t.Fatalf("expired or revoked grant reached upstream: %+v", calls)
	}
}

func TestUpstreamRedirectIsNotPassedToTheGuest(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://elsewhere.example/acme/widgets.git/info/refs", http.StatusFound)
	})
	if err := h.proxy.Bind("sbx-1", 1, h.grant("acme/widgets")); err != nil {
		t.Fatal(err)
	}
	response := h.do(1, http.MethodGet, "/acme/widgets.git/info/refs?service=git-upload-pack", "", nil)
	if response.StatusCode != http.StatusBadGateway || response.Header.Get("Location") != "" {
		t.Fatalf("redirect status=%d location=%q, want 502 without Location", response.StatusCode, response.Header.Get("Location"))
	}
}

func TestRebindRefreshesGrantAndReusedSlotEvictsStaleBinding(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.proxy.Bind("sbx-1", 4, h.grant("acme/widgets")); err != nil {
		t.Fatal(err)
	}
	refreshed := h.grant("acme/widgets")
	refreshed.Token = "ghs_refreshed"
	if err := h.proxy.Bind("sbx-1", 4, refreshed); err != nil {
		t.Fatal(err)
	}
	h.do(4, http.MethodGet, "/acme/widgets.git/info/refs?service=git-upload-pack", "", nil)
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ghs_refreshed"))
	if calls := h.upstreamCalls(); len(calls) != 1 || calls[0].auth != wantAuth {
		t.Fatalf("refreshed grant not used: %+v", calls)
	}

	// The slot is later reused by another sandbox: the old grant must not survive.
	if err := h.proxy.Bind("sbx-2", 4, h.grant("acme/gadgets")); err != nil {
		t.Fatal(err)
	}
	if response := h.do(4, http.MethodGet, "/acme/widgets.git/info/refs?service=git-upload-pack", "", nil); response.StatusCode != http.StatusForbidden {
		t.Fatalf("stale grant still served on reused slot: %d", response.StatusCode)
	}
	h.proxy.mu.Lock()
	_, stale := h.proxy.bindings["sbx-1"]
	h.proxy.mu.Unlock()
	if stale {
		t.Fatal("sbx-1 binding survived slot reuse")
	}
}

func TestGrantValidation(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	valid := Grant{Repository: "acme/widgets", Token: "ghs_x", ExpiresAt: now.Add(time.Hour)}
	if err := valid.Validate(now); err != nil {
		t.Fatalf("valid grant rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Grant){
		"bare name":      func(g *Grant) { g.Repository = "widgets" },
		"traversal":      func(g *Grant) { g.Repository = "acme/.." },
		"extra segment":  func(g *Grant) { g.Repository = "acme/widgets/x" },
		"url":            func(g *Grant) { g.Repository = "https://github.com/acme/widgets" },
		"empty token":    func(g *Grant) { g.Token = "" },
		"token newline":  func(g *Grant) { g.Token = "ghs_x\nHost: evil" },
		"expired":        func(g *Grant) { g.ExpiresAt = now },
		"too long-lived": func(g *Grant) { g.ExpiresAt = now.Add(3 * time.Hour) },
	} {
		grant := valid
		mutate(&grant)
		if err := grant.Validate(now); !errors.Is(err, ErrInvalidGrant) {
			t.Errorf("%s: err = %v, want ErrInvalidGrant", name, err)
		}
	}
}

func TestNewRequiresHTTPSUpstream(t *testing.T) {
	for _, upstream := range []string{"", "github.com", "http://github.com", "ftp://github.com"} {
		if _, err := New(Config{Upstream: upstream}); err == nil {
			t.Errorf("New(%q) succeeded", upstream)
		}
	}
	if got := RemoteURL("acme/widgets"); got != "http://172.16.0.1:7080/acme/widgets.git" {
		t.Errorf("RemoteURL = %q", got)
	}
}
