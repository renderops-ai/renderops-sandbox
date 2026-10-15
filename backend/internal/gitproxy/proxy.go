// Package gitproxy gives a sandbox read-only access to one GitHub repository
// without any credential entering the guest.
//
// For each granted sandbox the worker listens on the guest's gateway address
// (172.16.0.1) inside that sandbox's network namespace. Guest traffic to the
// gateway is delivered locally in the namespace, so it works with the host
// egress firewall and with the sandbox's internet switched off. Only git
// smart-HTTP fetch requests for the granted repository are forwarded upstream,
// and the proxy adds a short-lived, repository-scoped installation token on the
// way out. Push, LFS, other repositories and redirects are refused.
//
// Grants are looked up in the proxy's own table, never through the sandbox
// execution lock: the git fetch that uses the proxy runs as a guest exec,
// which holds that lock for its whole duration.
package gitproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// GatewayAddr is the address every slot's TAP holds: the guest's gateway.
	GatewayAddr = "172.16.0.1"
	// Port is where the proxy listens inside each granted sandbox's namespace.
	Port = 7080

	maxGrantTTL       = 2 * time.Hour
	maxRequestBody    = 16 << 20
	maxConcurrent     = 4
	requestTimeout    = 15 * time.Minute
	readHeaderTimeout = 10 * time.Second
)

var repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9._-]{1,100}$`)

var (
	ErrInvalidGrant = errors.New("invalid git access grant")
	errRedirect     = errors.New("upstream redirect refused")
)

// forwardedHeaders are the only guest headers sent upstream. Git needs nothing
// else; dropping the rest keeps the guest from injecting credentials, cookies
// or hop-by-hop tricks.
var forwardedHeaders = []string{"Accept", "Accept-Encoding", "Content-Encoding", "Content-Type", "Git-Protocol", "User-Agent"}

// Grant authorizes read-only fetches of one repository. Token is a GitHub App
// installation token narrowed to this repository with contents:read. It is
// never logged and never sent to the guest.
type Grant struct {
	Repository string
	Token      string
	ExpiresAt  time.Time
}

// Validate rejects grants that are malformed, already expired, or longer-lived
// than an installation token can be.
func (g Grant) Validate(now time.Time) error {
	if !repositoryPattern.MatchString(g.Repository) || strings.Contains(g.Repository, "..") {
		return fmt.Errorf("%w: repository must be owner/name", ErrInvalidGrant)
	}
	if strings.TrimSpace(g.Token) == "" || strings.ContainsAny(g.Token, " \t\r\n") {
		return fmt.Errorf("%w: token is required", ErrInvalidGrant)
	}
	if !g.ExpiresAt.After(now) {
		return fmt.Errorf("%w: grant has expired", ErrInvalidGrant)
	}
	if g.ExpiresAt.After(now.Add(maxGrantTTL)) {
		return fmt.Errorf("%w: grant outlives the maximum token lifetime", ErrInvalidGrant)
	}
	return nil
}

// RemoteURL is the guest-side git remote for a repository served by the proxy.
func RemoteURL(repository string) string {
	return "http://" + net.JoinHostPort(GatewayAddr, strconv.Itoa(Port)) + "/" + repository + ".git"
}

// ListenFunc opens a listener on address inside the network namespace of slot.
type ListenFunc func(slot int, address string) (net.Listener, error)

type Config struct {
	// Upstream is the git host, e.g. https://github.com.
	Upstream string
	// Transport, Listen and Now default to production implementations.
	Transport http.RoundTripper
	Listen    ListenFunc
	Now       func() time.Time
}

type Proxy struct {
	upstream  *url.URL
	transport http.RoundTripper
	listen    ListenFunc
	now       func() time.Time

	mu       sync.Mutex
	bindings map[string]*binding // by sandbox ID
}

type binding struct {
	sandboxID string
	slot      int
	grant     Grant // guarded by Proxy.mu
	server    *http.Server
	inflight  chan struct{}
}

func New(cfg Config) (*Proxy, error) {
	upstream, err := url.Parse(strings.TrimRight(cfg.Upstream, "/"))
	if err != nil || upstream.Scheme != "https" || upstream.Host == "" {
		return nil, fmt.Errorf("git proxy upstream %q must be an absolute https URL", cfg.Upstream)
	}
	p := &Proxy{
		upstream:  upstream,
		transport: cfg.Transport,
		listen:    cfg.Listen,
		now:       cfg.Now,
		bindings:  make(map[string]*binding),
	}
	if p.transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = 2 * time.Minute
		p.transport = transport
	}
	if p.listen == nil {
		p.listen = listenInSlot
	}
	if p.now == nil {
		p.now = time.Now
	}
	return p, nil
}

// Bind grants sandboxID read-only access through a listener in its slot's
// namespace. Binding the same sandbox and slot again refreshes the grant; a
// different slot (after resume) replaces the listener. A slot still held by
// another sandbox is reclaimed, since slots are only reused after release.
func (p *Proxy) Bind(sandboxID string, slot int, grant Grant) error {
	if sandboxID == "" || slot < 0 {
		return fmt.Errorf("%w: sandbox and slot are required", ErrInvalidGrant)
	}
	if err := grant.Validate(p.now()); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.bindings[sandboxID]; existing != nil && existing.slot == slot {
		existing.grant = grant
		return nil
	}
	var stale []*binding
	for id, b := range p.bindings {
		if id == sandboxID || b.slot == slot {
			stale = append(stale, b)
			delete(p.bindings, id)
		}
	}
	for _, b := range stale {
		closeBinding(b)
	}

	listener, err := p.listen(slot, net.JoinHostPort(GatewayAddr, strconv.Itoa(Port)))
	if err != nil {
		return fmt.Errorf("open git proxy listener for sandbox %s: %w", sandboxID, err)
	}
	b := &binding{sandboxID: sandboxID, slot: slot, grant: grant, inflight: make(chan struct{}, maxConcurrent)}
	b.server = &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { p.serve(b, w, r) }),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
	p.bindings[sandboxID] = b
	go func() {
		if err := b.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("git proxy listener stopped", "sandbox_id", sandboxID, "slot", slot, "err", err)
		}
	}()
	slog.Info("git access granted", "sandbox_id", sandboxID, "slot", slot,
		"repository", grant.Repository, "expires_at", grant.ExpiresAt)
	return nil
}

// Unbind revokes sandboxID's grant and closes its listener and any open
// connections. It is idempotent and must run before the slot is released.
func (p *Proxy) Unbind(sandboxID string) {
	p.mu.Lock()
	b := p.bindings[sandboxID]
	delete(p.bindings, sandboxID)
	p.mu.Unlock()
	if b != nil {
		closeBinding(b)
		slog.Info("git access revoked", "sandbox_id", sandboxID, "slot", b.slot)
	}
}

// Close revokes every grant (worker shutdown).
func (p *Proxy) Close() {
	p.mu.Lock()
	bindings := p.bindings
	p.bindings = make(map[string]*binding)
	p.mu.Unlock()
	for _, b := range bindings {
		closeBinding(b)
	}
}

func closeBinding(b *binding) {
	if err := b.server.Close(); err != nil {
		slog.Warn("close git proxy listener", "sandbox_id", b.sandboxID, "err", err)
	}
}

// currentGrant returns the grant only while b is still the live binding, so a
// request racing an unbind or rebind is refused.
func (p *Proxy) currentGrant(b *binding) (Grant, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bindings[b.sandboxID] != b {
		return Grant{}, false
	}
	return b.grant, true
}

func (p *Proxy) serve(b *binding, w http.ResponseWriter, r *http.Request) {
	started := p.now()
	grant, ok := p.currentGrant(b)
	if !ok {
		http.Error(w, "git access revoked", http.StatusForbidden)
		return
	}
	if !p.now().Before(grant.ExpiresAt) {
		http.Error(w, "git access expired", http.StatusForbidden)
		return
	}
	route, query, ok := allowedRoute(grant.Repository, r)
	if !ok {
		slog.Warn("git proxy request refused", "sandbox_id", b.sandboxID, "method", r.Method, "path", r.URL.Path)
		http.Error(w, "only read-only fetches of the granted repository are allowed", http.StatusForbidden)
		return
	}
	select {
	case b.inflight <- struct{}{}:
		defer func() { <-b.inflight }()
	default:
		http.Error(w, "too many concurrent git requests", http.StatusTooManyRequests)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)

	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	target := *p.upstream
	target.Path = strings.TrimRight(p.upstream.Path, "/") + "/" + grant.Repository + ".git/" + route
	target.RawPath = ""
	target.RawQuery = query
	proxy := &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			out.Out.URL = &target
			out.Out.Host = target.Host
			header := make(http.Header)
			for _, name := range forwardedHeaders {
				if values := out.In.Header.Values(name); len(values) > 0 {
					header[name] = values
				}
			}
			out.Out.Header = header
			out.Out.SetBasicAuth("x-access-token", grant.Token)
		},
		Transport:     p.transport,
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode >= 300 && resp.StatusCode < 400 {
				return errRedirect
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("git proxy upstream failed", "sandbox_id", b.sandboxID, "repository", grant.Repository, "err", err)
			http.Error(w, "git upstream unavailable", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(recorder, r)
	slog.Info("git proxy request", "sandbox_id", b.sandboxID, "repository", grant.Repository,
		"route", route, "status", recorder.status, "duration_ms", p.now().Sub(started).Milliseconds())
}

// allowedRoute admits exactly the two git smart-HTTP fetch endpoints of the
// granted repository and returns the upstream route and query. The upstream
// path is built from the grant, never from the request.
func allowedRoute(repository string, r *http.Request) (route, query string, ok bool) {
	prefix := "/" + repository + ".git/"
	if len(r.URL.Path) <= len(prefix) || !strings.EqualFold(r.URL.Path[:len(prefix)], prefix) {
		return "", "", false
	}
	switch rest := r.URL.Path[len(prefix):]; {
	case rest == "info/refs" && r.Method == http.MethodGet && r.URL.RawQuery == "service=git-upload-pack":
		return "info/refs", "service=git-upload-pack", true
	case rest == "git-upload-pack" && r.Method == http.MethodPost && r.URL.RawQuery == "":
		return "git-upload-pack", "", true
	}
	return "", "", false
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

// Flush keeps ReverseProxy streaming pack data as it arrives.
func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
