// Package modelproxy lets a sandbox's agent call a model without any provider
// key entering the guest.
//
// For each granted sandbox the worker listens on the guest's gateway address
// (172.16.0.1:7081) inside that sandbox's network namespace, like the git proxy
// (internal/gitproxy). The guest presents a random per-grant proxy token; the
// proxy checks it, keeps only Anthropic message requests and forwards them to
// the control plane's model relay (internal/modelrelay) with the worker token.
// The relay holds the provider key. A leaked proxy token is useless outside the
// sandbox's network slot, and the grant is revoked on pause and destroy.
package modelproxy

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"backend/internal/plane"
	"backend/internal/slotnet"
)

const (
	// Port is where the proxy listens inside each granted sandbox's namespace.
	Port = 7081

	maxGrantTTL       = 12 * time.Hour
	maxRequestBody    = 32 << 20
	maxConcurrent     = 4
	requestTimeout    = 30 * time.Minute
	readHeaderTimeout = 10 * time.Second
)

var ErrInvalidGrant = errors.New("invalid model access grant")

// forwardedHeaders are the only guest headers sent on. The guest's
// Authorization / X-Api-Key carry the proxy token and are never forwarded.
var forwardedHeaders = []string{"Accept", "Anthropic-Beta", "Anthropic-Version", "Content-Type", "User-Agent"}

// Grant lets one sandbox call one provider's model.
type Grant struct {
	Provider  string
	Model     string
	Token     string // proxy token the guest presents; never a provider key
	ExpiresAt time.Time
}

func (g Grant) Validate(now time.Time) error {
	if g.Provider != "anthropic" {
		return fmt.Errorf("%w: unsupported provider %q", ErrInvalidGrant, g.Provider)
	}
	if strings.TrimSpace(g.Model) == "" {
		return fmt.Errorf("%w: model is required", ErrInvalidGrant)
	}
	if len(g.Token) < 32 || strings.ContainsAny(g.Token, " \t\r\n") {
		return fmt.Errorf("%w: token must be at least 32 characters", ErrInvalidGrant)
	}
	if !g.ExpiresAt.After(now) {
		return fmt.Errorf("%w: grant has expired", ErrInvalidGrant)
	}
	if g.ExpiresAt.After(now.Add(maxGrantTTL)) {
		return fmt.Errorf("%w: grant is longer than %s", ErrInvalidGrant, maxGrantTTL)
	}
	return nil
}

// BaseURL is what the guest's agent uses as ANTHROPIC_BASE_URL.
func BaseURL() string {
	return "http://" + net.JoinHostPort(slotnet.GatewayAddr, strconv.Itoa(Port))
}

// ListenFunc opens a listener on address inside the network namespace of slot.
type ListenFunc func(slot int, address string) (net.Listener, error)

type Config struct {
	// RelayURL is the control plane's internal base URL (CONTROL_PLANE_INTERNAL_URL).
	RelayURL    string
	WorkerToken string
	// Transport, Listen and Now default to production implementations.
	Transport http.RoundTripper
	Listen    ListenFunc
	Now       func() time.Time
}

type Proxy struct {
	relay       *url.URL
	workerToken string
	transport   http.RoundTripper
	listen      ListenFunc
	now         func() time.Time

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
	relay, err := url.Parse(strings.TrimRight(cfg.RelayURL, "/"))
	if err != nil || (relay.Scheme != "http" && relay.Scheme != "https") || relay.Host == "" {
		return nil, fmt.Errorf("model proxy relay URL %q must be an absolute http(s) URL", cfg.RelayURL)
	}
	if cfg.WorkerToken == "" {
		return nil, errors.New("model proxy needs the worker token")
	}
	p := &Proxy{
		relay:       relay,
		workerToken: cfg.WorkerToken,
		transport:   cfg.Transport,
		listen:      cfg.Listen,
		now:         cfg.Now,
		bindings:    make(map[string]*binding),
	}
	if p.transport == nil {
		p.transport = http.DefaultTransport
	}
	if p.listen == nil {
		p.listen = slotnet.Listen
	}
	if p.now == nil {
		p.now = time.Now
	}
	return p, nil
}

// Bind grants sandboxID model access through a listener in its slot's
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

	listener, err := p.listen(slot, net.JoinHostPort(slotnet.GatewayAddr, strconv.Itoa(Port)))
	if err != nil {
		return fmt.Errorf("open model proxy listener for sandbox %s: %w", sandboxID, err)
	}
	b := &binding{sandboxID: sandboxID, slot: slot, grant: grant, inflight: make(chan struct{}, maxConcurrent)}
	b.server = &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { p.serve(b, w, r) }),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       5 * time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
	p.bindings[sandboxID] = b
	go func() {
		if err := b.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("model proxy listener stopped", "sandbox_id", sandboxID, "slot", slot, "err", err)
		}
	}()
	slog.Info("model access granted", "sandbox_id", sandboxID, "slot", slot,
		"provider", grant.Provider, "model", grant.Model, "expires_at", grant.ExpiresAt)
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
		slog.Info("model access revoked", "sandbox_id", sandboxID, "slot", b.slot)
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
		slog.Warn("close model proxy listener", "sandbox_id", b.sandboxID, "err", err)
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
		writeError(w, http.StatusForbidden, "permission_error", "model access was revoked")
		return
	}
	if !p.now().Before(grant.ExpiresAt) {
		writeError(w, http.StatusForbidden, "permission_error", "model access has expired")
		return
	}
	if !validToken(r, grant.Token) {
		writeError(w, http.StatusUnauthorized, "authentication_error", "invalid proxy token")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if r.Method != http.MethodPost || (path != "v1/messages" && path != "v1/messages/count_tokens") ||
		(r.URL.RawQuery != "" && r.URL.RawQuery != "beta=true") {
		writeError(w, http.StatusNotFound, "not_found_error", "only Anthropic message requests are allowed")
		return
	}
	select {
	case b.inflight <- struct{}{}:
		defer func() { <-b.inflight }()
	default:
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "too many concurrent model requests")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)

	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	target := *p.relay
	target.Path = strings.TrimRight(p.relay.Path, "/") + plane.RouteModelRelay + grant.Provider + "/" + path
	target.RawPath = ""
	target.RawQuery = r.URL.RawQuery
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
			header.Set("Authorization", "Bearer "+p.workerToken)
			header.Set(plane.SandboxIDHeader, b.sandboxID)
			out.Out.Header = header
		},
		Transport:     p.transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("model proxy relay failed", "sandbox_id", b.sandboxID, "err", err)
			writeError(w, http.StatusBadGateway, "api_error", "model relay unavailable")
		},
	}
	proxy.ServeHTTP(recorder, r)
	slog.Info("model proxy request", "sandbox_id", b.sandboxID, "path", path,
		"status", recorder.status, "duration_ms", p.now().Sub(started).Milliseconds())
}

// validToken accepts the proxy token the way Anthropic clients send keys:
// "Authorization: Bearer" (ANTHROPIC_AUTH_TOKEN) or "x-api-key" (ANTHROPIC_API_KEY).
func validToken(r *http.Request, token string) bool {
	provided := r.Header.Get("X-Api-Key")
	if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		provided = bearer
	}
	return provided != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1
}

// writeError answers in the Anthropic API's error shape, so agents show it.
func writeError(w http.ResponseWriter, status int, kind, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"type":"error","error":{"type":%q,"message":%q}}`, kind, message)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

// Flush keeps streamed responses (server-sent events) flowing as they arrive.
func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
