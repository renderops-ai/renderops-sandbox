// Package modelrelay is the control plane's half of the model proxy. Worker
// model proxies (internal/modelproxy) forward a sandbox's model requests here
// with the worker token; the relay looks up the sandbox owner's provider
// credentials (saved encrypted on the Secrets page), adds the key and forwards
// to the provider. Provider keys never leave the control plane: not to the
// worker, not to the guest.
package modelrelay

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/secrets"
)

const (
	// DefaultAnthropicModel is used when a saved Anthropic secret names no model.
	DefaultAnthropicModel = "claude-opus-5-5"
	defaultAnthropicURL   = "https://api.anthropic.com"

	maxRequestBody        = 32 << 20
	maxConcurrentPerSbx   = 4
	requestTimeout        = 30 * time.Minute
	responseHeaderTimeout = 10 * time.Minute
)

// ErrNoCredentials means the sandbox owner has not saved credentials for the provider.
var ErrNoCredentials = errors.New("no saved credentials for this provider")

// forwardedHeaders are the only request headers sent to the provider; the
// worker's auth and anything else the guest set are dropped.
var forwardedHeaders = []string{"Accept", "Anthropic-Beta", "Anthropic-Version", "Content-Type", "User-Agent"}

// Store reads what the relay needs from the platform database.
type Store interface {
	GetSandbox(ctx context.Context, id string) (platform.SandboxRef, bool, error)
	ProviderSecret(ctx context.Context, userID, kind string) (string, bool, error)
}

// Credentials are a provider endpoint, its key and the one model allowed.
type Credentials struct {
	BaseURL string
	APIKey  string
	Model   string
}

// AnthropicCredentials reads a saved Anthropic secret: either Azure AI Foundry
// (ANTHROPIC_FOUNDRY_*) or the Anthropic API (ANTHROPIC_API_KEY).
func AnthropicCredentials(values map[string]string) (Credentials, error) {
	creds := Credentials{Model: values["ANTHROPIC_MODEL"]}
	switch {
	case values["ANTHROPIC_FOUNDRY_API_KEY"] != "":
		creds.BaseURL, creds.APIKey = values["ANTHROPIC_FOUNDRY_BASE_URL"], values["ANTHROPIC_FOUNDRY_API_KEY"]
	case values["ANTHROPIC_API_KEY"] != "":
		creds.BaseURL, creds.APIKey = values["ANTHROPIC_BASE_URL"], values["ANTHROPIC_API_KEY"]
		if creds.BaseURL == "" {
			creds.BaseURL = defaultAnthropicURL
		}
	default:
		return Credentials{}, ErrNoCredentials
	}
	if creds.Model == "" {
		creds.Model = DefaultAnthropicModel
	}
	base, err := url.Parse(creds.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" {
		return Credentials{}, errors.New("saved base URL must be an absolute https URL")
	}
	return creds, nil
}

// LoadAnthropic returns the Anthropic credentials userID saved.
func LoadAnthropic(ctx context.Context, store Store, key []byte, userID string) (Credentials, error) {
	ciphertext, found, err := store.ProviderSecret(ctx, userID, secrets.KindAnthropic)
	if err != nil {
		return Credentials{}, fmt.Errorf("load secret: %w", err)
	}
	if !found {
		return Credentials{}, ErrNoCredentials
	}
	values, err := secrets.Decrypt(key, userID, secrets.KindAnthropic, ciphertext)
	if err != nil {
		return Credentials{}, err
	}
	return AnthropicCredentials(values)
}

type Relay struct {
	store       Store
	key         []byte
	workerToken string
	transport   http.RoundTripper
	now         func() time.Time

	mu       sync.Mutex
	inflight map[string]int // by sandbox ID
}

func New(store Store, key []byte, workerToken string) *Relay {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = responseHeaderTimeout
	return &Relay{store: store, key: key, workerToken: workerToken, transport: transport, now: time.Now, inflight: map[string]int{}}
}

func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	started := r.now()
	if r.workerToken == "" || !validBearer(req.Header.Get("Authorization"), r.workerToken) {
		writeError(w, http.StatusUnauthorized, "authentication_error", "worker authentication required")
		return
	}
	provider, apiPath, ok := strings.Cut(strings.TrimPrefix(req.URL.Path, plane.RouteModelRelay), "/")
	if !ok || req.Method != http.MethodPost || provider != secrets.KindAnthropic || !allowedPath(apiPath) ||
		(req.URL.RawQuery != "" && req.URL.RawQuery != "beta=true") {
		writeError(w, http.StatusNotFound, "not_found_error", "only Anthropic message requests are relayed")
		return
	}
	sandboxID := req.Header.Get(plane.SandboxIDHeader)
	sandbox, found, err := r.store.GetSandbox(req.Context(), sandboxID)
	if err != nil {
		slog.Error("model relay sandbox lookup failed", "sandbox_id", sandboxID, "err", err)
		writeError(w, http.StatusServiceUnavailable, "api_error", "sandbox lookup failed")
		return
	}
	if !found || sandbox.State != string(plane.StateActive) {
		writeError(w, http.StatusForbidden, "permission_error", "the sandbox is not active")
		return
	}
	creds, err := LoadAnthropic(req.Context(), r.store, r.key, sandbox.UserID)
	if err != nil {
		if !errors.Is(err, ErrNoCredentials) {
			slog.Error("model relay credentials unusable", "sandbox_id", sandboxID, "user_id", sandbox.UserID, "err", err)
		}
		writeError(w, http.StatusForbidden, "permission_error", "no usable Anthropic credentials are saved on the Secrets page")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxRequestBody))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
		return
	}
	var request struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "request body must be JSON")
		return
	}
	if request.Model != creds.Model {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			fmt.Sprintf("model %q is not allowed in this session; use %q", request.Model, creds.Model))
		return
	}
	if !r.acquire(sandboxID) {
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "too many concurrent model requests from this sandbox")
		return
	}
	defer r.release(sandboxID)

	ctx, cancel := context.WithTimeout(req.Context(), requestTimeout)
	defer cancel()
	target, _ := url.Parse(strings.TrimRight(creds.BaseURL, "/") + "/" + apiPath)
	target.RawQuery = req.URL.RawQuery
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			out.Out.URL = target
			out.Out.Host = target.Host
			header := make(http.Header)
			for _, name := range forwardedHeaders {
				if values := out.In.Header.Values(name); len(values) > 0 {
					header[name] = values
				}
			}
			header.Set("X-Api-Key", creds.APIKey)
			out.Out.Header = header
			out.Out.Body = io.NopCloser(bytes.NewReader(body))
			out.Out.ContentLength = int64(len(body))
		},
		Transport:     r.transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("model relay upstream failed", "sandbox_id", sandboxID, "err", err)
			writeError(w, http.StatusBadGateway, "api_error", "model provider unavailable")
		},
	}
	proxy.ServeHTTP(recorder, req.WithContext(ctx))
	slog.Info("model relay request", "sandbox_id", sandboxID, "user_id", sandbox.UserID, "model", creds.Model,
		"path", apiPath, "status", recorder.status, "duration_ms", r.now().Sub(started).Milliseconds())
}

func allowedPath(path string) bool {
	return path == "v1/messages" || path == "v1/messages/count_tokens"
}

func (r *Relay) acquire(sandboxID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inflight[sandboxID] >= maxConcurrentPerSbx {
		return false
	}
	r.inflight[sandboxID]++
	return true
}

func (r *Relay) release(sandboxID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inflight[sandboxID]--; r.inflight[sandboxID] <= 0 {
		delete(r.inflight, sandboxID)
	}
}

func validBearer(header, token string) bool {
	provided, ok := strings.CutPrefix(header, "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1
}

// writeError answers in the Anthropic API's error shape, so agents show it.
func writeError(w http.ResponseWriter, status int, kind, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]string{"type": kind, "message": message}})
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
