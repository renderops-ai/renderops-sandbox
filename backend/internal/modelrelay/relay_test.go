package modelrelay

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"backend/internal/plane"
	"backend/internal/platform"
)

const (
	workerToken = "worker-token"
	sandboxID   = "sbx-1"
	ownerID     = "user-1"
)

// seal encrypts like the platform does (see internal/secrets).
func seal(t *testing.T, key []byte, userID, kind string, values map[string]string) string {
	t.Helper()
	plain, _ := json.Marshal(values)
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	return "v1:" + base64.StdEncoding.EncodeToString(append(nonce, gcm.Seal(nil, nonce, plain, []byte("secrets:"+userID+":"+kind))...))
}

type fakeStore struct {
	sandbox platform.SandboxRef
	found   bool
	secret  string
}

func (f fakeStore) GetSandbox(_ context.Context, id string) (platform.SandboxRef, bool, error) {
	return f.sandbox, f.found && id == f.sandbox.ID, nil
}

func (f fakeStore) ProviderSecret(_ context.Context, userID, kind string) (string, bool, error) {
	return f.secret, f.secret != "" && userID == ownerID && kind == "anthropic", nil
}

type setup struct {
	relay    *Relay
	upstream *httptest.Server
	seen     *http.Request
	seenBody string
}

func newSetup(t *testing.T, state string, values map[string]string) *setup {
	t.Helper()
	s := &setup{}
	s.upstream = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.seen = r.Clone(context.Background())
		body, _ := io.ReadAll(r.Body)
		s.seenBody = string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {}\n\n")
	}))
	t.Cleanup(s.upstream.Close)
	key := make([]byte, 32)
	store := fakeStore{sandbox: platform.SandboxRef{ID: sandboxID, State: state, UserID: ownerID}, found: true}
	if values != nil {
		for k, v := range values {
			values[k] = strings.ReplaceAll(v, "UPSTREAM", s.upstream.URL)
		}
		store.secret = seal(t, key, ownerID, "anthropic", values)
	}
	s.relay = New(store, key, workerToken)
	s.relay.transport = s.upstream.Client().Transport
	return s
}

func foundry() map[string]string {
	return map[string]string{
		"ANTHROPIC_FOUNDRY_BASE_URL": "UPSTREAM/anthropic",
		"ANTHROPIC_FOUNDRY_API_KEY":  "real-provider-key",
		"ANTHROPIC_MODEL":            "claude-opus-5-5",
	}
}

func relayRequest(token, path, model string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, plane.RouteModelRelay+path, strings.NewReader(`{"model":"`+model+`","max_tokens":5}`))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set(plane.SandboxIDHeader, sandboxID)
	r.Header.Set("Anthropic-Version", "2023-06-01")
	r.Header.Set("X-Api-Key", "guest-supplied")
	r.Header.Set("Cookie", "guest=1")
	return r
}

func TestRelayForwardsWithTheSavedKeyOnly(t *testing.T) {
	s := newSetup(t, "active", foundry())
	w := httptest.NewRecorder()
	s.relay.ServeHTTP(w, relayRequest(workerToken, "anthropic/v1/messages", "claude-opus-5-5"))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "message_start") {
		t.Fatalf("status %d body %q", w.Code, w.Body.String())
	}
	if s.seen == nil || s.seen.URL.Path != "/anthropic/v1/messages" {
		t.Fatalf("upstream path = %v", s.seen)
	}
	if got := s.seen.Header.Get("X-Api-Key"); got != "real-provider-key" {
		t.Errorf("upstream x-api-key = %q", got)
	}
	for _, leaked := range []string{"Authorization", plane.SandboxIDHeader, "Cookie"} {
		if s.seen.Header.Get(leaked) != "" {
			t.Errorf("%s reached the provider", leaked)
		}
	}
	if s.seen.Header.Get("Anthropic-Version") != "2023-06-01" || !strings.Contains(s.seenBody, `"max_tokens":5`) {
		t.Errorf("headers or body not forwarded: %v %q", s.seen.Header, s.seenBody)
	}
}

func TestRelayRefuses(t *testing.T) {
	cases := []struct {
		name   string
		state  string
		values map[string]string
		req    *http.Request
		status int
	}{
		{"no worker token", "active", foundry(), relayRequest("", "anthropic/v1/messages", "claude-opus-5-5"), http.StatusUnauthorized},
		{"wrong worker token", "active", foundry(), relayRequest("nope", "anthropic/v1/messages", "claude-opus-5-5"), http.StatusUnauthorized},
		{"paused sandbox", "paused", foundry(), relayRequest(workerToken, "anthropic/v1/messages", "claude-opus-5-5"), http.StatusForbidden},
		{"no saved secret", "active", nil, relayRequest(workerToken, "anthropic/v1/messages", "claude-opus-5-5"), http.StatusForbidden},
		{"another model", "active", foundry(), relayRequest(workerToken, "anthropic/v1/messages", "claude-haiku-5-5"), http.StatusBadRequest},
		{"another path", "active", foundry(), relayRequest(workerToken, "anthropic/v1/models", "claude-opus-5-5"), http.StatusNotFound},
		{"openai", "active", foundry(), relayRequest(workerToken, "openai/v1/chat/completions", "claude-opus-5-5"), http.StatusNotFound},
		{"other query", "active", foundry(), relayRequest(workerToken, "anthropic/v1/messages?x=1", "claude-opus-5-5"), http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSetup(t, c.state, c.values)
			w := httptest.NewRecorder()
			s.relay.ServeHTTP(w, c.req)
			if w.Code != c.status {
				t.Fatalf("status = %d, want %d (%s)", w.Code, c.status, w.Body.String())
			}
			if s.seen != nil {
				t.Fatal("a refused request reached the provider")
			}
			var body struct {
				Type  string `json:"type"`
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Type != "error" || body.Error.Message == "" {
				t.Fatalf("not an Anthropic-style error: %s", w.Body.String())
			}
		})
	}
}

func TestAnthropicCredentials(t *testing.T) {
	direct, err := AnthropicCredentials(map[string]string{"ANTHROPIC_API_KEY": "k"})
	if err != nil || direct.BaseURL != "https://api.anthropic.com" || direct.Model != DefaultAnthropicModel {
		t.Fatalf("direct = %+v, %v", direct, err)
	}
	if _, err := AnthropicCredentials(map[string]string{"ANTHROPIC_FOUNDRY_API_KEY": "k", "ANTHROPIC_FOUNDRY_BASE_URL": "http://insecure"}); err == nil {
		t.Fatal("accepted a non-https base URL")
	}
	if _, err := AnthropicCredentials(map[string]string{"OPENAI_API_KEY": "k"}); err != ErrNoCredentials {
		t.Fatalf("err = %v, want ErrNoCredentials", err)
	}
}
