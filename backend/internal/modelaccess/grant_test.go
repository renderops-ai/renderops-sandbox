package modelaccess

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
)

const (
	ownerID   = "user-1"
	sandboxID = "sbx-1"
)

var testToken = strings.Repeat("a", 64)

// seal encrypts like the platform does (see internal/secrets).
func seal(t *testing.T, key []byte, userID, kind string, values map[string]string) string {
	t.Helper()
	plain, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	return "v1:" + base64.StdEncoding.EncodeToString(append(nonce, gcm.Seal(nil, nonce, plain, []byte("secrets:"+userID+":"+kind))...))
}

type fakeSandboxes map[string]*plane.SessionInfo

func (f fakeSandboxes) GetSession(_ context.Context, id string) (*plane.SessionInfo, bool) {
	info, ok := f[id]
	return info, ok
}

type fakePlacements struct {
	err error
}

func (f fakePlacements) Placement(_ context.Context, id string) (orchestrator.Placement, error) {
	return orchestrator.Placement{SandboxID: id, WorkerID: "worker-1", Endpoint: "http://worker-1:19876", State: "active"}, f.err
}

type fakeWorkers struct {
	requests []plane.ModelAccessRequest
	err      error
}

func (f *fakeWorkers) GrantModelAccess(_ context.Context, endpoint, id string, req plane.ModelAccessRequest) (plane.ModelAccessResponse, error) {
	f.requests = append(f.requests, req)
	return plane.ModelAccessResponse{BaseURL: "http://172.16.0.1:7081", ExpiresAt: req.ExpiresAt}, f.err
}

type fakeStore struct{ secret string }

func (f fakeStore) GetSandbox(context.Context, string) (platform.SandboxRef, bool, error) {
	return platform.SandboxRef{}, false, nil
}

func (f fakeStore) ProviderSecret(_ context.Context, userID, kind string) (string, bool, error) {
	return f.secret, f.secret != "" && userID == ownerID && kind == "anthropic", nil
}

func newGranter(t *testing.T, state plane.SessionState) (*Granter, *fakeWorkers) {
	t.Helper()
	key := make([]byte, 32)
	workers := &fakeWorkers{}
	now := time.Date(2026, 10, 16, 4, 0, 0, 0, time.UTC)
	return &Granter{
		Sandboxes:  fakeSandboxes{sandboxID: {ID: sandboxID, UserID: ownerID, State: state}},
		Placements: fakePlacements{},
		Workers:    workers,
		Store:      fakeStore{secret: seal(t, key, ownerID, "anthropic", map[string]string{"ANTHROPIC_API_KEY": "sk-test", "ANTHROPIC_MODEL": "claude-opus-5-5"})},
		SecretsKey: key,
		Now:        func() time.Time { return now },
	}, workers
}

func TestGrantUsesTheCallersTokenAndPinsTheModel(t *testing.T) {
	g, workers := newGranter(t, plane.StateActive)
	grant, err := g.Grant(context.Background(), ownerID, sandboxID, "anthropic", testToken)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if len(workers.requests) != 1 || workers.requests[0].Token != testToken || workers.requests[0].Model != "claude-opus-5-5" {
		t.Fatalf("worker requests = %+v", workers.requests)
	}
	if want := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC); !grant.ExpiresAt.Equal(want) || !workers.requests[0].ExpiresAt.Equal(want) {
		t.Fatalf("expiry = %s, want %s", grant.ExpiresAt, want)
	}
	if grant.Token != testToken || grant.BaseURL != "http://172.16.0.1:7081" || grant.Model != "claude-opus-5-5" {
		t.Fatalf("grant = %+v", grant)
	}
	if grant.Env["ANTHROPIC_AUTH_TOKEN"] != testToken || grant.Env["ANTHROPIC_SMALL_FAST_MODEL"] != "claude-opus-5-5" ||
		grant.Env["ANTHROPIC_BASE_URL"] != "http://172.16.0.1:7081" {
		t.Fatalf("env = %v", grant.Env)
	}
	for k, v := range grant.Env {
		if strings.Contains(v, "sk-test") {
			t.Fatalf("provider key leaked into %s", k)
		}
	}

	// Granting the same token again (after a resume) is allowed.
	if _, err := g.Grant(context.Background(), ownerID, sandboxID, "anthropic", testToken); err != nil {
		t.Fatalf("second Grant: %v", err)
	}
	if len(workers.requests) != 2 || workers.requests[1].Token != testToken {
		t.Fatalf("re-grant requests = %+v", workers.requests)
	}
}

func TestGrantRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Granter)
		user   string
		prov   string
		token  string
		want   error
	}{
		{"no secrets key", func(g *Granter) { g.SecretsKey = nil }, ownerID, "anthropic", testToken, ErrUnavailable},
		{"openai", nil, ownerID, "openai", testToken, ErrProviderNotReady},
		{"unknown provider", nil, ownerID, "gemini", testToken, ErrInvalidProvider},
		{"short token", nil, ownerID, "anthropic", "short", ErrInvalidToken},
		{"token with space", nil, ownerID, "anthropic", strings.Repeat("a", 40) + " x", ErrInvalidToken},
		{"other owner", nil, "user-2", "anthropic", testToken, ErrSandboxNotFound},
		{"paused", func(g *Granter) {
			g.Sandboxes = fakeSandboxes{sandboxID: {ID: sandboxID, UserID: ownerID, State: plane.StatePaused}}
		}, ownerID, "anthropic", testToken, ErrSandboxNotActive},
		{"no credentials", func(g *Granter) { g.Store = fakeStore{} }, ownerID, "anthropic", testToken, ErrCredentialsMissing},
		{"not placed", func(g *Granter) { g.Placements = fakePlacements{err: orchestrator.ErrSandboxNotFound} }, ownerID, "anthropic", testToken, ErrNotPlaced},
		{"placement down", func(g *Granter) { g.Placements = fakePlacements{err: errors.New("orchestrator unreachable")} }, ownerID, "anthropic", testToken, ErrPlacement},
		{"worker fails", func(g *Granter) { g.Workers = &fakeWorkers{err: errors.New("boom")} }, ownerID, "anthropic", testToken, ErrWorker},
	}
	for _, c := range cases {
		g, _ := newGranter(t, plane.StateActive)
		if c.mutate != nil {
			c.mutate(g)
		}
		_, err := g.Grant(context.Background(), c.user, sandboxID, c.prov, c.token)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: error = %v, want %v", c.name, err, c.want)
		}
	}
}

func TestNewTokenIsRandomHex(t *testing.T) {
	a, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 64 || a == b || strings.Trim(a, "0123456789abcdef") != "" {
		t.Fatalf("tokens %q %q", a, b)
	}
}
