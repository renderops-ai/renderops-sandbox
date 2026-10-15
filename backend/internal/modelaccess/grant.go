// Package modelaccess grants a sandbox's agent access to its owner's model
// through the worker's model proxy (internal/modelproxy) and the control
// plane's relay (internal/modelrelay). The sandbox only ever sees a proxy
// token, never a provider key.
//
// Grant is shared by the public POST /v1/sandboxes/{id}/model-access handler,
// which mints a fresh token per call, and the agent-session runner, which
// grants the same token again after a resume (pausing a sandbox revokes its
// grants) and before the TTL runs out.
package modelaccess

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"backend/internal/modelrelay"
	"backend/internal/orchestrator"
	"backend/internal/plane"
)

// TTL bounds one grant; long-running callers grant again before it ends.
const TTL = 4 * time.Hour

var (
	ErrUnavailable        = errors.New("model access is not configured on this server")
	ErrInvalidProvider    = errors.New("provider must be anthropic")
	ErrProviderNotReady   = errors.New("GPT models are not available for agents yet")
	ErrInvalidToken       = errors.New("proxy token must be at least 32 characters without whitespace")
	ErrSandboxNotFound    = errors.New("sandbox not found")
	ErrSandboxNotActive   = errors.New("sandbox must be active to grant model access")
	ErrCredentialsMissing = errors.New("no usable Anthropic credentials are saved on the Secrets page")
	ErrNotPlaced          = errors.New("sandbox is not placed on a worker")
	ErrPlacement          = errors.New("sandbox worker placement is unavailable")
	ErrWorker             = errors.New("worker failed to open the model proxy")
)

// Sandboxes reads sandbox ownership and state.
type Sandboxes interface {
	GetSession(ctx context.Context, id string) (*plane.SessionInfo, bool)
}

// Placements resolves the worker that runs a sandbox.
type Placements interface {
	Placement(ctx context.Context, sandboxID string) (orchestrator.Placement, error)
}

// Workers opens a sandbox's model proxy on its worker.
type Workers interface {
	GrantModelAccess(ctx context.Context, endpoint, sandboxID string, req plane.ModelAccessRequest) (plane.ModelAccessResponse, error)
}

// Granter checks ownership and saved credentials, then has the sandbox's
// worker open its model proxy for one proxy token.
type Granter struct {
	Sandboxes  Sandboxes
	Placements Placements
	Workers    Workers
	Store      modelrelay.Store
	// SecretsKey decrypts saved provider credentials. Empty disables Grant.
	SecretsKey []byte
	// Now defaults to time.Now.
	Now func() time.Time
}

// Grant is what the sandbox's agent needs to reach its model.
type Grant struct {
	Provider  string
	Model     string
	BaseURL   string
	Token     string
	ExpiresAt time.Time
	// Env is ready to pass to Claude Code (see ClaudeCodeEnv).
	Env map[string]string
}

// Grant opens model access for sandboxID, which userID must own and which
// must be active, using token as the proxy token. Granting the same token
// again replaces the previous grant (and extends its expiry).
func (g *Granter) Grant(ctx context.Context, userID, sandboxID, provider, token string) (Grant, error) {
	if len(g.SecretsKey) == 0 {
		return Grant{}, ErrUnavailable
	}
	switch provider {
	case "anthropic":
	case "openai":
		return Grant{}, ErrProviderNotReady
	default:
		return Grant{}, ErrInvalidProvider
	}
	if len(token) < 32 || strings.ContainsAny(token, " \t\r\n") {
		return Grant{}, ErrInvalidToken
	}
	sandbox, found := g.Sandboxes.GetSession(ctx, sandboxID)
	if !found || sandbox.UserID != userID {
		return Grant{}, ErrSandboxNotFound
	}
	if sandbox.State != plane.StateActive {
		return Grant{}, ErrSandboxNotActive
	}
	creds, err := modelrelay.LoadAnthropic(ctx, g.Store, g.SecretsKey, userID)
	if err != nil {
		if !errors.Is(err, modelrelay.ErrNoCredentials) {
			slog.Error("saved Anthropic credentials unusable", "component", "modelaccess", "user_id", userID, "err", err)
		}
		return Grant{}, ErrCredentialsMissing
	}
	placement, err := g.Placements.Placement(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, orchestrator.ErrSandboxNotFound) {
			return Grant{}, fmt.Errorf("%w: %w", ErrNotPlaced, err)
		}
		return Grant{}, fmt.Errorf("%w: %w", ErrPlacement, err)
	}

	now := time.Now
	if g.Now != nil {
		now = g.Now
	}
	expiresAt := now().Add(TTL).UTC().Truncate(time.Second)
	workerCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	granted, err := g.Workers.GrantModelAccess(workerCtx, placement.Endpoint, sandboxID, plane.ModelAccessRequest{
		Provider: provider, Model: creds.Model, Token: token, ExpiresAt: expiresAt,
	})
	if err != nil {
		slog.Error("worker model access grant failed", "component", "modelaccess", "sandbox_id", sandboxID,
			"worker_id", placement.WorkerID, "err", err)
		return Grant{}, fmt.Errorf("%w: %w", ErrWorker, err)
	}
	return Grant{
		Provider:  provider,
		Model:     creds.Model,
		BaseURL:   granted.BaseURL,
		Token:     token,
		ExpiresAt: expiresAt,
		Env:       ClaudeCodeEnv(granted.BaseURL, token, creds.Model),
	}, nil
}

// ClaudeCodeEnv points Claude Code at the sandbox's model proxy and pins every
// model alias to the granted model, so background calls (which default to a
// smaller model) are not refused by the relay.
func ClaudeCodeEnv(baseURL, token, model string) map[string]string {
	return map[string]string{
		"ANTHROPIC_BASE_URL":                       baseURL,
		"ANTHROPIC_AUTH_TOKEN":                     token,
		"ANTHROPIC_MODEL":                          model,
		"ANTHROPIC_DEFAULT_OPUS_MODEL":             model,
		"ANTHROPIC_DEFAULT_SONNET_MODEL":           model,
		"ANTHROPIC_DEFAULT_HAIKU_MODEL":            model,
		"ANTHROPIC_SMALL_FAST_MODEL":               model,
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
	}
}

// NewToken returns a random proxy token.
func NewToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("read random proxy token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
