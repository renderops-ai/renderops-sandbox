package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"backend/internal/middleware"
	"backend/internal/modelrelay"
	"backend/internal/orchestrator"
	"backend/internal/plane"
)

// modelAccessTTL bounds one grant; the session runner grants again for longer runs.
const modelAccessTTL = 4 * time.Hour

type ModelAccessWorker interface {
	GrantModelAccess(ctx context.Context, endpoint, sandboxID string, req plane.ModelAccessRequest) (plane.ModelAccessResponse, error)
	RevokeModelAccess(ctx context.Context, endpoint, sandboxID string) error
}

type CreateModelAccessRequest struct {
	Provider string `json:"provider"`
}

// CreateModelAccessResponse tells the session runner how the sandbox's agent
// reaches its model. Env is ready to pass to the agent process; the token in it
// is a proxy token that works only from inside this sandbox, never a provider key.
type CreateModelAccessResponse struct {
	Status    string            `json:"status"`
	RequestID string            `json:"request_id"`
	Provider  string            `json:"provider"`
	Model     string            `json:"model"`
	BaseURL   string            `json:"base_url"`
	Token     string            `json:"token"`
	ExpiresAt time.Time         `json:"expires_at"`
	Env       map[string]string `json:"env"`
}

// CreateModelAccessHandler serves POST /v1/sandboxes/{sandboxID}/model-access:
// it checks the caller owns the active sandbox and saved credentials for the
// provider, then has the sandbox's worker open its model proxy.
func CreateModelAccessHandler(sandboxes plane.Service, placements TerminalPlacementResolver, workers ModelAccessWorker, store modelrelay.Store, secretsKey []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := middleware.RequestIDFromContext(r.Context())
		auth, ok := middleware.AuthFromContext(r.Context())
		if !ok {
			writeTerminalError(w, http.StatusUnauthorized, "unauthorized", "authentication is required", requestID)
			return
		}
		if len(secretsKey) == 0 {
			writeTerminalError(w, http.StatusServiceUnavailable, "model_access_unavailable", "model access is not configured on this server", requestID)
			return
		}
		request := CreateModelAccessRequest{Provider: "anthropic"}
		if r.Body != nil && r.ContentLength != 0 {
			if err := decodeJSON(w, r, &request); err != nil {
				writeTerminalError(w, http.StatusBadRequest, "invalid_request", "invalid model access request", requestID)
				return
			}
		}
		switch request.Provider {
		case "anthropic":
		case "openai":
			writeTerminalError(w, http.StatusBadRequest, "provider_unavailable", "GPT models are not available for agents yet", requestID)
			return
		default:
			writeTerminalError(w, http.StatusBadRequest, "invalid_provider", "provider must be anthropic", requestID)
			return
		}

		sandboxID := r.PathValue("sandboxID")
		sandbox, found := sandboxes.GetSession(r.Context(), sandboxID)
		if !found || sandbox.UserID != auth.TenantID {
			writeTerminalError(w, http.StatusNotFound, "sandbox_not_found", "sandbox not found", requestID)
			return
		}
		if sandbox.State != plane.StateActive {
			writeTerminalError(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to grant model access", requestID)
			return
		}
		creds, err := modelrelay.LoadAnthropic(r.Context(), store, secretsKey, auth.TenantID)
		if err != nil {
			if !errors.Is(err, modelrelay.ErrNoCredentials) {
				slog.Error("saved Anthropic credentials unusable", "request_id", requestID, "user_id", auth.TenantID, "err", err)
			}
			writeTerminalError(w, http.StatusConflict, "credentials_missing", "save your Anthropic credentials on the Secrets page first", requestID)
			return
		}
		placement, err := placements.Placement(r.Context(), sandboxID)
		if err != nil {
			status, code := http.StatusServiceUnavailable, "placement_unavailable"
			if errors.Is(err, orchestrator.ErrSandboxNotFound) {
				status, code = http.StatusConflict, "sandbox_not_placed"
			}
			writeTerminalError(w, status, code, "sandbox worker placement is unavailable", requestID)
			return
		}

		token, err := newProxyToken()
		if err != nil {
			writeTerminalError(w, http.StatusInternalServerError, "model_access_failed", "failed to create a proxy token", requestID)
			return
		}
		expiresAt := time.Now().Add(modelAccessTTL).UTC().Truncate(time.Second)
		workerCtx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		granted, err := workers.GrantModelAccess(workerCtx, placement.Endpoint, sandboxID, plane.ModelAccessRequest{
			Provider: request.Provider, Model: creds.Model, Token: token, ExpiresAt: expiresAt,
		})
		cancel()
		if err != nil {
			slog.Error("worker model access grant failed", "request_id", requestID, "sandbox_id", sandboxID,
				"worker_id", placement.WorkerID, "err", err)
			writeTerminalError(w, http.StatusBadGateway, "model_access_failed", "worker failed to open the model proxy", requestID)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(CreateModelAccessResponse{
			Status:    "success",
			RequestID: requestID,
			Provider:  request.Provider,
			Model:     creds.Model,
			BaseURL:   granted.BaseURL,
			Token:     token,
			ExpiresAt: expiresAt,
			Env:       claudeCodeEnv(granted.BaseURL, token, creds.Model),
		})
	}
}

// DeleteModelAccessHandler serves DELETE /v1/sandboxes/{sandboxID}/model-access.
func DeleteModelAccessHandler(sandboxes plane.Service, placements TerminalPlacementResolver, workers ModelAccessWorker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := middleware.RequestIDFromContext(r.Context())
		auth, ok := middleware.AuthFromContext(r.Context())
		if !ok {
			writeTerminalError(w, http.StatusUnauthorized, "unauthorized", "authentication is required", requestID)
			return
		}
		sandboxID := r.PathValue("sandboxID")
		sandbox, found := sandboxes.GetSession(r.Context(), sandboxID)
		if !found || sandbox.UserID != auth.TenantID {
			writeTerminalError(w, http.StatusNotFound, "sandbox_not_found", "sandbox not found", requestID)
			return
		}
		placement, err := placements.Placement(r.Context(), sandboxID)
		if err != nil {
			// Not placed means no worker holds a grant for it.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		workerCtx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if err := workers.RevokeModelAccess(workerCtx, placement.Endpoint, sandboxID); err != nil {
			slog.Error("worker model access revoke failed", "request_id", requestID, "sandbox_id", sandboxID, "err", err)
			writeTerminalError(w, http.StatusBadGateway, "model_access_failed", "worker failed to close the model proxy", requestID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// claudeCodeEnv points Claude Code at the sandbox's model proxy and pins every
// model alias to the granted model, so background calls (which default to a
// smaller model) are not refused by the relay.
func claudeCodeEnv(baseURL, token, model string) map[string]string {
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

func newProxyToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
