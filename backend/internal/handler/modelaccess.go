package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"backend/internal/middleware"
	"backend/internal/modelaccess"
	"backend/internal/modelrelay"
	"backend/internal/plane"
)

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
// provider, then has the sandbox's worker open its model proxy
// (modelaccess.Granter) for a fresh proxy token.
func CreateModelAccessHandler(sandboxes plane.Service, placements TerminalPlacementResolver, workers ModelAccessWorker, store modelrelay.Store, secretsKey []byte) http.HandlerFunc {
	granter := &modelaccess.Granter{Sandboxes: sandboxes, Placements: placements, Workers: workers, Store: store, SecretsKey: secretsKey}
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

		token, err := modelaccess.NewToken()
		if err != nil {
			writeTerminalError(w, http.StatusInternalServerError, "model_access_failed", "failed to create a proxy token", requestID)
			return
		}
		sandboxID := r.PathValue("sandboxID")
		grant, err := granter.Grant(r.Context(), auth.TenantID, sandboxID, request.Provider, token)
		if err != nil {
			status, code, message := modelAccessError(err)
			slog.Warn("model access grant refused", "request_id", requestID, "sandbox_id", sandboxID, "code", code, "err", err)
			writeTerminalError(w, status, code, message, requestID)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(CreateModelAccessResponse{
			Status:    "success",
			RequestID: requestID,
			Provider:  grant.Provider,
			Model:     grant.Model,
			BaseURL:   grant.BaseURL,
			Token:     grant.Token,
			ExpiresAt: grant.ExpiresAt,
			Env:       grant.Env,
		})
	}
}

// modelAccessError maps a modelaccess.Granter error to the HTTP status, code
// and message this endpoint returned before the grant core was extracted.
func modelAccessError(err error) (int, string, string) {
	switch {
	case errors.Is(err, modelaccess.ErrUnavailable):
		return http.StatusServiceUnavailable, "model_access_unavailable", "model access is not configured on this server"
	case errors.Is(err, modelaccess.ErrProviderNotReady):
		return http.StatusBadRequest, "provider_unavailable", "GPT models are not available for agents yet"
	case errors.Is(err, modelaccess.ErrInvalidProvider):
		return http.StatusBadRequest, "invalid_provider", "provider must be anthropic"
	case errors.Is(err, modelaccess.ErrSandboxNotFound):
		return http.StatusNotFound, "sandbox_not_found", "sandbox not found"
	case errors.Is(err, modelaccess.ErrSandboxNotActive):
		return http.StatusConflict, "sandbox_not_active", "sandbox must be active to grant model access"
	case errors.Is(err, modelaccess.ErrCredentialsMissing):
		return http.StatusConflict, "credentials_missing", "save your Anthropic credentials on the Secrets page first"
	case errors.Is(err, modelaccess.ErrNotPlaced):
		return http.StatusConflict, "sandbox_not_placed", "sandbox worker placement is unavailable"
	case errors.Is(err, modelaccess.ErrPlacement):
		return http.StatusServiceUnavailable, "placement_unavailable", "sandbox worker placement is unavailable"
	case errors.Is(err, modelaccess.ErrWorker):
		return http.StatusBadGateway, "model_access_failed", "worker failed to open the model proxy"
	default:
		return http.StatusInternalServerError, "model_access_failed", "failed to grant model access"
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
