package worker

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"backend/internal/modelproxy"
	"backend/internal/plane"
)

type modelAccessProxy interface {
	Bind(sandboxID string, slot int, grant modelproxy.Grant) error
	Unbind(sandboxID string)
}

// SetModelProxy enables PUT/DELETE /worker/sandbox/{id}/model-access.
func (s *Server) SetModelProxy(proxy *modelproxy.Proxy) {
	s.model = proxy
}

func (s *Server) grantModelAccess(w http.ResponseWriter, r *http.Request, sandboxID string) {
	binder, ok := s.svc.(gitSlotBinder)
	if s.model == nil || !ok {
		writeErr(w, http.StatusNotImplemented, "model_proxy_unavailable", "worker model proxy is unavailable")
		return
	}
	var req plane.ModelAccessRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid model access request")
		return
	}
	grant := modelproxy.Grant{Provider: req.Provider, Model: req.Model, Token: req.Token, ExpiresAt: req.ExpiresAt}
	var bindErr error
	err := binder.BindSlotGrant(sandboxID, func(slot int) error {
		bindErr = s.model.Bind(sandboxID, slot, grant)
		return bindErr
	})
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, plane.ModelAccessResponse{BaseURL: modelproxy.BaseURL(), ExpiresAt: req.ExpiresAt})
	case errors.Is(err, modelproxy.ErrInvalidGrant):
		// Validation errors never include the token.
		writeErr(w, http.StatusBadRequest, "invalid_model_access", err.Error())
	case bindErr == nil && strings.Contains(err.Error(), "not found"):
		writeErr(w, http.StatusNotFound, "sandbox_not_found", "sandbox not found")
	case bindErr == nil:
		writeErr(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to grant model access")
	default:
		slog.Error("grant model access failed", "sandbox_id", sandboxID, "err", err)
		writeErr(w, http.StatusInternalServerError, "model_access_failed", "failed to open the model proxy for this sandbox")
	}
}

func (s *Server) revokeModelAccess(w http.ResponseWriter, sandboxID string) {
	if s.model != nil {
		s.model.Unbind(sandboxID)
	}
	w.WriteHeader(http.StatusNoContent)
}
