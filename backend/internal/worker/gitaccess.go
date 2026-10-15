package worker

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"backend/internal/gitproxy"
	"backend/internal/plane"
)

type gitAccessProxy interface {
	Bind(sandboxID string, slot int, grant gitproxy.Grant) error
	Unbind(sandboxID string)
}

// gitSlotBinder runs a bind against a sandbox's live network slot without
// racing pause or destroy (see session.Manager.BindSlotGrant).
type gitSlotBinder interface {
	BindSlotGrant(id string, bind func(slot int) error) error
}

// SetGitProxy enables PUT/DELETE /worker/sandbox/{id}/git-access.
func (s *Server) SetGitProxy(proxy *gitproxy.Proxy) {
	s.git = proxy
}

func (s *Server) grantGitAccess(w http.ResponseWriter, r *http.Request, sandboxID string) {
	binder, ok := s.svc.(gitSlotBinder)
	if s.git == nil || !ok {
		writeErr(w, http.StatusNotImplemented, "git_proxy_unavailable", "worker git proxy is unavailable")
		return
	}
	var req plane.GitAccessRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid git access request")
		return
	}
	grant := gitproxy.Grant{Repository: req.Repository, Token: req.Token, ExpiresAt: req.ExpiresAt}
	var bindErr error
	err := binder.BindSlotGrant(sandboxID, func(slot int) error {
		bindErr = s.git.Bind(sandboxID, slot, grant)
		return bindErr
	})
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, plane.GitAccessResponse{
			RemoteURL: gitproxy.RemoteURL(req.Repository), ExpiresAt: req.ExpiresAt,
		})
	case errors.Is(err, gitproxy.ErrInvalidGrant):
		// Validation errors never include the token.
		writeErr(w, http.StatusBadRequest, "invalid_git_access", err.Error())
	case bindErr == nil && strings.Contains(err.Error(), "not found"):
		writeErr(w, http.StatusNotFound, "sandbox_not_found", "sandbox not found")
	case bindErr == nil:
		writeErr(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to grant git access")
	default:
		slog.Error("grant git access failed", "sandbox_id", sandboxID, "err", err)
		writeErr(w, http.StatusInternalServerError, "git_access_failed", "failed to open the git proxy for this sandbox")
	}
}

func (s *Server) revokeGitAccess(w http.ResponseWriter, sandboxID string) {
	if s.git != nil {
		s.git.Unbind(sandboxID)
	}
	w.WriteHeader(http.StatusNoContent)
}
