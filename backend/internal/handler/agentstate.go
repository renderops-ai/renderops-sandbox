package handler

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
)

// agentStateTimeoutS bounds the guest command that reads the agent state.
const agentStateTimeoutS = 5

// AgentStateResponse is the body of GET /v1/sandboxes/{sandboxID}/agent-state.
// State is "working", "waiting", "done" or "idle" as the agent's hooks wrote
// it, or "unknown" (no state file, or an invalid one) with no other fields.
// AgeSeconds is how long ago the state was written, on the sandbox's own
// clock: compare it, not UpdatedAt, with the current time, because a resumed
// sandbox's clock can run behind.
type AgentStateResponse struct {
	State      string     `json:"state"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	AgeSeconds *int64     `json:"age_seconds,omitempty"`
}

// AgentStateHandler serves GET /v1/sandboxes/{sandboxID}/agent-state: what
// the sandbox's agent is doing, from the file its hooks write
// (plane.AgentStatePath). The caller must own the sandbox (404 otherwise) and
// it must be active (409; it is never resumed here). The read is a passive,
// quiet exec, so polling it neither keeps the sandbox awake nor reaches its
// logs.
func AgentStateHandler(sandboxes plane.Service, execer FileExecer) http.HandlerFunc {
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
		if sandbox.State != plane.StateActive {
			writeTerminalError(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to read its agent state", requestID)
			return
		}

		res, err := execer.ExecIfActive(r.Context(), sandboxID, plane.AgentStateCommand, agentStateTimeoutS,
			plane.ExecOptions{Passive: true, Quiet: true})
		if err != nil {
			switch {
			case errors.Is(err, plane.ErrSandboxNotActive):
				writeTerminalError(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active to read its agent state", requestID)
			case errors.Is(err, orchestrator.ErrSandboxNotFound):
				writeTerminalError(w, http.StatusConflict, "sandbox_not_placed", "sandbox worker placement is unavailable", requestID)
			default:
				slog.Error("agent state read failed", "request_id", requestID, "sandbox_id", sandboxID, "err", err)
				writeTerminalError(w, http.StatusBadGateway, "agent_state_failed", "the sandbox could not read its agent state", requestID)
			}
			return
		}

		response := AgentStateResponse{State: plane.AgentStateUnknown}
		state, err := plane.ParseAgentState(res.Stdout, time.Now())
		switch {
		case err == nil:
			age := int64(state.Age / time.Second)
			response = AgentStateResponse{State: state.State, UpdatedAt: &state.UpdatedAt, AgeSeconds: &age}
		case !errors.Is(err, plane.ErrAgentStateMissing):
			slog.Debug("agent state file is invalid", "request_id", requestID, "sandbox_id", sandboxID, "err", err)
		}
		writeFilesJSON(w, http.StatusOK, response)
	}
}
