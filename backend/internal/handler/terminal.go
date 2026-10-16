package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/terminal"
)

type TerminalPlacementResolver interface {
	Placement(context.Context, string) (orchestrator.Placement, error)
}

type TerminalWorker interface {
	OpenTerminal(ctx context.Context, endpoint, sandboxID, terminalID, shell string, columns, rows uint32, start terminal.Command) error
	CloseTerminal(context.Context, string, string, string) error
}

// CreateTerminalRequest is the body of POST /v1/sandboxes/{id}/terminals.
// Command, Cwd and EnvFile are optional (terminal.Command): the terminal
// starts in Cwd, loads EnvFile and runs Command instead of a prompt.
type CreateTerminalRequest struct {
	Shell   string   `json:"shell,omitempty"`
	Columns uint32   `json:"columns,omitempty"`
	Rows    uint32   `json:"rows,omitempty"`
	Command []string `json:"command,omitempty"`
	Cwd     string   `json:"cwd,omitempty"`
	EnvFile string   `json:"env_file,omitempty"`
}

type CreateTerminalResponse struct {
	Status          string           `json:"status"`
	RequestID       string           `json:"request_id"`
	Terminal        terminal.Session `json:"terminal"`
	WebSocketPath   string           `json:"websocket_path"`
	AttachmentToken string           `json:"attachment_token"`
	ExpiresIn       int              `json:"expires_in"`
}

// CreateTerminalHandler returns an attachment token only after the assigned
// worker confirms that the guest PTY exists.
func CreateTerminalHandler(sandboxes plane.Service, placements TerminalPlacementResolver, workers TerminalWorker, terminals *terminal.Manager) http.HandlerFunc {
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
			writeTerminalError(w, http.StatusConflict, "sandbox_not_active", "sandbox must be active before opening a terminal", requestID)
			return
		}

		request := CreateTerminalRequest{Shell: "/bin/bash", Columns: 120, Rows: 32}
		if r.Body != nil && r.ContentLength != 0 {
			if err := decodeJSON(w, r, &request); err != nil {
				writeTerminalError(w, http.StatusBadRequest, "invalid_terminal_options", "invalid terminal options", requestID)
				return
			}
		}
		if request.Shell == "" {
			request.Shell = "/bin/bash"
		}
		if request.Columns == 0 {
			request.Columns = 120
		}
		if request.Rows == 0 {
			request.Rows = 32
		}
		if request.Shell != "/bin/bash" || request.Columns < 20 || request.Columns > 500 || request.Rows < 5 || request.Rows > 200 {
			writeTerminalError(w, http.StatusBadRequest, "invalid_terminal_options", "unsupported shell or terminal size", requestID)
			return
		}
		start := terminal.Command{Argv: request.Command, Cwd: request.Cwd, EnvFile: request.EnvFile}
		if request.Command != nil && len(request.Command) == 0 {
			writeTerminalError(w, http.StatusBadRequest, "invalid_terminal_options", "command must not be empty; omit it for a shell", requestID)
			return
		}
		if err := start.Validate(); err != nil {
			writeTerminalError(w, http.StatusBadRequest, "invalid_terminal_options", err.Error(), requestID)
			return
		}
		placement, err := placements.Placement(r.Context(), sandboxID)
		if err != nil {
			status := http.StatusServiceUnavailable
			code := "placement_unavailable"
			if errors.Is(err, orchestrator.ErrSandboxNotFound) {
				status = http.StatusConflict
				code = "sandbox_not_placed"
			}
			writeTerminalError(w, status, code, "sandbox worker placement is unavailable", requestID)
			return
		}

		session, err := terminals.Reserve(sandboxID, auth.TenantID, placement.WorkerID)
		if err != nil {
			writeTerminalError(w, http.StatusInternalServerError, "terminal_authorization_failed", "failed to authorize terminal", requestID)
			return
		}
		terminalID := session.ID
		workerCtx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		err = workers.OpenTerminal(workerCtx, placement.Endpoint, sandboxID, terminalID, request.Shell, request.Columns, request.Rows, start)
		cancel()
		if err != nil {
			slog.Error("worker terminal creation failed",
				"request_id", requestID,
				"sandbox_id", sandboxID,
				"terminal_id", terminalID,
				"worker_id", placement.WorkerID,
				"err", err,
			)
			terminals.Cancel(terminalID)
			if errors.Is(err, terminal.ErrCommandUnsupported) {
				writeTerminalError(w, http.StatusBadGateway, "terminal_command_unsupported", "the sandbox's worker cannot start terminal commands yet", requestID)
				return
			}
			writeTerminalError(w, http.StatusBadGateway, "terminal_creation_failed", "worker failed to create terminal", requestID)
			return
		}
		session, token, err := terminals.Authorize(terminalID)
		if err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = workers.CloseTerminal(cleanupCtx, placement.Endpoint, sandboxID, terminalID)
			cleanupCancel()
			terminals.Cancel(terminalID)
			writeTerminalError(w, http.StatusInternalServerError, "terminal_authorization_failed", "failed to authorize terminal", requestID)
			return
		}

		response := CreateTerminalResponse{
			Status:          "success",
			RequestID:       requestID,
			Terminal:        session,
			WebSocketPath:   "/v1/terminals/" + session.ID,
			AttachmentToken: token,
			ExpiresIn:       max(0, int(time.Until(session.ExpiresAt).Seconds())),
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}
}

func writeTerminalError(w http.ResponseWriter, status int, code, message, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIError{
		Status:    "error",
		Code:      code,
		Message:   message,
		RequestID: requestID,
	})
}
