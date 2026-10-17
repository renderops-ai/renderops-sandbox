package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"backend/internal/plane"
)

// A lost sandbox's VM is gone (it crashed, e.g. out of memory, or its worker
// no longer has it). Commands on it get 410 sandbox_lost with the reason and a
// hint to remove it; DELETE still works.

// lostSandboxes reports lost sandboxes (controlplane.Service).
type lostSandboxes interface {
	LostSandbox(ctx context.Context, id string) (userID, reason string, lost bool)
}

// SandboxLostResponse is the 410 body for a command on a lost sandbox.
type SandboxLostResponse struct {
	Status    string `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Reason    string `json:"reason"`
	Hint      string `json:"hint"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

// lostOwnedSandbox reports whether sessionID is the tenant's lost sandbox,
// and why it was lost.
func lostOwnedSandbox(ctx context.Context, mgr plane.Service, sessionID, tenantID string) (string, bool) {
	lostReader, ok := mgr.(lostSandboxes)
	if !ok {
		return "", false
	}
	userID, reason, lost := lostReader.LostSandbox(ctx, sessionID)
	if !lost || userID != tenantID {
		return "", false
	}
	return reason, true
}

// ownsLiveSession is ownsSession for commands that need the sandbox's VM. It
// writes the response when the answer is no: 410 for the tenant's lost
// sandbox, otherwise 404.
func ownsLiveSession(w http.ResponseWriter, r *http.Request, mgr plane.Service, sessionID, tenantID, requestID string) bool {
	if ownsSession(r.Context(), mgr, sessionID, tenantID) {
		return true
	}
	if reason, lost := lostOwnedSandbox(r.Context(), mgr, sessionID, tenantID); lost {
		writeSandboxLost(w, requestID, sessionID, reason)
		return false
	}
	http.Error(w, "session not found", http.StatusNotFound)
	return false
}

// writeIfLost writes the 410 response when err says the sandbox is lost.
func writeIfLost(w http.ResponseWriter, requestID, sessionID string, err error) bool {
	var lost *plane.LostError
	if !errors.As(err, &lost) {
		return false
	}
	writeSandboxLost(w, requestID, sessionID, lost.Reason)
	return true
}

func writeSandboxLost(w http.ResponseWriter, requestID, sessionID, reason string) {
	if reason == "" {
		reason = "VM is gone"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusGone)
	if err := json.NewEncoder(w).Encode(SandboxLostResponse{
		Status:    "error",
		Code:      "sandbox_lost",
		Message:   "this sandbox's VM is gone and it cannot be resumed: " + reason,
		Reason:    reason,
		Hint:      "remove the sandbox (DELETE /session/" + sessionID + ") and create a new one",
		SessionID: sessionID,
		RequestID: requestID,
	}); err != nil {
		slog.Warn("write sandbox_lost response failed", "session_id", sessionID, "err", err)
	}
}
