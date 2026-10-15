package handler

import (
	"context"
	"errors"

	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/sandboximage"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SessionHandler routes all /session and /session/:id/* requests
//
//	POST   /session          → create session
//	POST   /session/:id/run  → run code in session
//	DELETE /session/:id      → destroy session
//	GET    /session/:id      → session info
func SessionHandler(mgr plane.Service, usageLogger platform.UsageLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := middleware.RequestIDFromContext(r.Context())

		auth, ok := middleware.AuthFromContext(r.Context())
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		tc := auth.Config

		// strip leading /session prefix, trim slashes
		// e.g. /session          → ""
		//      /session/abc/run  → "abc/run"
		//      /session/abc      → "abc"
		path := strings.TrimPrefix(r.URL.Path, "/session")
		path = strings.Trim(path, "/")

		switch {

		// POST /session — create new session
		case r.Method == http.MethodPost && path == "":
			createStartedAt := time.Now()
			if tc.MaxSessions == 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				json.NewEncoder(w).Encode(APIError{
					Status:    "error",
					Code:      "sessions_not_allowed",
					Message:   "sessions are disabled by the current execution policy",
					RequestID: requestID,
				})
				return
			}

			var createReq CreateSessionRequest
			if r.Body != nil && r.ContentLength != 0 {
				if err := decodeJSON(w, r, &createReq); err != nil {
					http.Error(w, "invalid request body", http.StatusBadRequest)
					return
				}
			}
			image, err := sandboximage.Resolve(createReq.OS, createReq.Template, createReq.Image)
			if err != nil {
				code := "invalid_image"
				if createReq.OS != "" || createReq.Template != "" {
					code = "invalid_template"
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(APIError{Status: "error", Code: code, Message: err.Error(), RequestID: requestID})
				return
			}

			size, err := resolveSize(createReq)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(APIError{
					Status:    "error",
					Code:      "invalid_resources",
					Message:   err.Error(),
					RequestID: requestID,
				})
				return
			}
			if !sandboximage.SizeAllowed(image, size.Name) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(APIError{
					Status:    "error",
					Code:      "size_not_available",
					Message:   sizeNotAvailableMessage(image, size.Name),
					RequestID: requestID,
				})
				return
			}

			internet := true // default: egress on
			if createReq.Network != nil && createReq.Network.Internet != nil {
				internet = *createReq.Network.Internet
			}

			// Resolve per-session idle/lifetime: caller may request values; idle is capped
			// at the max lifetime, lifetime is capped at the policy ceiling.
			idleTimeout := resolveDuration(createReq.IdleTimeoutS, tc.SessionIdleTimeout, tc.SessionMaxLifetime)
			maxLifetime := resolveDuration(createReq.MaxLifetimeS, tc.SessionMaxLifetime, tc.SessionMaxLifetime)

			sess, err := mgr.Create(r.Context(), auth.TenantID, auth.Billing.Model, image, createReq.Env, size.VCPUs, size.MemoryMB, size.DiskGB, internet, idleTimeout, maxLifetime)
			if err != nil {
				slog.Error("failed to create session", "err", err)
				status := http.StatusInternalServerError
				code := "provision_failed"
				if errors.Is(err, orchestrator.ErrNoCapacity) {
					status = http.StatusServiceUnavailable
					code = "no_capacity"
				} else if errors.Is(err, orchestrator.ErrPlacementBusy) {
					status = http.StatusServiceUnavailable
					code = "scheduler_busy"
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				json.NewEncoder(w).Encode(APIError{
					Status:    "error",
					Code:      code,
					Message:   err.Error(),
					RequestID: requestID,
				})
				return
			}

			// Capture monotonic elapsed time at VM-running acknowledgement, not
			// after metadata writes or asynchronous audit delivery. This is a
			// successful create latency, not application readiness or resume.
			startupDurationMs := time.Since(createStartedAt).Milliseconds()

			// The control plane already owns the durable lifecycle row. Enrich
			// dashboard fields without allowing this write to overwrite state.
			name := createReq.Name
			if name == "" {
				name = "sandbox"
			}
			if err := usageLogger.UpdateSandboxDetails(r.Context(), platform.Sandbox{
				ID:       sess.ID,
				APIKeyID: auth.APIKeyID,
				Name:     name,
				Metadata: createReq.Metadata,
			}); err != nil {
				slog.Warn("sandbox details update failed", "sandbox_id", sess.ID, "err", err)
			}
			recordAuditEventAsync(
				usageLogger,
				r,
				auth.TenantID,
				auth.APIKeyID,
				"sandbox.created",
				"sandbox",
				sess.ID,
				map[string]any{
					"name": name, "vcpus": size.VCPUs, "memory_mb": size.MemoryMB,
					"disk_gb": size.DiskGB, "image": image, "billing_model": auth.Billing.Model,
					"startup_duration_ms": startupDurationMs,
				},
			)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(CreateSessionResponse{
				Status:    "success",
				RequestID: requestID,
				Session: &SessionDetail{
					SessionID:    sess.ID,
					Image:        sess.Image,
					OS:           osOf(sess.Image),
					Template:     templateOf(sess.Image),
					State:        "active",
					BillingModel: sess.BillingModel,
					CreatedAt:    sess.CreatedAt.Format(time.RFC3339),
					LastUsed:     sess.LastUsed.Format(time.RFC3339),
					ExpiresAt:    sess.LastUsed.Add(sess.IdleTimeout).Format(time.RFC3339),
				},
				Limits: &SessionLimits{
					MaxSessions:    tc.MaxSessions,
					ActiveSessions: 0, // no per-tenant count yet
					MaxExecutionMs: int(tc.MaxExecTimeout.Milliseconds()),
					IdleTimeoutMs:  int(sess.IdleTimeout.Milliseconds()),
				},
				Tenant: &TenantContext{
					TenantID:     auth.TenantID,
					BillingModel: auth.Billing.Model,
				},
			})

			insertUsageLogAsync(usageLogger, platform.UsageLog{
				APIKeyID:      auth.APIKeyID,
				UserID:        auth.TenantID,
				ExecutionType: "session_create",
				Language:      "",
				DurationMs:    0,
				ExitCode:      0,
				CostUSD:       0.0,
			})

		// POST /session/:id/run — execute code in session
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/run"):
			sessionID := strings.TrimSuffix(path, "/run")
			if sessionID == "" {
				http.Error(w, "missing session id", http.StatusBadRequest)
				return
			}
			if !ownsSession(r.Context(), mgr, sessionID, auth.TenantID) {
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}

			var req RunInSessionRequest
			if err := decodeJSON(w, r, &req); err != nil {
				http.Error(w, "invalid request body", http.StatusBadRequest)
				return
			}
			if req.Code == "" {
				http.Error(w, "code is required", http.StatusBadRequest)
				return
			}
			if req.Language == "" {
				http.Error(w, "language is required", http.StatusBadRequest)
				return
			}

			runTimeoutSec := int(resolveDuration(req.Timeout, auth.Config.DefaultExecTimeout, auth.Config.MaxExecTimeout).Seconds())

			start := time.Now()
			result, err := mgr.Execute(r.Context(), sessionID, req.Code, req.Language, runTimeoutSec)
			if err != nil {
				slog.Error("session execute failed", "session_id", sessionID, "err", err)
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			execDurationMs := time.Since(start).Seconds() * 1000

			sessSt, _ := mgr.GetSession(r.Context(), sessionID)
			sessTc := auth.Config

			output := &ExecutionOutput{
				Stdout:            result.Stdout,
				Stderr:            result.Stderr,
				ExitCode:          int(result.ExitCode),
				TerminationReason: result.TerminationReason,
				DurationMs:        execDurationMs,
				GuestDurationMs:   result.GuestDuration * 1000,
			}

			runResp := SessionExecuteResponse{
				Status:    "success",
				RequestID: requestID,
				SessionID: sessionID,
				Result:    output,
				Usage: &UsageInfo{
					ExecutionTimeMs: result.GuestDuration * 1000,
					QueueWaitMs:     0,
					TimeoutLimitMs:  int(sessTc.MaxExecTimeout.Milliseconds()),
				},
				Tenant: &TenantContext{
					TenantID:     auth.TenantID,
					BillingModel: auth.Billing.Model,
				},
				Session: &SessionState{
					State:     "active",
					LastUsed:  sessSt.LastUsed.Format(time.RFC3339),
					ExpiresAt: sessSt.LastUsed.Add(sessSt.IdleTimeout).Format(time.RFC3339),
					RunCount:  sessSt.RunCount,
				},
			}

			if result.ExitCode != 0 {
				runResp.Status = "error"
				runResp.Error = &ErrorDetail{
					Code:    "execution_failed",
					Message: fmt.Sprintf("process exited with code %d", result.ExitCode),
				}
			}

			insertUsageLogAsync(usageLogger, platform.UsageLog{
				APIKeyID:      auth.APIKeyID,
				UserID:        auth.TenantID,
				ExecutionType: "session_run",
				Language:      req.Language,
				DurationMs:    int(execDurationMs),
				ExitCode:      int(result.ExitCode),
				CostUSD:       (execDurationMs / 1000.0) * auth.Billing.ExecutionRateUSDPerSec,
				Stdout:        truncate(result.Stdout, 64*1024),
				Stderr:        truncate(result.Stderr, 64*1024),
			})
			// Wall-clock sandbox time is what burns credit (not just this command).
			billRuntimeAsync(usageLogger, sessionID, auth.Billing.ExecutionRateUSDPerSec)
			recordRunOutputAsync(usageLogger, platform.SandboxRun{
				ID:         uuid.NewString(),
				SandboxID:  sessionID,
				UserID:     auth.TenantID,
				Kind:       "run",
				Language:   req.Language,
				Command:    req.Code,
				ExitCode:   int(result.ExitCode),
				Status:     runStatus(int(result.ExitCode), result.TerminationReason),
				DurationMs: int(execDurationMs),
				StartedAt:  start.UTC().Format(time.RFC3339Nano),
			}, result.Stdout, result.Stderr, result.OutputLogsCaptured)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(runResp)

		// POST /session/:id/exec — run a shell command in the session's persistent bash
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/exec"):
			sessionID := strings.TrimSuffix(path, "/exec")
			if sessionID == "" {
				http.Error(w, "missing session id", http.StatusBadRequest)
				return
			}
			if !ownsSession(r.Context(), mgr, sessionID, auth.TenantID) {
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}

			var req ExecInSessionRequest
			if err := decodeJSON(w, r, &req); err != nil {
				http.Error(w, "invalid request body", http.StatusBadRequest)
				return
			}
			if req.Command == "" {
				http.Error(w, "command is required", http.StatusBadRequest)
				return
			}

			// Same rule as /run: unset uses the policy default, and the policy max
			// (advertised as max_execution_ms / timeout_limit_ms) is enforced.
			execTimeoutSec := int(resolveDuration(&req.Timeout, auth.Config.DefaultExecTimeout, auth.Config.MaxExecTimeout).Seconds())

			start := time.Now()
			result, err := mgr.Exec(r.Context(), sessionID, req.Command, execTimeoutSec)
			if err != nil {
				slog.Error("session exec failed", "session_id", sessionID, "err", err)
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			execDurationMs := time.Since(start).Seconds() * 1000

			sessSt, _ := mgr.GetSession(r.Context(), sessionID)
			sessTc := auth.Config

			output := &ExecutionOutput{
				Stdout:            result.Stdout,
				Stderr:            result.Stderr,
				ExitCode:          int(result.ExitCode),
				TerminationReason: result.TerminationReason,
				DurationMs:        execDurationMs,
				GuestDurationMs:   result.GuestDuration * 1000,
			}

			execResp := SessionExecuteResponse{
				Status:    "success",
				RequestID: requestID,
				SessionID: sessionID,
				Result:    output,
				Usage: &UsageInfo{
					ExecutionTimeMs: result.GuestDuration * 1000,
					QueueWaitMs:     0,
					TimeoutLimitMs:  int(sessTc.MaxExecTimeout.Milliseconds()),
				},
				Tenant: &TenantContext{
					TenantID:     auth.TenantID,
					BillingModel: auth.Billing.Model,
				},
				Session: &SessionState{
					State:     "active",
					LastUsed:  sessSt.LastUsed.Format(time.RFC3339),
					ExpiresAt: sessSt.LastUsed.Add(sessSt.IdleTimeout).Format(time.RFC3339),
					RunCount:  sessSt.RunCount,
				},
			}

			if result.ExitCode != 0 {
				execResp.Status = "error"
				execResp.Error = &ErrorDetail{
					Code:    "exec_failed",
					Message: fmt.Sprintf("command exited with code %d", result.ExitCode),
				}
			}

			insertUsageLogAsync(usageLogger, platform.UsageLog{
				APIKeyID:      auth.APIKeyID,
				UserID:        auth.TenantID,
				ExecutionType: "session_exec",
				Language:      "bash",
				DurationMs:    int(execDurationMs),
				ExitCode:      int(result.ExitCode),
				CostUSD:       (execDurationMs / 1000.0) * auth.Billing.ExecutionRateUSDPerSec,
				Stdout:        truncate(result.Stdout, 64*1024),
				Stderr:        truncate(result.Stderr, 64*1024),
			})
			billRuntimeAsync(usageLogger, sessionID, auth.Billing.ExecutionRateUSDPerSec)
			recordRunOutputAsync(usageLogger, platform.SandboxRun{
				ID:         uuid.NewString(),
				SandboxID:  sessionID,
				UserID:     auth.TenantID,
				Kind:       "exec",
				Language:   "bash",
				Command:    req.Command,
				ExitCode:   int(result.ExitCode),
				Status:     runStatus(int(result.ExitCode), result.TerminationReason),
				DurationMs: int(execDurationMs),
				StartedAt:  start.UTC().Format(time.RFC3339Nano),
			}, result.Stdout, result.Stderr, result.OutputLogsCaptured)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(execResp)

			// POST /session/:id/pause — pause session (snapshot to disk, free RAM/slot)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/pause"):
			sessionID := strings.TrimSuffix(path, "/pause")
			if sessionID == "" {
				http.Error(w, "missing session id", http.StatusBadRequest)
				return
			}
			if !ownsSession(r.Context(), mgr, sessionID, auth.TenantID) {
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}
			if err := mgr.Pause(r.Context(), sessionID); err != nil {
				slog.Error("session pause failed", "session_id", sessionID, "err", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// Bill active time up to the pause.
			billRuntimeAsync(usageLogger, sessionID, auth.Billing.ExecutionRateUSDPerSec)
			// state + timeline sync to the DB is handled by the manager's state hook
			recordAuditEventAsync(usageLogger, r, auth.TenantID, auth.APIKeyID, "sandbox.paused", "sandbox", sessionID, nil)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "success", "session_id": sessionID, "state": "paused"})

			// POST /session/:id/resume — resume a paused session
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/resume"):
			sessionID := strings.TrimSuffix(path, "/resume")
			if sessionID == "" {
				http.Error(w, "missing session id", http.StatusBadRequest)
				return
			}
			if !ownsSession(r.Context(), mgr, sessionID, auth.TenantID) {
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}
			if err := mgr.Resume(r.Context(), sessionID); err != nil {
				slog.Error("session resume failed", "session_id", sessionID, "err", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// state + timeline sync to the DB is handled by the manager's state hook
			recordAuditEventAsync(usageLogger, r, auth.TenantID, auth.APIKeyID, "sandbox.resumed", "sandbox", sessionID, nil)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "success", "session_id": sessionID, "state": "active"})

			// DELETE /session/:id — destroy session
		case r.Method == http.MethodDelete && path != "":
			sessionID := path
			if !ownsSession(r.Context(), mgr, sessionID, auth.TenantID) {
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}

			// Debit remaining wall-clock before destroy (sync so we don't lose the charge).
			usageLogger.BillSandboxRuntime(r.Context(), sessionID, auth.Billing.ExecutionRateUSDPerSec)
			if err := mgr.Destroy(r.Context(), sessionID); err != nil {
				slog.Error("session destroy failed", "session_id", sessionID, "err", err)
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			// state + timeline sync to the DB is handled by the manager's state hook
			recordAuditEventAsync(usageLogger, r, auth.TenantID, auth.APIKeyID, "sandbox.destroyed", "sandbox", sessionID, nil)

			w.WriteHeader(http.StatusNoContent)

			// GET /session — list the caller's sandboxes (SDK ro.list())
		case r.Method == http.MethodGet && path == "":
			items, err := usageLogger.ListSandboxes(r.Context(), auth.TenantID)
			if err != nil {
				slog.Error("list sandboxes failed", "err", err)
				http.Error(w, "failed to list sandboxes", http.StatusInternalServerError)
				return
			}
			if items == nil {
				items = []platform.SandboxListItem{}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"sandboxes": items})

			// GET /session/:id — session info
		case r.Method == http.MethodGet && path != "":
			sessionID := path

			sess, ok := mgr.GetSession(r.Context(), sessionID)
			if !ok || sess.UserID != auth.TenantID {
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}

			sessTc := auth.Config
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(SessionInfoResponse{
				Status:    "success",
				RequestID: requestID,
				Session: &SessionDetail{
					SessionID:    sess.ID,
					Image:        sess.Image,
					OS:           osOf(sess.Image),
					Template:     templateOf(sess.Image),
					State:        string(sess.State),
					BillingModel: sess.BillingModel,
					CreatedAt:    sess.CreatedAt.Format(time.RFC3339),
					LastUsed:     sess.LastUsed.Format(time.RFC3339),
					ExpiresAt:    sess.LastUsed.Add(sess.IdleTimeout).Format(time.RFC3339),
				},
				Limits: &SessionLimits{
					MaxSessions:    sessTc.MaxSessions,
					ActiveSessions: 0, // no per-tenant count yet
					MaxExecutionMs: int(sessTc.MaxExecTimeout.Milliseconds()),
					IdleTimeoutMs:  int(sess.IdleTimeout.Milliseconds()),
				},
				Stats: &SessionStats{
					RunCount:         sess.RunCount,
					TotalExecutionMs: sess.TotalExecutionMs,
					LastExitCode:     sess.LastExitCode,
				},
				Tenant: &TenantContext{
					TenantID:     auth.TenantID,
					BillingModel: sess.BillingModel,
				},
			})

		default:
			http.NotFound(w, r)
		}
	}
}

// ownsSession keeps every id-based operation tenant-scoped. Returning the same
// 404 for missing and foreign sessions avoids leaking another tenant's ids.
func ownsSession(ctx context.Context, mgr plane.Service, sessionID, userID string) bool {
	sess, ok := mgr.GetSession(ctx, sessionID)
	return ok && sess.UserID == userID
}
