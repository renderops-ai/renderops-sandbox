package handler

import (
	"backend/internal/middleware"
	"backend/internal/platform"
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"
)

func recordAuditEventAsync(
	logger platform.UsageLogger,
	r *http.Request,
	userID, apiKeyID, action, resourceType, resourceID string,
	metadata map[string]any,
) {
	actorType := "user"
	if apiKeyID != "" {
		actorType = "api_key"
	}
	ipAddress := r.RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		ipAddress = host
	}
	event := platform.AuditEvent{
		ScopeType:     "personal",
		ScopeID:       userID,
		ActorType:     actorType,
		ActorUserID:   userID,
		ActorAPIKeyID: apiKeyID,
		Action:        action,
		ResourceType:  resourceType,
		ResourceID:    resourceID,
		Outcome:       "success",
		RequestID:     middleware.RequestIDFromContext(r.Context()),
		IPAddress:     ipAddress,
		UserAgent:     r.UserAgent(),
		Metadata:      metadata,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		if err := logger.InsertAuditEvent(ctx, event); err != nil {
			slog.Warn("audit event insert failed", "action", action, "resource_id", resourceID, "err", err)
		}
	}()
}

func insertUsageLogAsync(logger platform.UsageLogger, log platform.UsageLog) {
	// usage_logs.api_key_id is required in the existing schema. Dashboard calls
	// authenticate with a Better Auth session and have no API key, while their
	// resource usage is still recorded by usage_meters. Do not send an invalid
	// empty UUID to PostgREST.
	if log.APIKeyID == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		logger.InsertUsageLog(ctx, log)
	}()
}

// billRuntimeAsync debits unbilled sandbox wall-clock time against the user's credit.
func billRuntimeAsync(logger platform.UsageLogger, sandboxID string, ratePerSec float64) {
	if sandboxID == "" || ratePerSec <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		logger.BillSandboxRuntime(ctx, sandboxID, ratePerSec)
	}()
}

// recordRunAsync writes the run-history summary (sandbox_runs) and its output lines
// (sandbox_logs, tagged with the run id) so the dashboard can list runs and, per run,
// show its stdout/stderr. Best-effort, off the request path.
func recordRunAsync(w platform.UsageLogger, run platform.SandboxRun, stdout, stderr string) {
	recordRunOutputAsync(w, run, stdout, stderr, false)
}

func recordRunOutputAsync(w platform.UsageLogger, run platform.SandboxRun, stdout, stderr string, captured bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		run.Command = truncate(run.Command, 8*1024)
		w.InsertSandboxRun(ctx, run)
		if captured {
			return
		} // Output already arrives via the worker's durable feed.
		// stderr is warn by default, but error when the run itself failed/timed out.
		stderrLevel := "warn"
		if run.Status == "error" || run.Status == "timeout" {
			stderrLevel = "error"
		}
		if stdout != "" {
			w.InsertSandboxLog(ctx, platform.SandboxLog{SandboxID: run.SandboxID, RunID: run.ID, UserID: run.UserID, Stream: "stdout", Level: "info", Language: run.Language, Content: truncate(stdout, 64*1024)})
		}
		if stderr != "" {
			w.InsertSandboxLog(ctx, platform.SandboxLog{SandboxID: run.SandboxID, RunID: run.ID, UserID: run.UserID, Stream: "stderr", Level: stderrLevel, Language: run.Language, Content: truncate(stderr, 64*1024)})
		}
	}()
}

// runStatus classifies an execution for the run-history table.
func runStatus(exitCode int, terminationReason string) string {
	if terminationReason == "timeout" {
		return "timeout"
	}
	if exitCode != 0 {
		return "error"
	}
	return "ok"
}

func truncate(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	return s[:maxBytes]
}

// resolveDuration resolves a timeout (per-execution, idle, or lifetime) from an optional
// caller-supplied value in seconds: nil/<=0 → def; otherwise the request clamped to cap.
func resolveDuration(requestedSec *int, def, cap time.Duration) time.Duration {
	if requestedSec == nil || *requestedSec <= 0 {
		return def
	}
	d := time.Duration(*requestedSec) * time.Second
	if d > cap {
		return cap
	}
	return d
}
