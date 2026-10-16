// Control plane: auth, billing, metering ingestion, and the public REST API.
// Lifecycle commands delegate to the orchestrator; execution is sent directly
// to the selected private worker. This process never owns a local VM.
package main

import (
	"backend/internal/agent"
	"backend/internal/controlplane"
	controlconfig "backend/internal/controlplane/config"
	"backend/internal/handler"
	"backend/internal/ideauth"
	"backend/internal/logstream"
	"backend/internal/metering"
	"backend/internal/middleware"
	"backend/internal/modelrelay"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/preview"
	"backend/internal/secrets"
	"backend/internal/terminal"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func setupLogger() {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if os.Getenv("LOG_FORMAT") == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(h))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "control plane is healthy",
		"role":    "control-plane",
	})
}

func main() {
	setupLogger()

	cfg, err := controlconfig.Load()
	if err != nil {
		slog.Error("startup validation failed", "err", err)
		os.Exit(1)
	}

	platformClient, err := platform.NewClient(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("postgres initialization failed", "err", err)
		os.Exit(1)
	}
	defer platformClient.Close()
	retentionCtx, stopRetention := context.WithCancel(context.Background())
	defer stopRetention()
	go platformClient.RunLogRetention(retentionCtx)

	executionPolicy, err := platformClient.LoadExecutionPolicy(context.Background())
	if err != nil {
		slog.Error("execution policy load failed", "err", err)
		os.Exit(1)
	}
	billingConfig, err := platformClient.LoadBillingConfig(context.Background())
	if err != nil {
		slog.Error("billing config load failed", "err", err)
		os.Exit(1)
	}

	orchestratorClient := orchestrator.NewClient(cfg.OrchestratorURL, cfg.OrchestratorToken)
	if err := orchestratorClient.Health(context.Background()); err != nil {
		slog.Warn("orchestrator not healthy at startup; lifecycle requests will fail until it is", "url", cfg.OrchestratorURL, "err", err)
	} else {
		slog.Info("orchestrator reachable", "url", cfg.OrchestratorURL)
	}

	svc := controlplane.NewService(platformClient, orchestratorClient, cfg.WorkerToken)
	terminalWorkers := agent.NewTerminalPool(cfg.WorkerToken)
	defer terminalWorkers.Close()
	terminalManager, err := terminal.NewManager(terminal.DeriveSigningSecret(cfg.WorkerToken), 60*time.Second)
	if err != nil {
		slog.Error("terminal token initialization failed", "err", err)
		os.Exit(1)
	}
	previewSigner, err := preview.NewSigner(preview.DeriveSigningSecret(cfg.WorkerToken))
	if err != nil {
		slog.Error("preview token initialization failed", "err", err)
		os.Exit(1)
	}
	ideSigner, err := ideauth.NewSigner(ideauth.DeriveSigningSecret(cfg.WorkerToken))
	if err != nil {
		slog.Error("IDE token initialization failed", "err", err)
		os.Exit(1)
	}
	ideWorkers := agent.NewIDEClient(cfg.WorkerToken)

	// Model access: sandboxes call their agent's model through the worker's model
	// proxy and this relay, which adds the owner's saved provider key.
	var secretsKey []byte
	if cfg.SecretsEncryptionKey != "" {
		if secretsKey, err = secrets.ParseKey(cfg.SecretsEncryptionKey); err != nil {
			slog.Error("invalid SECRETS_ENCRYPTION_KEY", "err", err)
			os.Exit(1)
		}
	} else {
		slog.Warn("SECRETS_ENCRYPTION_KEY not set; model access for agent sessions is disabled")
	}
	modelWorkers := agent.NewModelAccessClient(cfg.WorkerToken)

	// Agent sessions: claim sessions from the platform's queue and run each
	// one's Claude Code turns in its sandbox (off unless AGENT_RUNNER_ENABLED).
	runnerCtx, stopRunner := context.WithCancel(context.Background())
	defer stopRunner()
	runnerDone := make(chan struct{})
	if cfg.AgentRunner.Enabled {
		pool := newAgentPool(cfg, svc, orchestratorClient, modelWorkers, platformClient, secretsKey, billingConfig.Model)
		go func() {
			defer close(runnerDone)
			pool.Run(runnerCtx)
		}()
	} else {
		close(runnerDone)
		slog.Info("agent session runner disabled (AGENT_RUNNER_ENABLED is not true)")
	}

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/session", handler.SessionHandler(svc, platformClient))
	publicMux.HandleFunc("/session/", handler.SessionHandler(svc, platformClient))
	publicMux.HandleFunc("GET /v1/catalog", handler.CatalogHandler())
	publicMux.HandleFunc("POST /v1/sandboxes/{sandboxID}/terminals", handler.CreateTerminalHandler(svc, orchestratorClient, terminalWorkers, terminalManager))
	publicMux.HandleFunc("POST /v1/sandboxes/{sandboxID}/ports/{port}/preview", handler.CreatePreviewHandler(svc, previewSigner, cfg.PreviewDomain))
	publicMux.HandleFunc("POST /v1/sandboxes/{sandboxID}/ide/sessions", handler.CreateIDESessionHandler(svc, orchestratorClient, ideWorkers, ideSigner, cfg.PreviewDomain))
	publicMux.HandleFunc("POST /v1/sandboxes/{sandboxID}/model-access", handler.CreateModelAccessHandler(svc, orchestratorClient, modelWorkers, platformClient, secretsKey))
	publicMux.HandleFunc("DELETE /v1/sandboxes/{sandboxID}/model-access", handler.DeleteModelAccessHandler(svc, orchestratorClient, modelWorkers))

	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/health", healthHandler)
	rootMux.Handle(metering.Route, metering.Handler(platformClient, cfg.WorkerToken))
	rootMux.Handle(logstream.Route, logstream.Handler(platformClient, cfg.WorkerToken))
	if len(secretsKey) > 0 {
		rootMux.Handle(plane.RouteModelRelay, modelrelay.New(platformClient, secretsKey, cfg.WorkerToken))
	}
	rootMux.HandleFunc("GET /v1/terminals/{terminalID}", handler.AttachTerminalHandler(orchestratorClient, terminalWorkers, terminalManager, cfg.TerminalAllowedOrigins))
	rootMux.Handle("/", middleware.Auth(platformClient, executionPolicy, billingConfig)(publicMux))

	port := ":" + cfg.Port
	slog.Info("control plane is running", "port", port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	chain := middleware.Logging(rootMux)

	srv := &http.Server{
		Addr:              port,
		Handler:           chain,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("error serving API", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("shutting down control plane")
	// Stop claiming agent sessions and stop their heartbeats. Running guest
	// turns are left alone: their leases expire and the next runner continues.
	stopRunner()
	select {
	case <-runnerDone:
	case <-time.After(10 * time.Second):
		slog.Warn("agent session runner did not stop in time")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http server shutdown error", "err", err)
	}
	slog.Info("shutdown complete")
}
