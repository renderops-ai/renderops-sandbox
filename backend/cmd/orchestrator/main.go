// Orchestrator owns worker registration, health, capacity reservations, and
// durable sandbox placement. It is an internal service and is never exposed to
// end users or placed in the execution streaming path.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"backend/internal/agent"
	"backend/internal/orchestrator"
	orchestratorconfig "backend/internal/orchestrator/config"
	"backend/internal/platform"
)

const (
	staleSchedulingThreshold = 5 * time.Minute
	staleSchedulingInterval  = time.Minute
)

func main() {
	cfg, err := orchestratorconfig.Load()
	if err != nil {
		slog.Error("orchestrator startup validation failed", "err", err)
		os.Exit(1)
	}

	db, err := platform.NewClient(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("orchestrator postgres initialization failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if count, err := db.FailStaleUnplacedSandboxes(cleanupCtx, staleSchedulingThreshold); err != nil {
		slog.Warn("initial stale scheduling cleanup failed", "err", err)
	} else if count > 0 {
		slog.Info("marked stale unplaced sandboxes as error", "count", count)
	}
	cleanupCancel()

	service := orchestrator.NewService(
		db,
		cfg.HeartbeatTTL,
		orchestrator.WithPlacementPolicy(orchestrator.PlacementPolicy{
			CPUOvercommitRatio:    cfg.CPUOvercommitRatio,
			MemoryOvercommitRatio: cfg.MemoryOvercommitRatio,
		}),
		orchestrator.WithWorkerClientFactory(func(endpoint string) orchestrator.WorkerClient {
			return agent.NewClient(endpoint, cfg.WorkerToken)
		}),
	)
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           orchestrator.NewHTTPServer(service, cfg.Token, cfg.WorkerToken).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// Pause and resume wait for the worker for up to agent.LifecycleTimeout.
		WriteTimeout:   agent.LifecycleTimeout + 30*time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		slog.Info(
			"orchestrator listening",
			"port", cfg.Port,
			"cpu_overcommit_ratio", cfg.CPUOvercommitRatio,
			"memory_overcommit_ratio", cfg.MemoryOvercommitRatio,
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("orchestrator server failed", "err", err)
			os.Exit(1)
		}
	}()

	cleanupStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(staleSchedulingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				count, err := db.FailStaleUnplacedSandboxes(ctx, staleSchedulingThreshold)
				cancel()
				if err != nil {
					slog.Warn("stale scheduling cleanup failed", "err", err)
				} else if count > 0 {
					slog.Info("marked stale unplaced sandboxes as error", "count", count)
				}
			case <-cleanupStop:
				return
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	close(cleanupStop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Warn("orchestrator shutdown failed", "err", err)
	}
}
