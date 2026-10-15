package main

import (
	"log/slog"
	"os"
	"regexp"

	"backend/internal/agent"
	"backend/internal/agentsession"
	"backend/internal/controlplane"
	controlconfig "backend/internal/controlplane/config"
	"backend/internal/gitworkspace"
	"backend/internal/modelaccess"
	"backend/internal/orchestrator"
	"backend/internal/platform"
)

// newAgentPool wires the agent-session runner to this control plane's
// sandboxes, model access and worker clients.
func newAgentPool(cfg *controlconfig.Config, svc *controlplane.Service, orch *orchestrator.Client,
	modelWorkers *agent.ModelAccessClient, store *platform.Client, secretsKey []byte, billingModel string) *agentsession.Pool {
	queue := agentsession.NewHTTPQueue(cfg.AgentRunner.PlatformInternalURL, cfg.AgentRunner.WorkerToken)
	runtime := &agentsession.SandboxRuntime{
		Sandboxes: svc,
		Models: &modelaccess.Granter{
			Sandboxes: svc, Placements: orch, Workers: modelWorkers, Store: store, SecretsKey: secretsKey,
		},
		Workspaces:    queue,
		Workers:       agentsession.PlacementWorkers{Placements: orch, WorkerToken: cfg.WorkerToken},
		BillingModel:  billingModel,
		TokenKey:      agentsession.DeriveTokenKey(cfg.WorkerToken),
		TurnTimeout:   cfg.AgentRunner.TurnTimeout,
		PreviewDomain: cfg.PreviewDomain,
	}
	workerID := "cp-" + runnerHost()
	slog.Info("agent session runner enabled", "component", "agentsession", "platform_url", cfg.AgentRunner.PlatformInternalURL,
		"concurrency", cfg.AgentRunner.Concurrency, "turn_timeout", cfg.AgentRunner.TurnTimeout.String(), "worker_id", workerID)
	return &agentsession.Pool{
		Runner: agentsession.Runner{
			Queue:       queue,
			Runtime:     runtime,
			WorkerID:    workerID,
			TurnTimeout: cfg.AgentRunner.TurnTimeout,
			RepoDir:     gitworkspace.DefaultDir,
		},
		Concurrency: cfg.AgentRunner.Concurrency,
	}
}

var unsafeWorkerID = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// runnerHost names this process in the platform's worker IDs
// (^[\w.:-]{1,64}$ there, with a "-<n>" suffix per runner).
func runnerHost() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unknown"
	}
	host = unsafeWorkerID.ReplaceAllString(host, "-")
	if len(host) > 40 {
		host = host[:40]
	}
	return host
}
