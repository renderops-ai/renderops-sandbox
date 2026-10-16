package config

import (
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgresql://renderops:secret@postgres:5432/renderops")
	t.Setenv("ORCHESTRATOR_URL", "http://orchestrator.internal:8090")
	t.Setenv("ORCHESTRATOR_TOKEN", "orchestration-secret")
	t.Setenv("WORKER_TOKEN", "worker-secret")
	t.Setenv("SECRETS_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	for _, name := range []string{"AGENT_RUNNER_ENABLED", "PLATFORM_INTERNAL_URL", "AGENT_WORKER_TOKEN", "AGENT_RUNNER_CONCURRENCY", "AGENT_TURN_TIMEOUT"} {
		t.Setenv(name, "")
	}
}

var agentToken = strings.Repeat("t", 32)

func TestAgentRunnerIsOffByDefault(t *testing.T) {
	setRequired(t)
	// Invalid runner settings are ignored while the runner is off.
	t.Setenv("AGENT_RUNNER_CONCURRENCY", "lots")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AgentRunner.Enabled || cfg.AgentRunner.WorkerToken != "" {
		t.Fatalf("runner = %+v, want disabled", cfg.AgentRunner)
	}
}

func TestAgentRunnerDefaults(t *testing.T) {
	setRequired(t)
	t.Setenv("AGENT_RUNNER_ENABLED", "true")
	t.Setenv("AGENT_WORKER_TOKEN", " "+agentToken+" ")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	r := cfg.AgentRunner
	if !r.Enabled || r.PlatformInternalURL != "http://platform:3000" || r.WorkerToken != agentToken ||
		r.Concurrency != 4 || r.TurnTimeout != 45*time.Minute {
		t.Fatalf("runner = %+v", r)
	}
}

func TestAgentRunnerOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("AGENT_RUNNER_ENABLED", "1")
	t.Setenv("AGENT_WORKER_TOKEN", agentToken)
	t.Setenv("PLATFORM_INTERNAL_URL", "http://platform.internal:3000/")
	t.Setenv("AGENT_RUNNER_CONCURRENCY", "2")
	t.Setenv("AGENT_TURN_TIMEOUT", "30m")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	r := cfg.AgentRunner
	if r.PlatformInternalURL != "http://platform.internal:3000" || r.Concurrency != 2 || r.TurnTimeout != 30*time.Minute {
		t.Fatalf("runner = %+v", r)
	}
}

func TestAgentRunnerFailsClosed(t *testing.T) {
	cases := map[string]map[string]string{
		"bad flag":          {"AGENT_RUNNER_ENABLED": "yes please"},
		"no token":          {"AGENT_RUNNER_ENABLED": "true"},
		"short token":       {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": "short"},
		"token with space":  {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken + " x"},
		"no secrets key":    {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "SECRETS_ENCRYPTION_KEY": ""},
		"bad platform url":  {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "PLATFORM_INTERNAL_URL": "platform:3000"},
		"url with creds":    {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "PLATFORM_INTERNAL_URL": "http://u:p@platform:3000"},
		"zero concurrency":  {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "AGENT_RUNNER_CONCURRENCY": "0"},
		"bad concurrency":   {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "AGENT_RUNNER_CONCURRENCY": "four"},
		"tiny turn timeout": {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "AGENT_TURN_TIMEOUT": "10s"},
		"bad turn timeout":  {"AGENT_RUNNER_ENABLED": "true", "AGENT_WORKER_TOKEN": agentToken, "AGENT_TURN_TIMEOUT": "45"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			for k, v := range env {
				t.Setenv(k, v)
			}
			if cfg, err := Load(); err == nil {
				t.Fatalf("Load accepted %v: %+v", env, cfg.AgentRunner)
			} else if strings.Contains(err.Error(), agentToken) {
				t.Fatal("error message contains the token")
			}
		})
	}
}
