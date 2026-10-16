package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config contains the control plane's database, API, and worker-connection settings.
type Config struct {
	DatabaseURL string
	Port        string
	LogLevel    string
	LogFormat   string

	OrchestratorURL        string
	OrchestratorToken      string
	WorkerToken            string
	TerminalAllowedOrigins []string
	PreviewDomain          string
	// SecretsEncryptionKey decrypts users' saved model provider credentials
	// (internal/secrets) for the model relay. Empty disables model access.
	SecretsEncryptionKey string

	AgentRunner AgentRunner
	Uploads     Uploads
}

// Uploads configures CLI folder uploads (internal/uploads): an Azure Blob
// container that clients PUT archives to through short-lived SAS URLs. Off
// unless UPLOADS_CONTAINER_NAME is set; then BLOB_STORAGE_ACCOUNT and
// BLOB_SECRET_KEY (the account's shared key, used to sign the URLs) are
// required.
type Uploads struct {
	Account    string
	AccountKey string
	Container  string
	MaxBytes   int64
}

// Enabled reports whether uploads are configured.
func (u Uploads) Enabled() bool {
	return u.Container != ""
}

// AgentRunner configures the agent-session runner (internal/agentsession),
// which claims sessions from the platform's queue and drives Claude Code in
// each session's sandbox. Off unless AGENT_RUNNER_ENABLED=true.
type AgentRunner struct {
	Enabled bool
	// PlatformInternalURL is the platform's base URL on the compose network.
	PlatformInternalURL string
	// WorkerToken authenticates the runner to the platform's internal
	// agent-session API (AGENT_WORKER_TOKEN, shared with the platform).
	WorkerToken string
	Concurrency int
	TurnTimeout time.Duration
}

const (
	defaultPlatformInternalURL = "http://platform:3000"
	defaultAgentConcurrency    = 4
	defaultAgentTurnTimeout    = 45 * time.Minute
	minAgentWorkerTokenLength  = 32
)

// Load reads only control-plane settings. It performs no worker host or asset
// validation, so the control plane remains independent of Firecracker.
func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:            strings.TrimSpace(os.Getenv("DATABASE_URL")),
		Port:                   defaultString(strings.TrimSpace(os.Getenv("PORT")), "8080"),
		LogLevel:               defaultString(strings.TrimSpace(os.Getenv("LOG_LEVEL")), "info"),
		LogFormat:              defaultString(strings.TrimSpace(os.Getenv("LOG_FORMAT")), "json"),
		OrchestratorURL:        strings.TrimRight(strings.TrimSpace(os.Getenv("ORCHESTRATOR_URL")), "/"),
		OrchestratorToken:      strings.TrimSpace(os.Getenv("ORCHESTRATOR_TOKEN")),
		WorkerToken:            strings.TrimSpace(os.Getenv("WORKER_TOKEN")),
		TerminalAllowedOrigins: splitCSV(os.Getenv("TERMINAL_ALLOWED_ORIGINS")),
		PreviewDomain:          defaultString(strings.TrimSpace(os.Getenv("PREVIEW_DOMAIN")), "dev-sandbox.renderops.com"),
		SecretsEncryptionKey:   strings.TrimSpace(os.Getenv("SECRETS_ENCRYPTION_KEY")),
	}

	if cfg.DatabaseURL == "" {
		return nil, errors.New("DATABASE_URL must be set")
	}
	if cfg.OrchestratorURL == "" {
		return nil, errors.New("ORCHESTRATOR_URL must be set")
	}
	if cfg.OrchestratorToken == "" {
		return nil, errors.New("ORCHESTRATOR_TOKEN must be set")
	}
	if cfg.WorkerToken == "" {
		return nil, errors.New("WORKER_TOKEN must be set")
	}
	runner, err := loadAgentRunner(cfg.SecretsEncryptionKey)
	if err != nil {
		return nil, err
	}
	cfg.AgentRunner = runner
	uploads, err := loadUploads()
	if err != nil {
		return nil, err
	}
	cfg.Uploads = uploads
	return cfg, nil
}

var (
	storageAccountPattern   = regexp.MustCompile(`^[a-z0-9]{3,24}$`)
	storageContainerPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
)

const (
	defaultUploadMaxBytes = 500 << 20
	minUploadMaxBytes     = 1 << 20
	maxUploadMaxBytes     = 5000 << 20
)

// loadUploads reads the UPLOADS_* settings (and the BLOB_* credentials they
// use). When uploads are on every setting must be valid (fail closed).
func loadUploads() (Uploads, error) {
	container := strings.TrimSpace(os.Getenv("UPLOADS_CONTAINER_NAME"))
	if container == "" {
		return Uploads{}, nil
	}
	uploads := Uploads{
		Account:    strings.TrimSpace(os.Getenv("BLOB_STORAGE_ACCOUNT")),
		AccountKey: strings.TrimSpace(os.Getenv("BLOB_SECRET_KEY")),
		Container:  container,
		MaxBytes:   defaultUploadMaxBytes,
	}
	if !storageContainerPattern.MatchString(container) || strings.Contains(container, "--") {
		return Uploads{}, fmt.Errorf("UPLOADS_CONTAINER_NAME %q is not a valid Azure Blob container name", container)
	}
	if !storageAccountPattern.MatchString(uploads.Account) {
		return Uploads{}, errors.New("BLOB_STORAGE_ACCOUNT must be set to a storage account name when UPLOADS_CONTAINER_NAME is set")
	}
	if uploads.AccountKey == "" {
		return Uploads{}, errors.New("BLOB_SECRET_KEY must be set when UPLOADS_CONTAINER_NAME is set")
	}
	if raw := strings.TrimSpace(os.Getenv("UPLOADS_MAX_BYTES")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < minUploadMaxBytes || value > maxUploadMaxBytes {
			return Uploads{}, fmt.Errorf("UPLOADS_MAX_BYTES must be a byte count from %d to %d, got %q", int64(minUploadMaxBytes), int64(maxUploadMaxBytes), raw)
		}
		uploads.MaxBytes = value
	}
	return uploads, nil
}

// loadAgentRunner reads the AGENT_* settings. When the runner is enabled every
// setting must be valid (fail closed); when it is off they are not read.
func loadAgentRunner(secretsKey string) (AgentRunner, error) {
	enabled := false
	if raw := strings.TrimSpace(os.Getenv("AGENT_RUNNER_ENABLED")); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return AgentRunner{}, fmt.Errorf("AGENT_RUNNER_ENABLED must be true or false: %w", err)
		}
		enabled = value
	}
	if !enabled {
		return AgentRunner{}, nil
	}

	runner := AgentRunner{
		Enabled:             true,
		PlatformInternalURL: strings.TrimRight(defaultString(strings.TrimSpace(os.Getenv("PLATFORM_INTERNAL_URL")), defaultPlatformInternalURL), "/"),
		WorkerToken:         strings.TrimSpace(os.Getenv("AGENT_WORKER_TOKEN")),
		Concurrency:         defaultAgentConcurrency,
		TurnTimeout:         defaultAgentTurnTimeout,
	}
	platformURL, err := url.Parse(runner.PlatformInternalURL)
	if err != nil || (platformURL.Scheme != "http" && platformURL.Scheme != "https") || platformURL.Host == "" ||
		platformURL.User != nil || platformURL.RawQuery != "" {
		return AgentRunner{}, fmt.Errorf("PLATFORM_INTERNAL_URL %q must be an absolute http(s) URL without credentials", runner.PlatformInternalURL)
	}
	if len(runner.WorkerToken) < minAgentWorkerTokenLength || strings.ContainsAny(runner.WorkerToken, " \t\r\n") {
		return AgentRunner{}, fmt.Errorf("AGENT_WORKER_TOKEN must be set to at least %d characters when AGENT_RUNNER_ENABLED=true", minAgentWorkerTokenLength)
	}
	if secretsKey == "" {
		// Agent sessions reach their model only through model access.
		return AgentRunner{}, errors.New("SECRETS_ENCRYPTION_KEY must be set when AGENT_RUNNER_ENABLED=true")
	}
	if raw := strings.TrimSpace(os.Getenv("AGENT_RUNNER_CONCURRENCY")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 64 {
			return AgentRunner{}, fmt.Errorf("AGENT_RUNNER_CONCURRENCY must be an integer from 1 to 64, got %q", raw)
		}
		runner.Concurrency = value
	}
	if raw := strings.TrimSpace(os.Getenv("AGENT_TURN_TIMEOUT")); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value < time.Minute || value > 6*time.Hour {
			return AgentRunner{}, fmt.Errorf("AGENT_TURN_TIMEOUT must be a duration from 1m to 6h, got %q", raw)
		}
		runner.TurnTimeout = value
	}
	return runner, nil
}

func splitCSV(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
