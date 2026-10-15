package config

import (
	"errors"
	"os"
	"strings"
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
}

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
	return cfg, nil
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
