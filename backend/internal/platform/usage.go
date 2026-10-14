package platform

import (
	"context"
	"log/slog"
)

type UsageLog struct {
	APIKeyID      string  `json:"api_key_id"`
	UserID        string  `json:"user_id"`
	ExecutionType string  `json:"execution_type"`
	Language      string  `json:"language"`
	DurationMs    int     `json:"duration_ms"`
	ExitCode      int     `json:"exit_code"`
	CostUSD       float64 `json:"cost_usd"`
	Stdout        string  `json:"stdout"`
	Stderr        string  `json:"stderr"`
}

func (c *Client) InsertUsageLog(ctx context.Context, entry UsageLog) {
	_, err := c.pool.Exec(ctx, `
		INSERT INTO usage_logs
		(api_key_id, user_id, execution_type, language, duration_ms, exit_code, cost_usd, stdout, stderr)
		VALUES ($1,$2,$3,$4,$5,$6,$7,NULL,NULL)`,
		entry.APIKeyID, entry.UserID, entry.ExecutionType, entry.Language,
		entry.DurationMs, entry.ExitCode, entry.CostUSD,
	)
	if err != nil {
		slog.Warn("usage log insert failed", "err", err)
	}
}
