package platform

import (
	"backend/internal/logstream"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

// No user_id is accepted from workers: ownership comes from the sandbox row.
// The placement must still belong to the reporting worker. Released/moved
// sandbox events and events beyond retention are consumed without storing.
func (c *Client) InsertLogBatch(ctx context.Context, events []logstream.Event) error {
	for i := range events {
		events[i].Content = strings.ReplaceAll(events[i].Content, "\x00", "")
	}
	body, err := json.Marshal(events)
	if err != nil {
		return err
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// All output writers serialize identity allocation until commit, so an SSE
	// ID cursor cannot jump over an earlier, still-uncommitted output insert.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1397772108)`); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO sandbox_logs (event_id,sandbox_id,user_id,stream,content,level,ts)
 SELECT e.id,s.id,s.user_id,e.stream,e.content,CASE WHEN e.stream='stderr' THEN 'warn' ELSE 'info' END,e.ts
 FROM jsonb_to_recordset($1::jsonb) AS e(id uuid,sandbox_id uuid,worker_id text,stream text,content text,ts timestamptz)
 JOIN sandboxes s ON s.id=e.sandbox_id AND s.worker_id=e.worker_id
 WHERE e.ts >= now()-interval '48 hours'
 ON CONFLICT (event_id) DO NOTHING`, body)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Small indexed deletes avoid a long locking transaction; reads independently
// enforce 48h so expired output isn't visible while cleanup catches up.
func (c *Client) PruneLogBatch(ctx context.Context) (int64, error) {
	result, err := c.pool.Exec(ctx, `DELETE FROM sandbox_logs WHERE id IN
 (SELECT id FROM sandbox_logs WHERE ts < now()-interval '48 hours' ORDER BY ts LIMIT 1000)`)
	if err != nil {
		return 0, err
	}
	// Old billing rows contain duplicate output. Keep billing metadata, but
	// remove those payloads under the same policy.
	usage, err := c.pool.Exec(ctx, `UPDATE usage_logs SET stdout=NULL,stderr=NULL WHERE id IN
	 (SELECT id FROM usage_logs WHERE created_at < now()-interval '48 hours'
	 AND (stdout IS NOT NULL OR stderr IS NOT NULL) ORDER BY created_at LIMIT 1000)`)
	return result.RowsAffected() + usage.RowsAffected(), err
}

func (c *Client) RunLogRetention(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		for i := 0; i < 10; i++ {
			deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			n, err := c.PruneLogBatch(deleteCtx)
			cancel()
			if err != nil {
				slog.Warn("output log retention cleanup failed", "err", err)
				break
			}
			if n < 1000 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
