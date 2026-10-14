package platform

import (
	"backend/internal/logstream"
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"testing"
	"time"
)

// Only a disposable local database is accepted. This never touches production
// and gives each test its own schema, removed on completion.
func TestLogBatchPostgresMigrationDedupOwnershipAndRetention(t *testing.T) {
	url := os.Getenv("LOG_STREAM_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LOG_STREAM_TEST_DATABASE_URL to a disposable local spawn_log_test database")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Database != "spawn_log_test" {
		t.Fatal("refusing a non-local/non-disposable database")
	}
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "log_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := &Client{pool: pool}
	_, err = pool.Exec(ctx, `CREATE TABLE sandboxes(id uuid PRIMARY KEY,user_id uuid,worker_id text,name text);
	 CREATE TABLE sandbox_logs(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,sandbox_id uuid,user_id uuid,stream text,content text,level text,ts timestamptz DEFAULT now());
	 CREATE TABLE usage_logs(id uuid PRIMARY KEY,created_at timestamptz,stdout text,stderr text);
	 CREATE TABLE audit_events(id uuid PRIMARY KEY,created_at timestamptz);`)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../../sandbox_platform/drizzle/0015_sandbox_log_stream.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, strings.ReplaceAll(string(migration), "--> statement-breakpoint", "")); err != nil {
		t.Fatal(err)
	}
	sandbox, user := uuid.NewString(), uuid.NewString()
	if _, err = pool.Exec(ctx, `INSERT INTO sandboxes VALUES($1,$2,'worker-1','test')`, sandbox, user); err != nil {
		t.Fatal(err)
	}
	event := logstream.Event{ID: uuid.NewString(), SandboxID: sandbox, WorkerID: "worker-1", Stream: "stdout", Content: "hello\x00world", TS: time.Now().UTC()}
	if err = client.InsertLogBatch(ctx, []logstream.Event{event}); err != nil {
		t.Fatal(err)
	}
	if err = client.InsertLogBatch(ctx, []logstream.Event{event}); err != nil {
		t.Fatal(err)
	}
	wrong := event
	wrong.ID = uuid.NewString()
	wrong.WorkerID = "different-worker"
	expired := event
	expired.ID = uuid.NewString()
	expired.TS = time.Now().Add(-49 * time.Hour)
	if err = client.InsertLogBatch(ctx, []logstream.Event{wrong, expired}); err != nil {
		t.Fatal(err)
	}
	var count int
	var owner, content string
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM sandbox_logs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate, expired or wrong-worker event stored: %d", count)
	}
	if err = pool.QueryRow(ctx, `SELECT user_id::text,content FROM sandbox_logs`).Scan(&owner, &content); err != nil {
		t.Fatal(err)
	}
	if owner != user || content != "helloworld" {
		t.Fatalf("ownership/NUL sanitation failed: %q %q", owner, content)
	}
	_, err = pool.Exec(ctx, `INSERT INTO sandbox_logs(sandbox_id,user_id,ts,content) VALUES($1,$2,now()-interval '49 hours','expired');
	`, sandbox, user)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO usage_logs VALUES($1,now()-interval '49 hours','legacy output','legacy error')`, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO audit_events VALUES($1,now()-interval '49 hours')`, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if n, err := client.PruneLogBatch(ctx); err != nil || n != 2 {
		t.Fatalf("retention prune: %d %v", n, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM sandbox_logs`).Scan(&count); err != nil || count != 1 {
		t.Fatal("recent output deleted", err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM usage_logs WHERE stdout IS NULL AND stderr IS NULL`).Scan(&count); err != nil || count != 1 {
		t.Fatal("billing metadata/payload retention failed", err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events`).Scan(&count); err != nil || count != 1 {
		t.Fatal("audit records deleted", err)
	}
}
