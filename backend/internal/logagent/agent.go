// Package logagent captures the guest's read-only output feed independently of
// terminal attachments. Batches live on the worker disk until ingestion ACKs.
package logagent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"backend/internal/executor/firecracker"
	"backend/internal/logstream"
	"github.com/google/uuid"
)

type Agent struct {
	Dir, URL, Token, WorkerID string
	client                    *http.Client
	mu                        sync.Mutex
	watches                   map[string]context.CancelFunc
	closed                    bool
	wg                        sync.WaitGroup
	spoolMu                   sync.Mutex
}

func New(dir, endpoint, token, workerID string) (*Agent, error) {
	if endpoint == "" || token == "" || workerID == "" {
		return nil, fmt.Errorf("log endpoint, worker token and worker ID are required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	return &Agent{Dir: dir, URL: endpoint, Token: token, WorkerID: workerID, client: &http.Client{Timeout: 10 * time.Second}, watches: make(map[string]context.CancelFunc)}, nil
}

func (a *Agent) Watch(id, path string) {
	a.Stop(id)
	ctx, cancel := context.WithCancel(context.Background())
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		cancel()
		return
	}
	a.watches[id] = cancel
	a.wg.Add(1)
	a.mu.Unlock()
	go func() { defer a.wg.Done(); a.capture(ctx, id, path) }()
}

func (a *Agent) Stop(id string) {
	a.mu.Lock()
	cancel := a.watches[id]
	delete(a.watches, id)
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (a *Agent) Close() {
	a.mu.Lock()
	a.closed = true
	for _, cancel := range a.watches {
		cancel()
	}
	a.watches = make(map[string]context.CancelFunc)
	a.mu.Unlock()
	a.wg.Wait()
}

type guestEvent struct {
	Epoch    string    `json:"epoch"`
	Sequence uint64    `json:"sequence"`
	Stream   string    `json:"stream"`
	Content  string    `json:"content"`
	TS       time.Time `json:"ts"`
}

func (a *Agent) capture(ctx context.Context, id, path string) {
	var epoch string
	var after uint64
	for ctx.Err() == nil {
		conn, err := firecracker.NewVsockClient(path).Connect()
		if err == nil {
			// Cancellation unblocks reads; this feed does not extend sandbox idle TTL.
			stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
			err = json.NewEncoder(conn).Encode(map[string]any{"type": "logs_follow", "log_sandbox_id": id, "log_epoch": epoch, "log_after": after})
			if err == nil {
				err = a.readFeed(ctx, conn, id, &epoch, &after)
			}
			stopClose()
			_ = conn.Close()
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Warn("guest output feed reconnecting", "sandbox_id", id, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (a *Agent) readFeed(ctx context.Context, conn interface {
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}, id string, epoch *string, after *uint64) error {
	reader := bufio.NewReaderSize(conn, 64*1024)
	batch := make([]logstream.Event, 0, 100)
	bytesInBatch := 0
	var last guestEvent
	lastFlush := time.Now()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := a.spool(batch); err != nil {
			return err
		}
		*epoch, *after = last.Epoch, last.Sequence
		batch = batch[:0]
		bytesInBatch = 0
		lastFlush = time.Now()
		return nil
	}
	// Reader can idle; a short deadline flushes pending batches within 250ms.
	// bufio.ReadBytes preserves partial fragments through timeouts below.
	var partial []byte
	for {
		_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		line, err := reader.ReadBytes('\n')
		partial = append(partial, line...)
		if len(partial) > 128*1024 {
			return fmt.Errorf("guest output frame too large")
		}
		if len(line) > 0 && line[len(line)-1] == '\n' {
			var event guestEvent
			if decodeErr := json.Unmarshal(partial, &event); decodeErr != nil {
				return decodeErr
			}
			partial = nil
			if event.Epoch != "" && event.Sequence > 0 && event.Content != "" {
				eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%s/%d", id, event.Epoch, event.Sequence))).String()
				batch = append(batch, logstream.Event{ID: eventID, SandboxID: id, WorkerID: a.WorkerID, Stream: event.Stream, Content: event.Content, TS: event.TS})
				last = event
				bytesInBatch += len(event.Content)
			}
		}
		if len(batch) >= 100 || bytesInBatch >= 64*1024 || time.Since(lastFlush) >= 250*time.Millisecond || err != nil {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
		}
		if err != nil {
			if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() && ctx.Err() == nil {
				continue
			}
			return err
		}
	}
}

func (a *Agent) spool(events []logstream.Event) error {
	a.spoolMu.Lock()
	defer a.spoolMu.Unlock()
	files, err := os.ReadDir(a.Dir)
	if err != nil {
		return err
	}
	var used int64
	for _, file := range files {
		if info, e := file.Info(); e == nil {
			used += info.Size()
		}
	}
	body, err := json.Marshal(logstream.Batch{Events: events})
	if err != nil {
		return err
	}
	if used+int64(len(body)) > 64<<20 {
		return fmt.Errorf("log spool full (64 MiB); guest replay will report any overflow")
	}
	file, err := os.CreateTemp(a.Dir, ".pending-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(body); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(a.Dir, fmt.Sprintf("%020d-%s.json", time.Now().UnixNano(), uuid.NewString()))); err != nil {
		return err
	}
	if dir, err := os.Open(a.Dir); err == nil {
		err = dir.Sync()
		_ = dir.Close()
		return err
	}
	return nil
}

// Run sends durable batches in order. Failed sends are retried, never silently
// removed; idempotent event IDs cover a lost ACK or guest replay after restart.
func (a *Agent) Run(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.sendPending(ctx); err != nil {
				slog.Warn("log batch pending retry", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
		}
	}
}

func (a *Agent) sendPending(ctx context.Context) error {
	files, err := os.ReadDir(a.Dir)
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		path := filepath.Join(a.Dir, file.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var batch logstream.Batch
		if err = json.Unmarshal(body, &batch); err != nil {
			return err
		}
		// The worker spool follows the same 48h retention, including outages.
		cutoff := time.Now().Add(-48 * time.Hour)
		retained := batch.Events[:0]
		for _, event := range batch.Events {
			if !event.TS.Before(cutoff) {
				retained = append(retained, event)
			}
		}
		batch.Events = retained
		if len(retained) == 0 {
			if err = os.Remove(path); err != nil {
				return err
			}
			continue
		}
		body, _ = json.Marshal(batch)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+a.Token)
		response, err := a.client.Do(req)
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			return fmt.Errorf("log ingest returned %d", response.StatusCode)
		}
		if err = os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}
