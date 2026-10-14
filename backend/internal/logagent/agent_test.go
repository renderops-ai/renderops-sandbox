package logagent

import (
	"backend/internal/logstream"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestDiskSpoolSurvivesRestartAndRetriesLostACK(t *testing.T) {
	status := http.StatusServiceUnavailable
	var received []logstream.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing worker token")
		}
		var batch logstream.Batch
		_ = json.NewDecoder(r.Body).Decode(&batch)
		received = batch.Events
		w.WriteHeader(status)
	}))
	defer server.Close()
	dir := t.TempDir()
	a, err := New(dir, server.URL, "secret", "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	event := logstream.Event{ID: uuid.NewString(), SandboxID: uuid.NewString(), WorkerID: "worker-1", Stream: "stdout", Content: "hello", TS: time.Now().UTC()}
	if err = a.spool([]logstream.Event{event}); err != nil {
		t.Fatal(err)
	}
	if err = a.sendPending(context.Background()); err == nil {
		t.Fatal("failure should retain spool")
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatal("batch lost")
	}
	a, err = New(dir, server.URL, "secret", "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	status = http.StatusNoContent
	if err = a.sendPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	files, _ = os.ReadDir(dir)
	if len(files) != 0 || len(received) != 1 || received[0].ID != event.ID {
		t.Fatal("bad ACK/replay")
	}
	event.TS = time.Now().Add(-49 * time.Hour)
	if err = a.spool([]logstream.Event{event}); err != nil {
		t.Fatal(err)
	}
	if err = a.sendPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	files, _ = os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("expired spool retained")
	}
}
