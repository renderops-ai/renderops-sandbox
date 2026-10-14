package logstream

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeStore struct {
	events []Event
	err    error
}

func (s *fakeStore) InsertLogBatch(_ context.Context, events []Event) error {
	s.events = events
	return s.err
}

func TestIngestValidatesAndAcknowledgesOnlyPersistedBatches(t *testing.T) {
	event := Event{ID: uuid.NewString(), SandboxID: uuid.NewString(), WorkerID: "worker-1", Stream: "stdout", Content: "hello\n", TS: time.Now().UTC()}
	body, _ := json.Marshal(Batch{Events: []Event{event}})
	store := &fakeStore{}
	call := func(payload, token string) int {
		req := httptest.NewRequest("POST", Route, strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		Handler(store, "secret").ServeHTTP(w, req)
		return w.Code
	}
	if call(string(body), "wrong") != http.StatusUnauthorized || len(store.events) != 0 {
		t.Fatal("unauthorized write")
	}
	if call(string(body), "secret") != http.StatusNoContent || len(store.events) != 1 {
		t.Fatal("valid write failed")
	}
	store.err = errors.New("offline")
	if call(string(body), "secret") != http.StatusServiceUnavailable {
		t.Fatal("ACK before persistence")
	}
	store.err = nil
	for _, payload := range []string{`{"events":[]}`, string(body) + `{}`, `{"events":[],"user_id":"spoof"}`} {
		if call(payload, "secret") != http.StatusBadRequest {
			t.Fatal("invalid payload accepted")
		}
	}
	event.TS = time.Now().Add(time.Hour)
	body, _ = json.Marshal(Batch{Events: []Event{event}})
	if call(string(body), "secret") != http.StatusBadRequest {
		t.Fatal("future timestamp accepted")
	}
}
