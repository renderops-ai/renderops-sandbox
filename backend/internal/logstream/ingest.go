package logstream

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"net/http"
	"strings"
	"time"
)

const Route = "/internal/logs/batches"
const Retention = 48 * time.Hour

type Event struct {
	ID        string    `json:"id"`
	SandboxID string    `json:"sandbox_id"`
	WorkerID  string    `json:"worker_id"`
	Stream    string    `json:"stream"`
	Content   string    `json:"content"`
	TS        time.Time `json:"ts"`
}
type Batch struct {
	Events []Event `json:"events"`
}
type Store interface {
	InsertLogBatch(context.Context, []Event) error
}

func Handler(store Store, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "method not allowed", 405)
			return
		}
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var batch Batch
		var extra any
		if decoder.Decode(&batch) != nil || decoder.Decode(&extra) != io.EOF || len(batch.Events) == 0 || len(batch.Events) > 100 {
			http.Error(w, "invalid log batch", 400)
			return
		}
		for _, event := range batch.Events {
			_, idErr := uuid.Parse(event.ID)
			_, sandboxErr := uuid.Parse(event.SandboxID)
			if idErr != nil || sandboxErr != nil || event.WorkerID == "" || len(event.WorkerID) > 128 || len(event.Content) == 0 || len(event.Content) > 64*1024 || event.TS.IsZero() || event.TS.After(time.Now().Add(5*time.Minute)) || (event.Stream != "stdout" && event.Stream != "stderr" && event.Stream != "system") {
				http.Error(w, "invalid log event", 400)
				return
			}
		}
		if err := store.InsertLogBatch(r.Context(), batch.Events); err != nil {
			http.Error(w, "unable to persist log batch", 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
