package agentsession

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	workerToken = "agent-worker-token-0123456789abcdef"
	leaseToken  = "0f4c6a5e-7a39-4b1e-9c55-2d8f3f1a6b10"
	sessionID   = "6a1f0c2e-3b4d-4e5f-8a9b-0c1d2e3f4a5b"
)

type recorded struct {
	path string
	body map[string]any
}

type platformStub struct {
	t        *testing.T
	mu       sync.Mutex
	requests []recorded
	respond  func(path string, body map[string]any) (int, string)
}

func newPlatform(t *testing.T, respond func(path string, body map[string]any) (int, string)) (*HTTPQueue, *platformStub) {
	t.Helper()
	stub := &platformStub{t: t, respond: respond}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+workerToken {
			t.Errorf("authorization header %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type %q", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("body %q: %v", raw, err)
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/internal/agent-sessions")
		stub.mu.Lock()
		stub.requests = append(stub.requests, recorded{path: path, body: body})
		stub.mu.Unlock()
		status, answer := stub.respond(path, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if _, err := io.WriteString(w, answer); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return NewHTTPQueue(server.URL+"/", workerToken), stub
}

func (p *platformStub) last() recorded {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests[len(p.requests)-1]
}

var testLease = Lease{SessionID: sessionID, Token: leaseToken}

func TestHTTPQueueClaim(t *testing.T) {
	answer := 204
	q, stub := newPlatform(t, func(string, map[string]any) (int, string) {
		if answer == 204 {
			return 204, ""
		}
		// Postgres bigints may arrive as strings; timestamps in either format.
		return 200, `{"session":{"id":"` + sessionID + `","ownerUserId":"user-1","projectId":"p-1","sourceKind":"issue",
			"sourceNumber":2,"title":"Validate events","agent":"claude-code","status":"queued","sandboxId":null,
			"cancelRequested":false,"interruptRequested":true,"runtimeCursor":"1234","deliveredSeq":2,
			"leaseToken":"` + leaseToken + `","leaseExpiresAt":"2026-10-16 04:21:00.123+00"}}`
	})
	s, err := q.Claim(context.Background(), "cp-0", time.Minute)
	if err != nil || s != nil {
		t.Fatalf("empty claim = %+v, %v", s, err)
	}
	if req := stub.last(); req.path != "/claim" || req.body["workerId"] != "cp-0" || req.body["leaseSeconds"] != float64(60) {
		t.Fatalf("claim request = %+v", req)
	}
	answer = 200
	s, err = q.Claim(context.Background(), "cp-0", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := Session{ID: sessionID, OwnerUserID: "user-1", ProjectID: "p-1", SourceKind: "issue", SourceNumber: 2,
		Title: "Validate events", Agent: "claude-code", Status: "queued", InterruptRequested: true,
		RuntimeCursor: 1234, DeliveredSeq: 2, LeaseToken: leaseToken}
	got := *s
	if got.LeaseExpiresAt.IsZero() {
		t.Fatal("lease expiry not parsed")
	}
	got.LeaseExpiresAt = time.Time{}
	if got != want {
		t.Fatalf("session = %+v", got)
	}
}

func TestHTTPQueueRefusesAClaimWithoutAValidLease(t *testing.T) {
	q, _ := newPlatform(t, func(string, map[string]any) (int, string) {
		return 200, `{"session":{"id":"` + sessionID + `","leaseToken":"worker-1"}}`
	})
	if _, err := q.Claim(context.Background(), "cp-0", time.Minute); err == nil {
		t.Fatal("accepted a claim without a lease token")
	}
}

func TestHTTPQueueLeasedCalls(t *testing.T) {
	q, stub := newPlatform(t, func(path string, body map[string]any) (int, string) {
		switch path {
		case "/" + sessionID + "/heartbeat":
			return 200, `{"cancelRequested":true,"interruptRequested":false,"status":"running","leaseExpiresAt":"2026-10-16T04:21:00.000Z"}`
		case "/" + sessionID + "/events":
			return 200, `{"lastSeq":9,"status":"running","runtimeCursor":120,"duplicate":true}`
		case "/" + sessionID + "/inbox":
			return 200, `{"messages":[{"seq":2,"text":"Fix it"},{"seq":"5","text":"and test"}]}`
		case "/" + sessionID + "/inbox/ack":
			return 200, `{"deliveredSeq":5}`
		case "/" + sessionID + "/release":
			return 200, `{"released":true,"status":"waiting","lastSeq":10}`
		case "/" + sessionID + "/workspace-grant":
			return 200, `{"repository":"o/r","headSha":"` + strings.Repeat("a", 40) + `","baseSha":"` + strings.Repeat("a", 40) + `","token":"ghs_x","expiresAt":"2026-10-16T05:00:00Z"}`
		}
		return 500, `{"error":"unexpected"}`
	})
	ctx := context.Background()

	hb, err := q.Heartbeat(ctx, testLease, 90*time.Second)
	if err != nil || !hb.CancelRequested || hb.Status != "running" || hb.LeaseExpiresAt.IsZero() {
		t.Fatalf("heartbeat = %+v, %v", hb, err)
	}
	if req := stub.last(); req.body["leaseToken"] != leaseToken || req.body["leaseSeconds"] != float64(90) {
		t.Fatalf("heartbeat body = %v", req.body)
	}

	events := []Event{{Type: "message", Actor: "agent", Payload: map[string]any{"text": "a <b> & c"}}}
	if err := q.Append(ctx, testLease, &CursorRange{From: 100, To: 120}, events); err != nil {
		t.Fatal(err)
	}
	req := stub.last()
	if req.body["cursorFrom"] != float64(100) || req.body["cursorTo"] != float64(120) || len(req.body["events"].([]any)) != 1 {
		t.Fatalf("append body = %v", req.body)
	}
	if err := q.Append(ctx, testLease, nil, events); err != nil {
		t.Fatal(err)
	}
	if _, ok := stub.last().body["cursorFrom"]; ok {
		t.Fatal("runner-origin events carried a cursor")
	}

	messages, err := q.Inbox(ctx, testLease)
	if err != nil || len(messages) != 2 || messages[1] != (Message{Seq: 5, Text: "and test"}) {
		t.Fatalf("inbox = %+v, %v", messages, err)
	}
	if err := q.Ack(ctx, testLease, 5); err != nil || stub.last().body["seq"] != float64(5) {
		t.Fatalf("ack = %v, body %v", err, stub.last().body)
	}
	if err := q.Release(ctx, testLease, true); err != nil || stub.last().body["idle"] != true {
		t.Fatalf("release = %v, body %v", err, stub.last().body)
	}
	grant, err := q.WorkspaceGrant(ctx, testLease)
	if err != nil || grant.Repository != "o/r" || grant.Token != "ghs_x" || grant.ExpiresAt.IsZero() {
		t.Fatalf("grant = %+v, %v", grant, err)
	}
}

func TestHTTPQueueErrors(t *testing.T) {
	type answer struct {
		status int
		body   string
	}
	cases := []struct {
		name string
		call func(q *HTTPQueue) error
		ans  answer
		want func(error) bool
	}{
		{"lease lost", func(q *HTTPQueue) error {
			_, err := q.Heartbeat(context.Background(), testLease, time.Minute)
			return err
		},
			answer{409, `{"error":"Lease lost"}`}, func(err error) bool { return errors.Is(err, ErrLeaseLost) }},
		{"cursor conflict", func(q *HTTPQueue) error {
			return q.Append(context.Background(), testLease, &CursorRange{From: 1, To: 2}, []Event{{Type: "step"}})
		}, answer{409, `{"error":"Runtime cursor mismatch","runtimeCursor":"77"}`}, func(err error) bool {
			var c *CursorConflictError
			return errors.As(err, &c) && c.RuntimeCursor == 77
		}},
		{"pending", func(q *HTTPQueue) error { return q.Release(context.Background(), testLease, true) },
			answer{409, `{"error":"Undelivered messages","pending":true}`}, func(err error) bool { return errors.Is(err, ErrPending) }},
		{"bad event", func(q *HTTPQueue) error {
			return q.Append(context.Background(), testLease, nil, []Event{{Type: "preview"}})
		},
			answer{400, `{"error":"unknown event type: preview"}`}, func(err error) bool {
				return errors.Is(err, ErrRejected) && strings.Contains(err.Error(), "unknown event type: preview")
			}},
		{"too large", func(q *HTTPQueue) error {
			return q.Append(context.Background(), testLease, nil, []Event{{Type: "step"}})
		},
			answer{413, `{"error":"The request is larger than 256 KB"}`}, func(err error) bool { return errors.Is(err, ErrRejected) }},
		{"no token on platform", func(q *HTTPQueue) error { _, err := q.Claim(context.Background(), "cp", time.Minute); return err },
			answer{404, ``}, func(err error) bool { return errors.Is(err, ErrQueueAuth) }},
		{"wrong token", func(q *HTTPQueue) error { _, err := q.Inbox(context.Background(), testLease); return err },
			answer{401, ``}, func(err error) bool { return errors.Is(err, ErrQueueAuth) }},
		{"unknown session", func(q *HTTPQueue) error { _, err := q.Inbox(context.Background(), testLease); return err },
			answer{404, `{"error":"Unknown session."}`}, func(err error) bool { return errors.Is(err, ErrLeaseLost) }},
		{"repo access revoked", func(q *HTTPQueue) error { _, err := q.WorkspaceGrant(context.Background(), testLease); return err },
			answer{403, `{"error":"The GitHub App no longer has access to o/r."}`}, func(err error) bool {
				var w *WorkspaceGrantError
				return errors.As(err, &w) && w.Message == "The GitHub App no longer has access to o/r."
			}},
		{"grant on a finished session", func(q *HTTPQueue) error { _, err := q.WorkspaceGrant(context.Background(), testLease); return err },
			answer{409, `{"error":"This session has finished."}`}, func(err error) bool {
				var w *WorkspaceGrantError
				return errors.As(err, &w)
			}},
		{"grant lease lost", func(q *HTTPQueue) error { _, err := q.WorkspaceGrant(context.Background(), testLease); return err },
			answer{409, `{"error":"Lease lost"}`}, func(err error) bool { return errors.Is(err, ErrLeaseLost) }},
		{"server error", func(q *HTTPQueue) error { _, err := q.Inbox(context.Background(), testLease); return err },
			answer{502, `bad gateway`}, func(err error) bool {
				return err != nil && !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrRejected)
			}},
	}
	for _, c := range cases {
		q, _ := newPlatform(t, func(string, map[string]any) (int, string) { return c.ans.status, c.ans.body })
		if err := c.call(q); !c.want(err) {
			t.Errorf("%s: error = %v", c.name, err)
		}
	}
}
