package agentsession

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrLeaseLost means the platform no longer accepts this runner's lease
	// (409): stop working on the session without touching its sandbox.
	ErrLeaseLost = errors.New("agent session lease lost")
	// ErrPending means release was refused because user messages are waiting.
	ErrPending = errors.New("agent session has undelivered messages")
	// ErrQueueAuth means the platform refused AGENT_WORKER_TOKEN, or has none
	// configured (its internal API then answers 404 everywhere).
	ErrQueueAuth = errors.New("platform refused the agent worker token")
	// ErrRejected means the platform refused a request as invalid (400).
	ErrRejected = errors.New("platform rejected the request")
)

// CursorConflictError is a 409 on an event append whose cursorFrom is not the
// session's runtime cursor: another runner moved it, so this one must stop.
type CursorConflictError struct {
	RuntimeCursor int64
}

func (e *CursorConflictError) Error() string {
	return fmt.Sprintf("runtime cursor conflict (platform is at %d)", e.RuntimeCursor)
}

// WorkspaceGrantError is a refused workspace-grant (403: repository access was
// revoked). Message is the platform's user-facing explanation.
type WorkspaceGrantError struct {
	Status  int
	Message string
}

func (e *WorkspaceGrantError) Error() string {
	return fmt.Sprintf("workspace grant refused (%d): %s", e.Status, e.Message)
}

const (
	queueTimeout    = 30 * time.Second
	maxResponseBody = 1 << 20
)

// HTTPQueue is the client for the platform's internal agent-session API
// (/api/internal/agent-sessions/*), authenticated with AGENT_WORKER_TOKEN.
type HTTPQueue struct {
	base   string
	token  string
	client *http.Client
}

// NewHTTPQueue returns a client for the platform at platformURL
// (PLATFORM_INTERNAL_URL).
func NewHTTPQueue(platformURL, token string) *HTTPQueue {
	return &HTTPQueue{
		base:   strings.TrimRight(platformURL, "/") + "/api/internal/agent-sessions",
		token:  token,
		client: &http.Client{Timeout: queueTimeout},
	}
}

type wireSession struct {
	ID                 string   `json:"id"`
	OwnerUserID        string   `json:"ownerUserId"`
	ProjectID          string   `json:"projectId"`
	SourceKind         string   `json:"sourceKind"`
	SourceNumber       flexInt  `json:"sourceNumber"`
	Title              string   `json:"title"`
	Agent              string   `json:"agent"`
	Status             string   `json:"status"`
	SandboxID          string   `json:"sandboxId"`
	CancelRequested    bool     `json:"cancelRequested"`
	InterruptRequested bool     `json:"interruptRequested"`
	RuntimeCursor      flexInt  `json:"runtimeCursor"`
	DeliveredSeq       flexInt  `json:"deliveredSeq"`
	LeaseToken         string   `json:"leaseToken"`
	LeaseExpiresAt     flexTime `json:"leaseExpiresAt"`
}

// Claim asks for the next session that needs a runner; nil when there is none.
func (q *HTTPQueue) Claim(ctx context.Context, workerID string, lease time.Duration) (*Session, error) {
	var out struct {
		Session *wireSession `json:"session"`
	}
	status, err := q.post(ctx, "/claim", map[string]any{"workerId": workerID, "leaseSeconds": leaseSeconds(lease)}, &out)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent || out.Session == nil {
		return nil, nil
	}
	w := out.Session
	if _, err := uuid.Parse(w.ID); err != nil {
		return nil, fmt.Errorf("claimed session has an invalid id %q", w.ID)
	}
	if _, err := uuid.Parse(w.LeaseToken); err != nil {
		return nil, errors.New("claimed session has no valid lease token")
	}
	return &Session{
		ID:                 w.ID,
		OwnerUserID:        w.OwnerUserID,
		ProjectID:          w.ProjectID,
		SourceKind:         w.SourceKind,
		SourceNumber:       int64(w.SourceNumber),
		Title:              w.Title,
		Agent:              w.Agent,
		Status:             w.Status,
		SandboxID:          w.SandboxID,
		CancelRequested:    w.CancelRequested,
		InterruptRequested: w.InterruptRequested,
		RuntimeCursor:      int64(w.RuntimeCursor),
		DeliveredSeq:       int64(w.DeliveredSeq),
		LeaseToken:         w.LeaseToken,
		LeaseExpiresAt:     time.Time(w.LeaseExpiresAt),
	}, nil
}

// Heartbeat renews the lease for another lease period.
func (q *HTTPQueue) Heartbeat(ctx context.Context, l Lease, lease time.Duration) (Heartbeat, error) {
	var out struct {
		CancelRequested    bool     `json:"cancelRequested"`
		InterruptRequested bool     `json:"interruptRequested"`
		Status             string   `json:"status"`
		LeaseExpiresAt     flexTime `json:"leaseExpiresAt"`
	}
	if _, err := q.post(ctx, q.sessionPath(l, "/heartbeat"), map[string]any{"leaseToken": l.Token, "leaseSeconds": leaseSeconds(lease)}, &out); err != nil {
		return Heartbeat{}, err
	}
	return Heartbeat{
		CancelRequested:    out.CancelRequested,
		InterruptRequested: out.InterruptRequested,
		Status:             out.Status,
		LeaseExpiresAt:     time.Time(out.LeaseExpiresAt),
	}, nil
}

// Append stores events. With a cursor range the platform stores them only if
// its runtime cursor is cursor.From (a retry of a stored batch succeeds);
// runner-origin events carry no cursor.
func (q *HTTPQueue) Append(ctx context.Context, l Lease, cursor *CursorRange, events []Event) error {
	body := map[string]any{"leaseToken": l.Token, "events": events}
	if cursor != nil {
		body["cursorFrom"] = cursor.From
		body["cursorTo"] = cursor.To
	}
	_, err := q.post(ctx, q.sessionPath(l, "/events"), body, nil)
	return err
}

// Inbox returns the user's messages the agent has not been given, oldest first.
func (q *HTTPQueue) Inbox(ctx context.Context, l Lease) ([]Message, error) {
	var out struct {
		Messages []struct {
			Seq  flexInt `json:"seq"`
			Text string  `json:"text"`
		} `json:"messages"`
	}
	if _, err := q.post(ctx, q.sessionPath(l, "/inbox"), map[string]any{"leaseToken": l.Token}, &out); err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		messages = append(messages, Message{Seq: int64(m.Seq), Text: m.Text})
	}
	return messages, nil
}

// Ack records that messages up to seq were handed to the agent.
func (q *HTTPQueue) Ack(ctx context.Context, l Lease, seq int64) error {
	_, err := q.post(ctx, q.sessionPath(l, "/inbox/ack"), map[string]any{"leaseToken": l.Token, "seq": seq}, nil)
	return err
}

// Release gives the session back. With idle the turn is over and the session
// waits for the user; ErrPending means a message must be delivered first.
func (q *HTTPQueue) Release(ctx context.Context, l Lease, idle bool) error {
	_, err := q.post(ctx, q.sessionPath(l, "/release"), map[string]any{"leaseToken": l.Token, "idle": idle}, nil)
	return err
}

// WorkspaceGrant returns the session's pinned commits and a short-lived,
// read-only repository token for the worker's git proxy.
func (q *HTTPQueue) WorkspaceGrant(ctx context.Context, l Lease) (WorkspaceGrant, error) {
	var out struct {
		Repository string   `json:"repository"`
		HeadSHA    string   `json:"headSha"`
		BaseSHA    string   `json:"baseSha"`
		Token      string   `json:"token"`
		ExpiresAt  flexTime `json:"expiresAt"`
	}
	if _, err := q.post(ctx, q.sessionPath(l, "/workspace-grant"), map[string]any{"leaseToken": l.Token}, &out); err != nil {
		return WorkspaceGrant{}, err
	}
	return WorkspaceGrant{
		Repository: out.Repository, HeadSHA: out.HeadSHA, BaseSHA: out.BaseSHA,
		Token: out.Token, ExpiresAt: time.Time(out.ExpiresAt),
	}, nil
}

func (q *HTTPQueue) sessionPath(l Lease, suffix string) string {
	return "/" + url.PathEscape(l.SessionID) + suffix
}

// post sends one JSON request and decodes a 2xx JSON answer into out.
func (q *HTTPQueue) post(ctx context.Context, path string, body, out any) (int, error) {
	payload, err := marshalJSON(body)
	if err != nil {
		return 0, fmt.Errorf("encode %s request: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.base+path, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("build %s request: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+q.token)
	// SvelteKit refuses cross-origin form posts; JSON is never treated as one.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := q.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("platform unreachable (%s): %w", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("read %s response: %w", path, err)
	}

	switch {
	case resp.StatusCode == http.StatusNoContent:
		return resp.StatusCode, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out != nil && len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				return resp.StatusCode, fmt.Errorf("decode %s response: %w", path, err)
			}
		}
		return resp.StatusCode, nil
	}

	var problem struct {
		Error         string   `json:"error"`
		Pending       bool     `json:"pending"`
		RuntimeCursor *flexInt `json:"runtimeCursor"`
	}
	// The error body is optional; its absence keeps the status mapping below.
	if err := json.Unmarshal(raw, &problem); err != nil {
		problem.Error = ""
	}
	message := clean(problem.Error, 300)
	switch resp.StatusCode {
	case http.StatusConflict:
		switch {
		case problem.Pending:
			return resp.StatusCode, ErrPending
		case problem.RuntimeCursor != nil:
			return resp.StatusCode, &CursorConflictError{RuntimeCursor: int64(*problem.RuntimeCursor)}
		case strings.HasSuffix(path, "/workspace-grant") && message != "" && !strings.HasPrefix(message, "Lease"):
			return resp.StatusCode, &WorkspaceGrantError{Status: resp.StatusCode, Message: message}
		default:
			return resp.StatusCode, ErrLeaseLost
		}
	case http.StatusUnauthorized:
		return resp.StatusCode, ErrQueueAuth
	case http.StatusNotFound:
		if path == "/claim" {
			return resp.StatusCode, ErrQueueAuth
		}
		// The session is gone (or the platform lost its token): stop driving it.
		return resp.StatusCode, fmt.Errorf("%w: %s answered 404", ErrLeaseLost, path)
	case http.StatusForbidden:
		if strings.HasSuffix(path, "/workspace-grant") {
			return resp.StatusCode, &WorkspaceGrantError{Status: resp.StatusCode, Message: message}
		}
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		return resp.StatusCode, fmt.Errorf("%w (%d) %s: %s", ErrRejected, resp.StatusCode, path, message)
	}
	return resp.StatusCode, fmt.Errorf("platform answered %d to %s: %s", resp.StatusCode, path, message)
}

func leaseSeconds(d time.Duration) int {
	s := int(d / time.Second)
	if s < 15 {
		s = 15
	}
	return s
}

// marshalJSON encodes without HTML escaping, so batch sizes are what we measure.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// flexInt accepts a JSON number, a numeric string (Postgres bigint) or null.
type flexInt int64

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		v, ferr := strconv.ParseFloat(s, 64)
		if ferr != nil {
			return fmt.Errorf("not an integer: %s", s)
		}
		n = int64(v)
	}
	*f = flexInt(n)
	return nil
}

// flexTime accepts RFC 3339 or Postgres timestamps, or null. Unparsable
// values read as zero: the runner keeps its own lease clock.
type flexTime time.Time

func (f *flexTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil || s == "" {
		*f = flexTime{}
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07", "2006-01-02 15:04:05.999999999Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			*f = flexTime(t)
			return nil
		}
	}
	*f = flexTime{}
	return nil
}
