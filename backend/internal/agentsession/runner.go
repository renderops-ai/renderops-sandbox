// Package agentsession runs agent sessions (v1: Claude Code coding agents) for
// the platform. A Pool of runners inside the control plane claims sessions from
// the platform's queue (/api/internal/agent-sessions/*), prepares each
// session's sandbox (Runtime), starts one Claude Code turn per user message in
// the guest, and streams the guest's event journal back as session events.
//
// The runner never runs repository code or agent binaries on the host: every
// turn runs detached inside the session's Firecracker sandbox and survives the
// runner. The guest journal plus the platform's runtime cursor are the only
// state, so another runner can take over at any time. See
// AGENT_SESSION_EXECUTION.md for the contract.
package agentsession

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrCancelled is the work-context cause when the user ended the session.
var ErrCancelled = errors.New("agent session cancelled by the user")

// Session is one claimed agent session.
type Session struct {
	ID                 string
	OwnerUserID        string
	ProjectID          string
	SourceKind         string // issue, pull_request or prompt
	SourceNumber       int64
	Title              string
	Agent              string
	Status             string
	SandboxID          string
	CancelRequested    bool
	InterruptRequested bool
	RuntimeCursor      int64
	DeliveredSeq       int64
	LeaseToken         string
	LeaseExpiresAt     time.Time
	// Instructions are the platform's agent instructions; nil means the
	// runner's generic fallback (an older platform, or invalid ones).
	Instructions *Instructions
}

// Lease identifies the claim on a session. Never log the token.
type Lease struct {
	SessionID string
	Token     string
}

// Lease returns the session's lease.
func (s *Session) Lease() Lease { return Lease{SessionID: s.ID, Token: s.LeaseToken} }

// Heartbeat is what the user asked for since the last heartbeat.
type Heartbeat struct {
	CancelRequested    bool
	InterruptRequested bool
	Status             string
	LeaseExpiresAt     time.Time
}

// Message is a user message for the agent; Seq is its session event seq.
type Message struct {
	Seq  int64
	Text string
}

// CursorRange is the journal byte range an event batch covers.
type CursorRange struct {
	From, To int64
}

// WorkspaceGrant is the session's pinned checkout and a short-lived,
// read-only token for the worker's git proxy. The token never enters the guest.
type WorkspaceGrant struct {
	Repository string
	HeadSHA    string
	BaseSHA    string
	Token      string
	ExpiresAt  time.Time
}

// Queue is the platform's internal agent-session API (HTTPQueue).
type Queue interface {
	Claim(ctx context.Context, workerID string, lease time.Duration) (*Session, error)
	Heartbeat(ctx context.Context, l Lease, lease time.Duration) (Heartbeat, error)
	Append(ctx context.Context, l Lease, cursor *CursorRange, events []Event) error
	Inbox(ctx context.Context, l Lease) ([]Message, error)
	Ack(ctx context.Context, l Lease, seq int64) error
	Release(ctx context.Context, l Lease, idle bool) error
}

// EmitFunc appends runner-origin events (no journal cursor).
type EmitFunc func(ctx context.Context, events ...Event) error

// Runtime prepares and finishes a session's sandbox (SandboxRuntime).
type Runtime interface {
	// Ensure makes the session's sandbox, model access, guest files and
	// workspace ready. It is idempotent and safe after any crash.
	Ensure(ctx context.Context, s *Session, emit EmitFunc) (Guest, error)
	// Finish ends the session: it stops any turn and pauses the sandbox. It
	// returns the ports the session last showed as open (pausing closes them).
	Finish(ctx context.Context, s *Session) ([]Port, error)
}

// Guest is one session's agent inside its sandbox.
type Guest interface {
	// Poll reports what changed in the guest since cursor and the ports
	// signature: journal data, whether a turn is running, listening ports.
	Poll(ctx context.Context, cursor int64, portsSig string) (*PollResult, error)
	// Deliver starts the turn for user message seq (idempotent per seq).
	Deliver(ctx context.Context, seq int64, prompt string) error
	// Stop interrupts the running turn, if any.
	Stop(ctx context.Context) error
	// RecordPorts remembers which ports the session shows as open, so a
	// later claim can close them after a pause.
	RecordPorts(ctx context.Context, ports []Port) error
}

// PollResult is one guest poll. Changed is false when nothing changed (the
// poll script exited 10); the other fields are then empty.
type PollResult struct {
	Changed bool
	Size    int64
	Alive   bool
	// LastTurn is the highest seq a turn was ever started for.
	LastTurn int64
	PortsSig string
	Ports    []Port
	// Data is journal bytes from offset From; Full means it was cut at the
	// poll's read limit.
	From int64
	Data []byte
	Full bool
	// Resumed: the sandbox was resumed since the last poll; servers from
	// before the pause may be gone, so every known port is closed and the
	// ports are published again.
	Resumed bool
	// Previous (first poll of a claim only): the ports an earlier claim
	// published as open.
	Previous []Port
	// LastTurnEnd (first poll of a claim only): the last turn_end in the
	// journal when the session was claimed, which may precede the committed
	// cursor; nil when no turn has ended.
	LastTurnEnd *TurnEnd
}

// Port is a listening TCP socket in the guest.
type Port struct {
	Port int
	Bind string
}

const (
	defaultLease             = 60 * time.Second
	defaultHeartbeatInterval = 15 * time.Second
	defaultPollInterval      = 2 * time.Second
	defaultTurnTimeout       = 45 * time.Minute

	maxBatchEvents = 100
	maxBatchBytes  = 200 << 10 // the platform refuses requests over 256 KB
	maxPromptBytes = 64 << 10

	maxPollFailures    = 20
	maxDeliverFailures = 5
	maxEnsureAttempts  = 5
	// maxInterruptHold bounds how long messages sent before a Stop are held
	// back; see holdAfterStop.
	maxInterruptHold = 15 * time.Minute
	// turnGuardMargin: if the guest's own timeout never fires, the runner
	// stops a turn this long after the turn timeout.
	turnGuardMargin = 5 * time.Minute
	finishTimeout   = 12 * time.Minute
	cleanupTimeout  = 30 * time.Second
)

// Runner drives one claimed session at a time (see Pool).
type Runner struct {
	Queue   Queue
	Runtime Runtime
	// WorkerID names this runner in the platform's logs.
	WorkerID string

	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	TurnTimeout       time.Duration
	// RepoDir is stripped from paths in published events.
	RepoDir string

	// Tests shorten retry waits and the turn guard.
	backoffUnit time.Duration
	guardMargin time.Duration
}

// backoff is the wait before retry number failures.
func (r *Runner) backoff(failures int) time.Duration {
	unit := orDefault(r.backoffUnit, time.Second)
	return min(unit<<min(failures, 4), 15*unit)
}

func (r *Runner) lease() time.Duration { return orDefault(r.LeaseDuration, defaultLease) }
func (r *Runner) heartbeatInterval() time.Duration {
	return orDefault(r.HeartbeatInterval, defaultHeartbeatInterval)
}
func (r *Runner) pollInterval() time.Duration { return orDefault(r.PollInterval, defaultPollInterval) }
func (r *Runner) turnTimeout() time.Duration  { return orDefault(r.TurnTimeout, defaultTurnTimeout) }

func orDefault(d, fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return d
}

// control carries what heartbeats learn to the session loop.
type control struct {
	mu        sync.Mutex
	cancel    bool
	interrupt bool
	// interruptAt is when the request that reported interrupt was sent; a
	// report sent before an ack is stale (the ack clears the flag).
	interruptAt time.Time
}

func (c *control) set(hb Heartbeat, sentAt time.Time) (newCancel bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newCancel = hb.CancelRequested && !c.cancel
	c.cancel = c.cancel || hb.CancelRequested
	c.interrupt = hb.InterruptRequested
	c.interruptAt = sentAt
	return newCancel
}

func (c *control) cancelled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancel
}

// interrupted reports a Stop request observed after since.
func (c *control) interrupted(since time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.interrupt && !c.interruptAt.Before(since)
}

// RunOne claims and drives at most one session. It returns false when there
// was nothing to claim.
func (r *Runner) RunOne(ctx context.Context) (bool, error) {
	if r.Queue == nil || r.Runtime == nil {
		return false, errors.New("agent session runner needs a queue and a runtime")
	}
	claimedAt := time.Now()
	s, err := r.Queue.Claim(ctx, r.WorkerID, r.lease())
	if err != nil {
		return false, fmt.Errorf("claim agent session: %w", err)
	}
	if s == nil {
		return false, nil
	}
	log := slog.With("component", "agentsession", "session_id", s.ID, "worker_id", r.WorkerID)
	log.Info("claimed agent session", "status", s.Status, "sandbox_id", s.SandboxID,
		"cancel_requested", s.CancelRequested, "runtime_cursor", s.RuntimeCursor,
		"instructions_version", clean(s.instructionsVersion(), maxVersionBytes))

	ctl := &control{}
	ctl.set(Heartbeat{CancelRequested: s.CancelRequested, InterruptRequested: s.InterruptRequested}, claimedAt)

	// Heartbeats outlive the work context, so the lease is kept while a
	// cancelled session is being finished.
	hbCtx, stopHeartbeats := context.WithCancel(ctx)
	work, cancelWork := context.WithCancelCause(hbCtx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		r.heartbeat(hbCtx, cancelWork, s, ctl, claimedAt, log)
	}()
	err = r.drive(ctx, work, s, ctl, log)
	cancelWork(nil)
	stopHeartbeats()
	<-hbDone
	if err != nil {
		return true, fmt.Errorf("agent session %s: %w", s.ID, err)
	}
	return true, nil
}

func (r *Runner) heartbeat(ctx context.Context, cancelWork context.CancelCauseFunc, s *Session, ctl *control, start time.Time, log *slog.Logger) {
	interval := r.heartbeatInterval()
	expires := start.Add(r.lease())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		sentAt := time.Now()
		hbCtx, cancel := context.WithTimeout(ctx, interval)
		hb, err := r.Queue.Heartbeat(hbCtx, s.Lease(), r.lease())
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if errors.Is(err, ErrLeaseLost) || !time.Now().Before(expires) {
				log.Warn("agent session lease lost", "err", err)
				cancelWork(ErrLeaseLost)
				return
			}
			log.Warn("agent session heartbeat failed; retrying", "err", err)
			continue
		}
		// The platform's clock decides; ours only needs a safe estimate.
		expires = sentAt.Add(r.lease())
		if isFinal(hb.Status) {
			log.Warn("agent session ended elsewhere; stopping", "status", hb.Status)
			cancelWork(ErrLeaseLost)
			return
		}
		if ctl.set(hb, sentAt) {
			log.Info("user ended the agent session")
			cancelWork(ErrCancelled)
		}
	}
}

func isFinal(status string) bool {
	return status == "done" || status == "failed" || status == "cancelled"
}

// tracker is the session loop's view of the guest.
type tracker struct {
	decoder   *Decoder
	committed int64
	pending   []Released
	size      int64
	alive     bool
	more      bool // the last poll stopped at its read limit
	lastTurn  int64
	portsSig  string
	// ports are the ports the session shows as open (published to the
	// platform); baselined once an earlier claim's are known.
	ports      map[int]string
	baselined  bool
	resetPorts bool
	// For the next turn's notices: the sandbox was resumed since the last
	// turn was delivered, and how the last finished turn ended (nil once a
	// delivered turn carried it).
	resumedSinceTurn bool
	lastEnd          *TurnEnd

	aliveSince     time.Time
	stopRequested  bool
	lastAck        time.Time
	heldSeq        int64
	holdingSince   time.Time
	pollFailures   int
	deliverFailure int
}

// drive runs the session until its turn work is done (released idle), it is
// cancelled, it fails, the lease is lost or the runner shuts down.
func (r *Runner) drive(parent, ctx context.Context, s *Session, ctl *control, log *slog.Logger) error {
	lease := s.Lease()
	emit := func(ctx context.Context, events ...Event) error {
		return r.Queue.Append(ctx, lease, nil, events)
	}
	if s.Agent != "" && s.Agent != "claude-code" {
		return r.fail(ctx, s, fmt.Sprintf("This runner only supports Claude Code sessions (got %q).", clean(s.Agent, 40)),
			fmt.Errorf("unsupported agent %q", s.Agent), log)
	}
	t := &tracker{
		decoder:   NewDecoder(s.RuntimeCursor, DecoderOptions{RepoDir: r.RepoDir, TurnTimeout: r.turnTimeout()}),
		committed: s.RuntimeCursor,
		ports:     map[int]string{},
	}
	if ctl.cancelled() {
		return r.finish(parent, s, t, log)
	}

	guest, err := r.ensure(ctx, s, emit, log)
	if err != nil {
		return r.stopped(parent, ctx, s, t, err, log)
	}

	for {
		if err := ctx.Err(); err != nil {
			return r.stopped(parent, ctx, s, t, err, log)
		}
		r.honorInterrupt(ctx, guest, t, ctl, log)

		res, err := guest.Poll(ctx, t.decoder.Offset(), t.portsSig)
		if err != nil {
			if ctx.Err() != nil {
				return r.stopped(parent, ctx, s, t, ctx.Err(), log)
			}
			t.pollFailures++
			if t.pollFailures >= maxPollFailures || errors.Is(err, errJournalLost) {
				return r.fail(ctx, s, userMessage(err, "Lost contact with the sandbox."), err, log)
			}
			log.Warn("guest poll failed; retrying", "failures", t.pollFailures, "err", err)
			if err := sleep(ctx, r.backoff(t.pollFailures)); err != nil {
				return r.stopped(parent, ctx, s, t, err, log)
			}
			continue
		}
		t.pollFailures = 0
		t.more = false
		var publishErr error
		switch {
		case res.Changed:
			publishErr = r.absorb(ctx, s, guest, t, res, log)
		case len(t.pending) > 0:
			publishErr = r.commit(ctx, s, t, log) // retry what a failed append left
		}
		if publishErr != nil {
			if fatal := r.queueError(publishErr); fatal != nil {
				return r.stopped(parent, ctx, s, t, fatal, log)
			}
			log.Warn("publishing agent events failed; retrying", "err", publishErr)
			if err := sleep(ctx, r.pollInterval()); err != nil {
				return r.stopped(parent, ctx, s, t, err, log)
			}
			continue
		}

		if t.more {
			continue // the poll was cut at its read limit: read on right away
		}
		if t.alive {
			if err := sleep(ctx, r.pollInterval()); err != nil {
				return r.stopped(parent, ctx, s, t, err, log)
			}
			continue
		}

		// No turn is running and the whole journal is decoded.
		t.decoder.Flush()
		if err := r.commit(ctx, s, t, log); err != nil {
			if fatal := r.queueError(err); fatal != nil {
				return r.stopped(parent, ctx, s, t, fatal, log)
			}
			continue
		}
		done, err := r.idle(ctx, s, guest, t, ctl, log)
		if err != nil {
			if fatal := r.queueError(err); fatal != nil {
				return r.stopped(parent, ctx, s, t, fatal, log)
			}
			if errors.Is(err, errDeliverFailed) {
				return r.fail(ctx, s, userMessage(err, "The agent could not be started in the sandbox."), err, log)
			}
			log.Warn("agent session inbox step failed; retrying", "err", err)
			if err := sleep(ctx, r.pollInterval()); err != nil {
				return r.stopped(parent, ctx, s, t, err, log)
			}
			continue
		}
		if done {
			log.Info("agent session is waiting for the user")
			return nil
		}
	}
}

var errDeliverFailed = errors.New("deliver failed")

// idle runs when no turn is running: deliver waiting messages as the next
// turn, or release the session to wait for the user. done means released.
func (r *Runner) idle(ctx context.Context, s *Session, guest Guest, t *tracker, ctl *control, log *slog.Logger) (bool, error) {
	lease := s.Lease()
	messages, err := r.Queue.Inbox(ctx, lease)
	if err != nil {
		return false, err
	}
	// Messages a turn was already started for (a runner died before acking).
	var fresh []Message
	var started int64
	for _, m := range messages {
		if m.Seq <= t.lastTurn {
			started = max(started, m.Seq)
			continue
		}
		fresh = append(fresh, m)
	}
	if started > 0 {
		if err := r.Queue.Ack(ctx, lease, started); err != nil {
			return false, err
		}
		t.lastAck = time.Now()
	}
	if len(fresh) == 0 {
		if err := r.Queue.Release(ctx, lease, true); err != nil {
			if errors.Is(err, ErrPending) {
				return false, nil // a message just arrived; deliver it next round
			}
			return false, err
		}
		return true, nil
	}
	if r.holdAfterStop(ctx, t, ctl, fresh, log) {
		return false, sleep(ctx, r.pollInterval())
	}

	seq, prompt := combine(fresh)
	// Notices are about the previous turn, so the first turn gets none.
	var notice string
	var notices []string
	if t.lastTurn > 0 {
		notice, notices = turnNotice(s.notices(), t.resumedSinceTurn, t.lastEnd)
	}
	if err := guest.Deliver(ctx, seq, notice+prompt); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		t.deliverFailure++
		if t.deliverFailure >= maxDeliverFailures {
			return false, fmt.Errorf("%w: %w", errDeliverFailed, err)
		}
		return false, fmt.Errorf("deliver turn %d: %w", seq, err)
	}
	t.deliverFailure = 0
	// The prompt file is written: these events are never noticed again.
	t.resumedSinceTurn, t.lastEnd = false, nil
	if err := r.Queue.Ack(ctx, lease, seq); err != nil {
		// The turn runs; a later claim acks it from the guest's turn record.
		return false, err
	}
	log.Info("started agent turn", "seq", seq, "messages", len(fresh), "notices", notices)
	t.lastAck = time.Now()
	t.lastTurn = max(t.lastTurn, seq)
	t.alive = true
	t.aliveSince = time.Now()
	t.stopRequested = false
	t.heldSeq, t.holdingSince = 0, time.Time{}
	return false, nil
}

// holdAfterStop: after Stop, messages the user sent before stopping are held
// until a new message arrives or the platform clears the request, so the
// stopped work does not resume on its own. Delivering acks, which clears the
// request. The hold is bounded so a session cannot stay leased forever.
func (r *Runner) holdAfterStop(ctx context.Context, t *tracker, ctl *control, fresh []Message, log *slog.Logger) bool {
	if !ctl.interrupted(t.lastAck) {
		t.heldSeq, t.holdingSince = 0, time.Time{}
		return false
	}
	newest := fresh[len(fresh)-1].Seq
	if t.heldSeq == 0 {
		t.heldSeq, t.holdingSince = newest, time.Now()
		log.Info("holding messages sent before Stop", "seq", newest)
		return true
	}
	if newest > t.heldSeq || time.Since(t.holdingSince) > maxInterruptHold {
		return false
	}
	return true
}

// honorInterrupt stops the running turn when the user asked to (or when the
// guest's own turn timeout did not fire).
func (r *Runner) honorInterrupt(ctx context.Context, guest Guest, t *tracker, ctl *control, log *slog.Logger) {
	if !t.alive || t.stopRequested {
		return
	}
	reason := ""
	switch {
	case ctl.interrupted(t.lastAck):
		reason = "user stop"
	case !t.aliveSince.IsZero() && time.Since(t.aliveSince) > r.turnTimeout()+orDefault(r.guardMargin, turnGuardMargin):
		reason = "turn time limit"
	default:
		return
	}
	t.stopRequested = true
	log.Info("stopping agent turn", "reason", reason, "seq", t.lastTurn)
	if err := guest.Stop(ctx); err != nil {
		// The next poll still sees the turn; a later heartbeat retries.
		t.stopRequested = false
		log.Warn("stopping agent turn failed", "err", err)
	}
}

// absorb takes one poll's changes: journal events and port changes.
func (r *Runner) absorb(ctx context.Context, s *Session, guest Guest, t *tracker, res *PollResult, log *slog.Logger) error {
	if res.Size < t.decoder.Offset() {
		return fmt.Errorf("%w: journal is %d bytes, behind the cursor at %d", errJournalLost, res.Size, t.decoder.Offset())
	}
	if res.Alive && !t.alive {
		t.aliveSince = time.Now()
	}
	if !res.Alive {
		t.stopRequested = false
	}
	t.size, t.alive = res.Size, res.Alive
	t.lastTurn = max(t.lastTurn, res.LastTurn)
	// Keep what this poll says about ports before any call can fail.
	if res.Previous != nil && !t.baselined {
		t.ports, t.baselined = portMap(res.Previous), true
	}
	if res.Resumed {
		t.resetPorts = true
		t.resumedSinceTurn = true
	}
	// The claim-time turn end first: turn ends decoded below are later.
	if res.LastTurnEnd != nil {
		end := *res.LastTurnEnd
		t.lastEnd = &end
	}
	if len(res.Data) > 0 {
		if _, err := t.decoder.Feed(res.From, res.Data, res.Full); err != nil {
			return fmt.Errorf("%w: %w", errJournalLost, err)
		}
		t.more = res.Full
		if end, ok := t.decoder.TakeTurnEnd(); ok {
			t.lastEnd = &end
		}
	}
	if err := r.publishPorts(ctx, s, guest, t, res, log); err != nil {
		return err
	}
	return r.commit(ctx, s, t, log)
}

// publishPorts turns port changes into preview events. After a resume every
// known port is closed first (servers may not have survived the pause) and
// the listening ones are opened again.
func (r *Runner) publishPorts(ctx context.Context, s *Session, guest Guest, t *tracker, res *PollResult, log *slog.Logger) error {
	if res.PortsSig == "" || (res.PortsSig == t.portsSig && !t.resetPorts) {
		return nil
	}
	before := t.ports
	var events []Event
	if t.resetPorts {
		events = diffPorts(before, nil)
		before = map[int]string{}
	}
	events = append(events, diffPorts(before, res.Ports)...)
	published := len(events) > 0
	if published {
		err := r.Queue.Append(ctx, s.Lease(), nil, events)
		switch {
		case errors.Is(err, ErrRejected):
			// A platform without preview events: drop them.
			log.Warn("platform refused preview events; dropping them", "err", err)
			published = false
		case err != nil:
			t.portsSig = "" // the next poll reports every port again
			return err
		}
	}
	t.ports, t.portsSig, t.resetPorts = portMap(res.Ports), res.PortsSig, false
	if published {
		if err := guest.RecordPorts(ctx, sortedPorts(t.ports)); err != nil {
			log.Warn("recording published ports in the guest failed", "err", err)
		}
	}
	return nil
}

func sortedPorts(m map[int]string) []Port {
	ports := make([]Port, 0, len(m))
	for port, bind := range m {
		ports = append(ports, Port{Port: port, Bind: bind})
	}
	sort.Slice(ports, func(i, j int) bool { return ports[i].Port < ports[j].Port })
	return ports
}

var errJournalLost = errors.New("the sandbox's agent journal does not match this session")

// commit appends decoded events with their journal cursor range, in batches
// the platform accepts. Lines without events are committed with the next
// batch that has some.
func (r *Runner) commit(ctx context.Context, s *Session, t *tracker, log *slog.Logger) error {
	for _, released := range t.decoder.Take() {
		// Consecutive lines without events only move the cursor.
		if n := len(t.pending); n > 0 && len(released.Events) == 0 && len(t.pending[n-1].Events) == 0 {
			t.pending[n-1].End = released.End
			continue
		}
		t.pending = append(t.pending, released)
	}
	for {
		b, used := nextBatch(t.pending)
		if used == 0 {
			return nil
		}
		err := r.Queue.Append(ctx, s.Lease(), &CursorRange{From: t.committed, To: b.to}, b.events)
		if errors.Is(err, ErrRejected) {
			// An event the platform will never take must not stall the
			// session; the next batch's range covers these lines.
			log.Error("platform refused agent events; skipping them", "cursor_to", b.to, "err", err)
		} else if err != nil {
			return err
		} else {
			t.committed = b.to
		}
		t.pending = t.pending[used:]
	}
}

type batch struct {
	to     int64
	events []Event
}

// nextBatch takes whole lines from groups into one batch of 1..100 events and
// at most maxBatchBytes, extended over following lines without events. used
// is 0 when no line with events is left.
func nextBatch(groups []Released) (batch, int) {
	var b batch
	size := 0
	used := 0
	for i, g := range groups {
		if len(g.Events) > 0 && len(b.events) > 0 {
			gsize := eventsSize(g.Events)
			if len(b.events)+len(g.Events) > maxBatchEvents || size+gsize > maxBatchBytes {
				break
			}
		}
		for _, ev := range g.Events {
			n := eventsSize([]Event{ev})
			if len(b.events) >= maxBatchEvents || (len(b.events) > 0 && size+n > maxBatchBytes) {
				break // one line larger than a batch: the rest of it is dropped
			}
			b.events = append(b.events, ev)
			size += n
		}
		b.to = g.End
		used = i + 1
	}
	if len(b.events) == 0 {
		return batch{}, 0
	}
	return b, used
}

func eventsSize(events []Event) int {
	raw, err := marshalJSON(events)
	if err != nil {
		return maxBatchBytes
	}
	return len(raw)
}

// queueError returns the error that must end the session loop, or nil for a
// transient one worth retrying.
func (r *Runner) queueError(err error) error {
	var conflict *CursorConflictError
	switch {
	case errors.Is(err, ErrLeaseLost), errors.As(err, &conflict):
		return ErrLeaseLost
	case errors.Is(err, errJournalLost), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, ErrQueueAuth):
		return err
	}
	return nil
}

// stopped decides what an interrupted session loop leaves behind.
func (r *Runner) stopped(parent, ctx context.Context, s *Session, t *tracker, err error, log *slog.Logger) error {
	cause := context.Cause(ctx)
	switch {
	case errors.Is(err, ErrLeaseLost), errors.Is(cause, ErrLeaseLost):
		// Another runner may own the session now: leave the guest alone.
		log.Warn("stopped driving agent session: lease lost")
		return nil
	case errors.Is(cause, ErrCancelled) && parent.Err() == nil:
		return r.finish(parent, s, t, log)
	case parent.Err() != nil:
		// Shutdown: the turn keeps running in the guest and the lease
		// expires; the next runner picks the session up.
		log.Info("runner shutting down; leaving agent session to the next runner")
		return nil
	case errors.Is(err, ErrQueueAuth):
		return err
	}
	return r.fail(ctx, s, userMessage(err, "Spawn could not run this session."), err, log)
}

// finish ends a cancelled session: stop the turn, pause the sandbox, close
// the ports it showed, mark it cancelled and give the lease back.
func (r *Runner) finish(parent context.Context, s *Session, t *tracker, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), finishTimeout)
	defer cancel()
	recorded, err := r.Runtime.Finish(ctx, s)
	if err != nil {
		log.Warn("finishing the sandbox of a cancelled session failed", "err", err)
	}
	known := map[int]string{}
	for port, bind := range portMap(recorded) {
		known[port] = bind
	}
	for port, bind := range t.ports {
		known[port] = bind
	}
	if closes := diffPorts(known, nil); len(closes) > 0 {
		err := r.Queue.Append(ctx, s.Lease(), nil, closes)
		if errors.Is(err, ErrLeaseLost) {
			log.Warn("lease lost before the cancellation was recorded")
			return nil
		}
		if err != nil {
			log.Warn("closing the session's previews failed", "err", err)
		}
	}
	err = r.Queue.Append(ctx, s.Lease(), nil, []Event{{Type: "status", Actor: "system", Payload: map[string]any{"status": "cancelled"}}})
	if errors.Is(err, ErrLeaseLost) {
		log.Warn("lease lost before the cancellation was recorded")
		return nil
	}
	if err != nil {
		return fmt.Errorf("record cancellation: %w", err)
	}
	// The final status already dropped the lease; release is a no-op then.
	if err := r.Queue.Release(ctx, s.Lease(), false); err != nil && !errors.Is(err, ErrLeaseLost) {
		log.Warn("release after cancellation failed", "err", err)
	}
	log.Info("agent session cancelled")
	return nil
}

// fail ends the session with an error event: an infrastructure failure the
// session cannot continue from. message is shown to the user.
func (r *Runner) fail(ctx context.Context, s *Session, message string, cause error, log *slog.Logger) error {
	log.Error("agent session failed", "err", cause)
	appendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	err := r.Queue.Append(appendCtx, s.Lease(), nil, []Event{{Type: "error", Actor: "system", Payload: map[string]any{"message": message}}})
	if err != nil && !errors.Is(err, ErrLeaseLost) {
		return fmt.Errorf("record failure (%v): %w", cause, err)
	}
	return cause
}

// ensure runs Runtime.Ensure, retrying while the sandbox is busy elsewhere.
func (r *Runner) ensure(ctx context.Context, s *Session, emit EmitFunc, log *slog.Logger) (Guest, error) {
	for attempt := 1; ; attempt++ {
		guest, err := r.Runtime.Ensure(ctx, s, emit)
		if err == nil {
			return guest, nil
		}
		if attempt >= maxEnsureAttempts || !isTransient(err) || ctx.Err() != nil {
			return nil, err
		}
		log.Warn("preparing the session sandbox failed; retrying", "attempt", attempt, "err", err)
		if err := sleep(ctx, r.backoff(attempt)*2); err != nil {
			return nil, err
		}
	}
}

// combine joins the waiting messages into one prompt; seq is the last
// message included (the turn's id and the ack).
func combine(messages []Message) (int64, string) {
	sorted := append([]Message(nil), messages...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })
	var b strings.Builder
	var seq int64
	for i, m := range sorted {
		text := strings.TrimSpace(m.Text)
		if i > 0 && b.Len()+len(text)+2 > maxPromptBytes {
			break
		}
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(text)
		seq = m.Seq
	}
	prompt := b.String()
	if len(prompt) > maxPromptBytes {
		prompt = clean(prompt, maxPromptBytes)
	}
	return seq, prompt
}

// diffPorts turns a change in listening ports into preview events. VS Code's
// port and loopback-only ephemeral listeners (test servers) are hidden.
func diffPorts(before map[int]string, now []Port) []Event {
	after := portMap(now)
	var events []Event
	keys := make([]int, 0, len(before)+len(after))
	for p := range after {
		keys = append(keys, p)
	}
	for p := range before {
		if _, ok := after[p]; !ok {
			keys = append(keys, p)
		}
	}
	sort.Ints(keys)
	for _, p := range keys {
		bind, open := after[p]
		old, was := before[p]
		switch {
		case open && (!was || old != bind):
			events = append(events, previewEvent(p, true, bind))
		case !open && was:
			events = append(events, previewEvent(p, false, old))
		}
	}
	return events
}

const idePort = 3001

func portMap(ports []Port) map[int]string {
	m := map[int]string{}
	for _, p := range ports {
		if p.Port == idePort || p.Port <= 0 || (isLoopback(p.Bind) && p.Port >= 32768) {
			continue
		}
		// One server often listens on both 0.0.0.0 and ::; prefer the
		// widest bind.
		if old, ok := m[p.Port]; ok && bindRank(old) >= bindRank(p.Bind) {
			continue
		}
		m[p.Port] = p.Bind
	}
	return m
}

func bindRank(bind string) int {
	switch {
	case bind == "0.0.0.0" || bind == "::":
		return 2
	case isLoopback(bind):
		return 0
	}
	return 1
}

func isLoopback(bind string) bool {
	return strings.HasPrefix(bind, "127.") || bind == "::1"
}

func previewEvent(port int, open bool, bind string) Event {
	return Event{Type: "preview", Actor: "system", Payload: map[string]any{"port": port, "open": open, "bind": bind}}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Pool runs Concurrency runners until its context ends.
type Pool struct {
	Runner      Runner
	Concurrency int
	// IdleInterval is the wait after an empty claim; ErrorInterval after a
	// failed one.
	IdleInterval  time.Duration
	ErrorInterval time.Duration
}

// Run blocks until ctx is done and every runner has stopped. On shutdown no
// new session is claimed, heartbeats stop and running guest turns are left
// alone: their leases expire and another runner continues them.
func (p *Pool) Run(ctx context.Context) {
	n := max(p.Concurrency, 1)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		runner := p.Runner
		runner.WorkerID = fmt.Sprintf("%s-%d", p.Runner.WorkerID, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx, &runner)
		}()
	}
	slog.Info("agent session runner started", "component", "agentsession", "concurrency", n)
	wg.Wait()
	slog.Info("agent session runner stopped", "component", "agentsession")
}

func (p *Pool) loop(ctx context.Context, r *Runner) {
	idle := orDefault(p.IdleInterval, 5*time.Second)
	failed := orDefault(p.ErrorInterval, 15*time.Second)
	claimErrors := 0
	for ctx.Err() == nil {
		worked, err := r.RunOne(ctx)
		wait := time.Duration(0)
		switch {
		case err != nil && !worked:
			claimErrors++
			if claimErrors == 1 || claimErrors%20 == 0 {
				slog.Warn("agent session claim failed", "component", "agentsession", "worker_id", r.WorkerID,
					"failures", claimErrors, "err", err)
			}
			wait = failed
		case err != nil:
			claimErrors = 0
			slog.Error("agent session ended with an error", "component", "agentsession", "worker_id", r.WorkerID, "err", err)
		case !worked:
			claimErrors = 0
			wait = idle
		default:
			claimErrors = 0
		}
		if wait > 0 {
			if sleep(ctx, wait) != nil {
				return
			}
		}
	}
}
