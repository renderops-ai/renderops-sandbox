package agentsession

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/controlplane"
)

// fakeQueue behaves like the platform's queue for one session.
type fakeQueue struct {
	mu        sync.Mutex
	session   *Session
	claimed   bool
	messages  []Message
	delivered int64
	cancel    bool
	interrupt bool
	hbErr     error
	appendErr func(cursor *CursorRange, events []Event) error

	appends  []appendCall
	acks     []int64
	releases []bool
	inbox    int
	onInbox  func(q *fakeQueue, call int)
}

type appendCall struct {
	cursor *CursorRange
	events []Event
}

func (q *fakeQueue) Claim(context.Context, string, time.Duration) (*Session, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.claimed || q.session == nil {
		return nil, nil
	}
	q.claimed = true
	s := *q.session
	return &s, nil
}

func (q *fakeQueue) Heartbeat(context.Context, Lease, time.Duration) (Heartbeat, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.hbErr != nil {
		return Heartbeat{}, q.hbErr
	}
	return Heartbeat{CancelRequested: q.cancel, InterruptRequested: q.interrupt, Status: "running"}, nil
}

func (q *fakeQueue) Append(_ context.Context, _ Lease, cursor *CursorRange, events []Event) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.appendErr != nil {
		if err := q.appendErr(cursor, events); err != nil {
			return err
		}
	}
	q.appends = append(q.appends, appendCall{cursor: cursor, events: append([]Event(nil), events...)})
	return nil
}

func (q *fakeQueue) Inbox(context.Context, Lease) ([]Message, error) {
	q.mu.Lock()
	q.inbox++
	call, hook := q.inbox, q.onInbox
	q.mu.Unlock()
	if hook != nil {
		hook(q, call)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []Message
	for _, m := range q.messages {
		if m.Seq > q.delivered {
			out = append(out, m)
		}
	}
	return out, nil
}

func (q *fakeQueue) Ack(_ context.Context, _ Lease, seq int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acks = append(q.acks, seq)
	if seq > q.delivered {
		q.delivered = seq
		q.interrupt = false // delivering answers a Stop, as on the platform
	}
	return nil
}

func (q *fakeQueue) Release(_ context.Context, _ Lease, idle bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if idle {
		for _, m := range q.messages {
			if m.Seq > q.delivered {
				return ErrPending
			}
		}
	}
	q.releases = append(q.releases, idle)
	return nil
}

func (q *fakeQueue) set(f func(q *fakeQueue)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	f(q)
}

// events returns every appended event; cursored only when cursored is true.
func (q *fakeQueue) events(cursored bool) []Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []Event
	for _, a := range q.appends {
		if (a.cursor != nil) == cursored {
			out = append(out, a.events...)
		}
	}
	return out
}

func (q *fakeQueue) ranges() []CursorRange {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []CursorRange
	for _, a := range q.appends {
		if a.cursor != nil {
			out = append(out, *a.cursor)
		}
	}
	return out
}

// fakeGuest is an in-memory guest: a journal and a turn that test hooks drive.
type fakeGuest struct {
	mu        sync.Mutex
	journal   []byte
	alive     bool
	dead      bool // the wrapper died without writing turn_end
	turnSeq   int64
	lastTurn  int64
	ports     []Port
	pollErr   error
	delivered []delivery
	stops     int
	polls     int
	onDeliver func(g *fakeGuest, seq int64)
	// Port bookkeeping as SandboxRuntime does it.
	resumed      bool
	previous     []Port
	previousSent bool
	recorded     [][]Port
	// claimEnd is the journal's last turn_end at claim time (first poll).
	claimEnd *TurnEnd
}

func (g *fakeGuest) RecordPorts(_ context.Context, ports []Port) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recorded = append(g.recorded, ports)
	return nil
}

type delivery struct {
	seq    int64
	prompt string
}

func (g *fakeGuest) write(lines ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, l := range lines {
		g.journal = append(g.journal, l+"\n"...)
	}
}

func (g *fakeGuest) endTurn(reason string, exit int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.journal = append(g.journal, fmt.Sprintf("\n{\"type\":\"spawn\",\"event\":\"turn_end\",\"seq\":%d,\"exit\":%d,\"reason\":%q}\n", g.turnSeq, exit, reason)...)
	g.alive = false
}

func (g *fakeGuest) Poll(_ context.Context, cursor int64, sig string) (*PollResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.polls++
	if g.pollErr != nil {
		return nil, g.pollErr
	}
	if g.alive && g.dead { // what the poll script does for a dead wrapper
		g.journal = append(g.journal, fmt.Sprintf("\n{\"type\":\"spawn\",\"event\":\"turn_end\",\"seq\":%d,\"exit\":-1,\"reason\":\"lost\"}\n", g.turnSeq)...)
		g.alive, g.dead = false, false
	}
	portsSig := fmt.Sprintf("sig:%v", g.ports)
	size := int64(len(g.journal))
	if g.resumed {
		sig = ""
	}
	if cursor == size && sig == portsSig {
		return &PollResult{}, nil
	}
	end := min(cursor+pollChunk, size)
	data := append([]byte(nil), g.journal[cursor:end]...)
	res := &PollResult{Changed: true, Size: size, Alive: g.alive, LastTurn: g.lastTurn, PortsSig: portsSig,
		Ports: append([]Port(nil), g.ports...), From: cursor, Data: data, Full: len(data) == pollChunk, Resumed: g.resumed}
	g.resumed = false
	if !g.previousSent {
		res.Previous, g.previousSent = append([]Port{}, g.previous...), true
		res.LastTurnEnd = g.claimEnd
	}
	return res, nil
}

func (g *fakeGuest) Deliver(_ context.Context, seq int64, prompt string) error {
	g.mu.Lock()
	if seq <= g.lastTurn {
		g.mu.Unlock()
		return nil
	}
	g.delivered = append(g.delivered, delivery{seq, prompt})
	g.lastTurn, g.turnSeq, g.alive = seq, seq, true
	g.journal = append(g.journal, fmt.Sprintf("\n{\"type\":\"spawn\",\"event\":\"turn_start\",\"seq\":%d}\n", seq)...)
	hook := g.onDeliver
	g.mu.Unlock()
	if hook != nil {
		hook(g, seq)
	}
	return nil
}

func (g *fakeGuest) Stop(context.Context) error {
	g.mu.Lock()
	g.stops++
	alive := g.alive
	g.mu.Unlock()
	if alive {
		g.endTurn("interrupted", 130)
	}
	return nil
}

func (g *fakeGuest) snapshot() (deliveries []delivery, stops int, journal string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]delivery(nil), g.delivered...), g.stops, string(g.journal)
}

type fakeRuntime struct {
	mu        sync.Mutex
	guest     *fakeGuest
	ensureErr []error
	ensures   int
	finishes  int
	emit      []Event
	recorded  []Port // what Finish reads from the guest
}

func (r *fakeRuntime) Ensure(ctx context.Context, _ *Session, emit EmitFunc) (Guest, error) {
	r.mu.Lock()
	r.ensures++
	var err error
	if len(r.ensureErr) > 0 {
		err, r.ensureErr = r.ensureErr[0], r.ensureErr[1:]
	}
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(r.emit) > 0 {
		if err := emit(ctx, r.emit...); err != nil {
			return nil, err
		}
	}
	return r.guest, nil
}

func (r *fakeRuntime) Finish(context.Context, *Session) ([]Port, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishes++
	return r.recorded, nil
}

func (r *fakeRuntime) counts() (ensures, finishes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensures, r.finishes
}

func newSession() *Session {
	return &Session{ID: sessionID, OwnerUserID: "user-1", Agent: "claude-code", Status: "queued",
		SourceKind: "issue", SourceNumber: 2, LeaseToken: leaseToken}
}

func testRunner(q Queue, rt Runtime) *Runner {
	return &Runner{Queue: q, Runtime: rt, WorkerID: "test", LeaseDuration: time.Second,
		HeartbeatInterval: 5 * time.Millisecond, PollInterval: 2 * time.Millisecond,
		TurnTimeout: 45 * time.Minute, RepoDir: "/workspace/repo", backoffUnit: time.Millisecond}
}

func runOne(t *testing.T, r *Runner) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	worked, err := r.RunOne(ctx)
	if !worked {
		t.Fatal("nothing was claimed")
	}
	if ctx.Err() != nil {
		t.Fatal("RunOne did not finish in time")
	}
	return err
}

// A first turn: claim, ensure, deliver the prompt, publish the turn with
// contiguous journal cursors, then release the session to wait for the user.
func TestRunnerRunsTheFirstTurnAndReleasesIdle(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "Resolve issue #2"}}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {
		g.write(
			`{"type":"system","subtype":"init","model":"claude-opus-5-5"}`,
			`{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"hidden"}]}}`,
			bashUse, bashResult, textLine,
			`{"type":"result","subtype":"success","is_error":false}`,
		)
		g.endTurn("", 0)
	}}
	rt := &fakeRuntime{guest: g, emit: []Event{{Type: "status", Actor: "system", Payload: map[string]any{"status": "provisioning"}}}}
	if err := runOne(t, testRunner(q, rt)); err != nil {
		t.Fatal(err)
	}

	deliveries, _, journal := g.snapshot()
	if len(deliveries) != 1 || deliveries[0] != (delivery{2, "Resolve issue #2"}) {
		t.Fatalf("deliveries = %+v", deliveries)
	}
	if fmt.Sprint(q.acks) != "[2]" || fmt.Sprint(q.releases) != "[true]" {
		t.Fatalf("acks %v releases %v", q.acks, q.releases)
	}
	if ensures, finishes := rt.counts(); ensures != 1 || finishes != 0 {
		t.Fatalf("ensures %d finishes %d", ensures, finishes)
	}
	var kinds []string
	for _, e := range q.events(true) {
		kinds = append(kinds, e.Type)
	}
	if got := strings.Join(kinds, " "); got != "status step command message" {
		t.Fatalf("journal events = %q", got)
	}
	ranges := q.ranges()
	if ranges[0].From != 0 || ranges[len(ranges)-1].To != int64(len(journal)) {
		t.Fatalf("ranges %v do not cover the journal (%d bytes)", ranges, len(journal))
	}
	for i := 1; i < len(ranges); i++ {
		if ranges[i].From != ranges[i-1].To || ranges[i].To <= ranges[i].From {
			t.Fatalf("ranges not contiguous: %v", ranges)
		}
	}
	if runner := q.events(false); len(runner) != 1 || runner[0].Payload["status"] != "provisioning" {
		t.Fatalf("runner-origin events = %+v", runner)
	}
}

// A follow-up claim starts from the committed cursor, acks messages a turn
// already started for, and delivers the rest as one turn.
func TestRunnerFollowUpContinuesFromTheCursor(t *testing.T) {
	g := &fakeGuest{lastTurn: 2}
	g.write(`{"type":"spawn","event":"turn_start","seq":2}`, textLine, `{"type":"spawn","event":"turn_end","seq":2,"exit":0}`)
	_, _, journal := g.snapshot()
	s := newSession()
	s.Status, s.RuntimeCursor = "running", int64(len(journal))
	q := &fakeQueue{session: s, messages: []Message{{Seq: 2, Text: "first"}, {Seq: 6, Text: "add a test"}, {Seq: 7, Text: "and docs"}}}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		g.write(textLine)
		g.endTurn("", 0)
	}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	deliveries, _, _ := g.snapshot()
	if len(deliveries) != 1 || deliveries[0] != (delivery{7, "add a test\n\nand docs"}) {
		t.Fatalf("deliveries = %+v", deliveries)
	}
	if fmt.Sprint(q.acks) != "[2 7]" {
		t.Fatalf("acks = %v", q.acks)
	}
	if ranges := q.ranges(); ranges[0].From != s.RuntimeCursor {
		t.Fatalf("replayed committed events: %v", ranges)
	}
	if n := len(q.events(true)); n != 2 { // status running + message
		t.Fatalf("published %d events", n)
	}
}

func TestRunnerCancelStopsAndPausesThenRecordsCancelled(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {
		g.write(textLine) // and keeps running
		q.set(func(q *fakeQueue) { q.cancel = true })
	}}
	rt := &fakeRuntime{guest: g}
	if err := runOne(t, testRunner(q, rt)); err != nil {
		t.Fatal(err)
	}
	if _, finishes := rt.counts(); finishes != 1 {
		t.Fatalf("finishes = %d", finishes)
	}
	last := q.events(false)
	if len(last) == 0 || last[len(last)-1].Payload["status"] != "cancelled" {
		t.Fatalf("runner events = %+v", last)
	}
	if fmt.Sprint(q.releases) != "[false]" {
		t.Fatalf("releases = %v", q.releases)
	}
}

func TestRunnerCancelWithoutEnsure(t *testing.T) {
	s := newSession()
	s.Status, s.CancelRequested = "waiting", true
	q := &fakeQueue{session: s}
	rt := &fakeRuntime{guest: &fakeGuest{}}
	if err := runOne(t, testRunner(q, rt)); err != nil {
		t.Fatal(err)
	}
	if ensures, finishes := rt.counts(); ensures != 0 || finishes != 1 {
		t.Fatalf("ensures %d finishes %d", ensures, finishes)
	}
	if ev := q.events(false); len(ev) != 1 || ev[0].Payload["status"] != "cancelled" {
		t.Fatalf("events = %+v", ev)
	}
}

func TestRunnerInterruptStopsTheTurnOnceAndWaits(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {
		g.write(textLine)
		// The platform clears the flag on ack; Stop is pressed after that.
		go func() {
			time.Sleep(20 * time.Millisecond)
			q.set(func(q *fakeQueue) { q.interrupt = true })
		}()
	}}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	_, stops, _ := g.snapshot()
	if stops != 1 {
		t.Fatalf("stops = %d", stops)
	}
	if fmt.Sprint(q.releases) != "[true]" {
		t.Fatalf("releases = %v", q.releases)
	}
	events := q.events(true)
	if last := events[len(events)-1]; last.Payload["text"] != "Stopped." {
		t.Fatalf("last event = %+v", last)
	}
}

// Messages sent before Stop are held until a new one arrives.
func TestRunnerHoldsMessagesSentBeforeStop(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		if seq != 2 {
			g.endTurn("", 0)
			return
		}
		go func() {
			time.Sleep(20 * time.Millisecond)
			q.set(func(q *fakeQueue) {
				q.messages = append(q.messages, Message{Seq: 5, Text: "also this"})
				q.interrupt = true
			})
		}()
	}
	q.onInbox = func(q *fakeQueue, call int) {
		q.mu.Lock()
		defer q.mu.Unlock()
		// Some rounds after the stop, the user writes again.
		if len(q.messages) == 2 && q.inbox > 8 {
			q.messages = append(q.messages, Message{Seq: 8, Text: "now this"})
		}
	}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	deliveries, stops, _ := g.snapshot()
	// The turn after a Stop says so (the session has no platform
	// instructions, so the fallback notice).
	want := "<spawn_notice>\n" + fallbackNotices.Interrupted + "\n</spawn_notice>\n\nalso this\n\nnow this"
	if stops != 1 || len(deliveries) != 2 || deliveries[1] != (delivery{8, want}) {
		t.Fatalf("stops %d deliveries %+v", stops, deliveries)
	}
}

// testInstructions are platform instructions with recognizable notices.
var testInstructions = &Instructions{Version: "2026-10-17.1", System: "Rules.",
	Notices: Notices{Resumed: "R!", Interrupted: " I!\n", TurnFailed: "F!"}}

func notice(lines ...string) string {
	return "<spawn_notice>\n" + strings.Join(lines, "\n") + "\n</spawn_notice>\n\n"
}

// Each turn carries the notices for what happened since the previous one,
// once: a resume, and an interrupted or failed previous turn. The first turn
// gets none.
func TestRunnerPrependsTurnNoticesOnce(t *testing.T) {
	s := newSession()
	s.Instructions = testInstructions
	q := &fakeQueue{session: s, messages: []Message{{Seq: 2, Text: "first"}}}
	// Resumed before the first turn: nothing to say yet (no earlier turn).
	g := &fakeGuest{resumed: true}
	next := func(seq int64, text string) {
		q.set(func(q *fakeQueue) { q.messages = append(q.messages, Message{Seq: seq, Text: text}) })
	}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		switch seq {
		case 2:
			next(4, "second")
			g.endTurn("interrupted", 130)
		case 4:
			next(6, "third")
			g.endTurn("", 0)
		case 6:
			next(8, "fourth")
			g.mu.Lock()
			g.resumed = true
			g.mu.Unlock()
			g.endTurn("", 0)
		case 8:
			next(10, "fifth")
			g.mu.Lock()
			g.resumed = true
			g.mu.Unlock()
			g.endTurn("timeout", 124)
		case 10:
			next(12, "sixth")
			g.endTurn("", 1) // an API error or the step limit
		default:
			g.endTurn("", 0)
		}
	}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	deliveries, _, _ := g.snapshot()
	want := []delivery{
		{2, "first"},
		{4, notice("I!") + "second"},
		{6, "third"},
		{8, notice("R!") + "fourth"},
		{10, notice("R!", "F!") + "fifth"},
		{12, notice("F!") + "sixth"},
	}
	if fmt.Sprintf("%q", deliveries) != fmt.Sprintf("%q", want) {
		t.Fatalf("deliveries:\n%q\nwant:\n%q", deliveries, want)
	}
}

// A follow-up claim knows how the turn before it ended, though that turn_end
// is behind the committed cursor.
func TestRunnerNoticesATurnThatEndedBeforeTheClaim(t *testing.T) {
	for _, c := range []struct {
		end  TurnEnd
		want string
	}{
		{TurnEnd{Seq: 2, Exit: 124, Reason: "timeout"}, notice(fallbackNotices.TurnFailed) + "retry"},
		{TurnEnd{Seq: 2, Exit: -1, Reason: "lost"}, notice(fallbackNotices.TurnFailed) + "retry"},
		{TurnEnd{Seq: 2, Exit: 127, Reason: "start_failed"}, notice(fallbackNotices.TurnFailed) + "retry"},
		{TurnEnd{Seq: 2, Exit: 130, Reason: "interrupted"}, notice(fallbackNotices.Interrupted) + "retry"},
		{TurnEnd{Seq: 2, Exit: 0}, "retry"},
	} {
		g := &fakeGuest{lastTurn: 2, claimEnd: &c.end}
		g.write(`{"type":"spawn","event":"turn_start","seq":2}`, textLine,
			fmt.Sprintf(`{"type":"spawn","event":"turn_end","seq":2,"exit":%d,"reason":%q}`, c.end.Exit, c.end.Reason))
		_, _, journal := g.snapshot()
		s := newSession()
		s.Status, s.RuntimeCursor = "waiting", int64(len(journal))
		q := &fakeQueue{session: s, delivered: 2, messages: []Message{{Seq: 2, Text: "first"}, {Seq: 5, Text: "retry"}}}
		g.onDeliver = func(g *fakeGuest, seq int64) { g.endTurn("", 0) }
		if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
			t.Fatal(err)
		}
		deliveries, _, _ := g.snapshot()
		if len(deliveries) != 1 || deliveries[0] != (delivery{5, c.want}) {
			t.Errorf("%+v: deliveries = %q", c.end, deliveries)
		}
	}
}

func TestTurnNotice(t *testing.T) {
	failed := &TurnEnd{Seq: 3, Exit: 1}
	cases := []struct {
		notices Notices
		resumed bool
		end     *TurnEnd
		want    string
		names   string
	}{
		{testInstructions.Notices, false, nil, "", "[]"},
		{testInstructions.Notices, false, &TurnEnd{Seq: 3}, "", "[]"},
		{testInstructions.Notices, true, &TurnEnd{Seq: 3}, notice("R!"), "[resumed]"},
		{testInstructions.Notices, true, failed, notice("R!", "F!"), "[resumed turn_failed]"},
		{testInstructions.Notices, false, &TurnEnd{Seq: 3, Exit: 143, Reason: "interrupted"}, notice("I!"), "[interrupted]"},
		// The platform may leave a notice out: no block for it.
		{Notices{Resumed: "R!"}, false, failed, "", "[]"},
		{Notices{Resumed: "  ", TurnFailed: "F!"}, true, failed, notice("F!"), "[turn_failed]"},
	}
	for i, c := range cases {
		got, names := turnNotice(c.notices, c.resumed, c.end)
		if got != c.want || fmt.Sprint(names) != c.names {
			t.Errorf("case %d: %q %v, want %q %s", i, got, names, c.want, c.names)
		}
	}
}

func TestRunnerLeaseLostStopsWithoutTouchingTheGuest(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {
		q.set(func(q *fakeQueue) { q.hbErr = ErrLeaseLost })
	}}
	rt := &fakeRuntime{guest: g}
	if err := runOne(t, testRunner(q, rt)); err != nil {
		t.Fatal(err)
	}
	_, stops, _ := g.snapshot()
	if _, finishes := rt.counts(); stops != 0 || finishes != 0 || len(q.releases) != 0 {
		t.Fatalf("stops %d finishes %d releases %v", stops, finishes, q.releases)
	}
	for _, e := range q.events(false) {
		if e.Type == "error" {
			t.Fatal("lease loss published an error")
		}
	}
}

func TestRunnerCursorConflictStops(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}},
		appendErr: func(cursor *CursorRange, _ []Event) error {
			if cursor != nil {
				return &CursorConflictError{RuntimeCursor: 999}
			}
			return nil
		}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) { g.write(textLine) }}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	if len(q.releases) != 0 {
		t.Fatalf("released after a cursor conflict: %v", q.releases)
	}
}

// An unchanged guest (poll exit 10) publishes nothing.
func TestRunnerQuietPollsPublishNothing(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		go func() {
			for {
				time.Sleep(time.Millisecond)
				g.mu.Lock()
				polls := g.polls
				g.mu.Unlock()
				if polls > 30 {
					g.endTurn("", 0)
					return
				}
			}
		}()
	}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	// turn_start's status and nothing else, however many polls ran.
	if events := q.events(true); len(events) != 1 || events[0].Payload["status"] != "running" {
		t.Fatalf("events = %+v", events)
	}
}

func TestRunnerTurnsADeadWrapperIntoAFailedTurn(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {
		g.write(bashUse) // the tool call never finishes
		g.mu.Lock()
		g.dead = true
		g.mu.Unlock()
	}}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	events := q.events(true)
	var kinds []string
	for _, e := range events {
		kinds = append(kinds, e.Type)
	}
	if got := strings.Join(kinds, " "); got != "status command message" {
		t.Fatalf("events = %q", got)
	}
	if text := events[2].Payload["text"]; !strings.Contains(text.(string), "exited unexpectedly") {
		t.Fatalf("message = %v", text)
	}
	if fmt.Sprint(q.releases) != "[true]" {
		t.Fatalf("releases = %v", q.releases)
	}
}

func TestRunnerShutdownLeavesTheTurnRunning(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {
		go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	}}
	rt := &fakeRuntime{guest: g}
	done := make(chan error, 1)
	go func() {
		_, err := testRunner(q, rt).RunOne(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop on shutdown")
	}
	_, stops, _ := g.snapshot()
	if _, finishes := rt.counts(); stops != 0 || finishes != 0 || len(q.releases) != 0 {
		t.Fatalf("shutdown touched the session: stops %d finishes %d releases %v", stops, finishes, q.releases)
	}
}

func TestRunnerEnsureFailurePublishesTheUserMessage(t *testing.T) {
	q := &fakeQueue{session: newSession()}
	rt := &fakeRuntime{ensureErr: []error{asUserError("Save your Anthropic credentials on the Secrets page, then start the session again.", errors.New("no creds"))}}
	if err := runOne(t, testRunner(q, rt)); err == nil {
		t.Fatal("failure not returned")
	}
	events := q.events(false)
	if len(events) != 1 || events[0].Type != "error" || !strings.Contains(events[0].Payload["message"].(string), "Secrets page") {
		t.Fatalf("events = %+v", events)
	}
	if ensures, _ := rt.counts(); ensures != 1 {
		t.Fatalf("a permanent failure was retried %d times", ensures)
	}
}

func TestRunnerRetriesABusySandbox(t *testing.T) {
	q := &fakeQueue{session: newSession()}
	busy := fmt.Errorf("create sandbox: %w", controlplane.ErrSandboxBusy)
	rt := &fakeRuntime{guest: &fakeGuest{}, ensureErr: []error{busy, busy}}
	if err := runOne(t, testRunner(q, rt)); err != nil {
		t.Fatal(err)
	}
	if ensures, _ := rt.counts(); ensures != 3 {
		t.Fatalf("ensures = %d", ensures)
	}
	if fmt.Sprint(q.releases) != "[true]" {
		t.Fatalf("releases = %v", q.releases)
	}
}

func TestRunnerFailsWhenTheJournalWasLost(t *testing.T) {
	s := newSession()
	s.Status, s.RuntimeCursor = "running", 500
	q := &fakeQueue{session: s}
	g := &fakeGuest{pollErr: fmt.Errorf("%w: journal has 0 bytes", errJournalLost)}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err == nil {
		t.Fatal("failure not returned")
	}
	events := q.events(false)
	if len(events) != 1 || events[0].Type != "error" || !strings.Contains(events[0].Payload["message"].(string), "history") {
		t.Fatalf("events = %+v", events)
	}
}

func TestRunnerPublishesPreviewsAndSurvivesARefusal(t *testing.T) {
	refuse := true
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	q.appendErr = func(cursor *CursorRange, events []Event) error {
		if cursor == nil && events[0].Type == "preview" && refuse {
			refuse = false
			return fmt.Errorf("%w (400): unknown event type: preview", ErrRejected)
		}
		return nil
	}
	g := &fakeGuest{ports: []Port{{3001, "0.0.0.0"}, {22, "0.0.0.0"}}}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		go func() {
			time.Sleep(10 * time.Millisecond)
			g.mu.Lock()
			g.ports = []Port{{3001, "0.0.0.0"}, {5173, "0.0.0.0"}, {5173, "::"}, {41234, "127.0.0.1"}}
			g.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			g.endTurn("", 0)
		}()
	}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	var previews []string
	for _, e := range q.events(false) {
		if e.Type == "preview" {
			previews = append(previews, fmt.Sprintf("%v:%v:%v", e.Payload["port"], e.Payload["open"], e.Payload["bind"]))
		}
	}
	// The first report (port 22 open) was refused and dropped; the change
	// is still published.
	if got := strings.Join(previews, " "); got != "22:false:0.0.0.0 5173:true:0.0.0.0" {
		t.Fatalf("previews = %q", got)
	}
}

func previews(q *fakeQueue) string {
	var out []string
	for _, e := range q.events(false) {
		if e.Type == "preview" {
			out = append(out, fmt.Sprintf("%v:%v:%v", e.Payload["port"], e.Payload["open"], e.Payload["bind"]))
		}
	}
	return strings.Join(out, " ")
}

// After a resume, every port the session showed is closed, then the ports
// still listening are opened again.
func TestRunnerResetsPortsAfterAResume(t *testing.T) {
	s := newSession()
	s.Status = "running"
	q := &fakeQueue{session: s, messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{resumed: true, previous: []Port{{8080, "0.0.0.0"}, {5173, "0.0.0.0"}},
		ports: []Port{{5173, "0.0.0.0"}}}
	g.onDeliver = func(g *fakeGuest, seq int64) { g.endTurn("", 0) }
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	if got, want := previews(q), "5173:false:0.0.0.0 8080:false:0.0.0.0 5173:true:0.0.0.0"; got != want {
		t.Fatalf("previews = %q, want %q", got, want)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.recorded) != 1 || fmt.Sprint(g.recorded[0]) != "[{5173 0.0.0.0}]" {
		t.Fatalf("recorded = %v", g.recorded)
	}
}

// A new claim without a pause only closes what stopped listening.
func TestRunnerStartsFromThePortsAnEarlierClaimPublished(t *testing.T) {
	s := newSession()
	s.Status = "running"
	q := &fakeQueue{session: s, messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{previous: []Port{{8080, "0.0.0.0"}, {5173, "0.0.0.0"}}, ports: []Port{{5173, "0.0.0.0"}}}
	g.onDeliver = func(g *fakeGuest, seq int64) { g.endTurn("", 0) }
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	if got := previews(q); got != "8080:false:0.0.0.0" {
		t.Fatalf("previews = %q", got)
	}
}

// A sandbox resumed in the middle of a claim gets the same reset.
func TestRunnerResetsPortsOnAResumeDuringTheClaim(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{ports: []Port{{3000, "0.0.0.0"}}}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		go func() {
			time.Sleep(15 * time.Millisecond)
			g.mu.Lock()
			g.resumed = true
			g.mu.Unlock()
			time.Sleep(15 * time.Millisecond)
			g.endTurn("", 0)
		}()
	}
	if err := runOne(t, testRunner(q, &fakeRuntime{guest: g})); err != nil {
		t.Fatal(err)
	}
	if got, want := previews(q), "3000:true:0.0.0.0 3000:false:0.0.0.0 3000:true:0.0.0.0"; got != want {
		t.Fatalf("previews = %q, want %q", got, want)
	}
}

func TestRunnerCancelClosesKnownPorts(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{ports: []Port{{5173, "0.0.0.0"}}}
	g.onDeliver = func(g *fakeGuest, seq int64) {
		go func() {
			time.Sleep(15 * time.Millisecond)
			q.set(func(q *fakeQueue) { q.cancel = true })
		}()
	}
	rt := &fakeRuntime{guest: g, recorded: []Port{{8000, "127.0.0.1"}}}
	if err := runOne(t, testRunner(q, rt)); err != nil {
		t.Fatal(err)
	}
	if got, want := previews(q), "5173:true:0.0.0.0 5173:false:0.0.0.0 8000:false:127.0.0.1"; got != want {
		t.Fatalf("previews = %q, want %q", got, want)
	}
	events := q.events(false)
	if events[len(events)-1].Payload["status"] != "cancelled" {
		t.Fatal("cancelled is not the last event")
	}
}

func TestRunnerStopsATurnPastItsTimeLimit(t *testing.T) {
	q := &fakeQueue{session: newSession(), messages: []Message{{Seq: 2, Text: "go"}}}
	g := &fakeGuest{onDeliver: func(g *fakeGuest, seq int64) {}}
	r := testRunner(q, &fakeRuntime{guest: g})
	r.TurnTimeout, r.guardMargin = 10*time.Millisecond, 10*time.Millisecond
	if err := runOne(t, r); err != nil {
		t.Fatal(err)
	}
	if _, stops, _ := g.snapshot(); stops != 1 {
		t.Fatalf("stops = %d", stops)
	}
}

func TestNextBatchRespectsPlatformLimits(t *testing.T) {
	var groups []Released
	end := int64(0)
	for i := 0; i < 250; i++ {
		end += 10
		var events []Event
		if i%5 != 4 {
			events = []Event{{Type: "step", Actor: "agent", Payload: map[string]any{"title": strings.Repeat("x", 3000)}}}
		}
		groups = append(groups, Released{End: end, Events: events})
	}
	var to int64
	total := 0
	for len(groups) > 0 {
		b, used := nextBatch(groups)
		if used == 0 {
			break
		}
		if len(b.events) == 0 || len(b.events) > maxBatchEvents || eventsSize(b.events) > maxBatchBytes {
			t.Fatalf("batch of %d events, %d bytes", len(b.events), eventsSize(b.events))
		}
		if b.to <= to || b.to != groups[used-1].End {
			t.Fatalf("batch ends at %d after %d", b.to, to)
		}
		to, total = b.to, total+len(b.events)
		groups = groups[used:]
	}
	if total != 200 || to != 2500 {
		t.Fatalf("batched %d events up to %d", total, to)
	}
	if _, used := nextBatch([]Released{{End: 5}, {End: 9}}); used != 0 {
		t.Fatal("made a batch without events")
	}
}

func TestDiffPorts(t *testing.T) {
	before := portMap([]Port{{5173, "127.0.0.1"}, {8080, "0.0.0.0"}})
	events := diffPorts(before, []Port{{5173, "0.0.0.0"}, {3001, "0.0.0.0"}, {33000, "127.0.0.1"}, {33001, "0.0.0.0"}})
	var got []string
	for _, e := range events {
		got = append(got, fmt.Sprintf("%v:%v:%v", e.Payload["port"], e.Payload["open"], e.Payload["bind"]))
	}
	want := "5173:true:0.0.0.0 8080:false:0.0.0.0 33001:true:0.0.0.0"
	if strings.Join(got, " ") != want {
		t.Fatalf("diff = %q, want %q", got, want)
	}
}

func TestCombineCapsThePrompt(t *testing.T) {
	// Messages are joined oldest first until the next would not fit; the
	// rest are delivered as the following turn.
	big := strings.Repeat("a", maxPromptBytes-10)
	seq, prompt := combine([]Message{{Seq: 9, Text: "third"}, {Seq: 4, Text: "second"}, {Seq: 3, Text: big}})
	if seq != 4 || prompt != big+"\n\nsecond" {
		t.Fatalf("seq %d, prompt %d bytes", seq, len(prompt))
	}
	seq, prompt = combine([]Message{{Seq: 3, Text: big + "aaaaaa"}, {Seq: 4, Text: "second"}})
	if seq != 3 || len(prompt) != maxPromptBytes-4 {
		t.Fatalf("seq %d, prompt %d bytes", seq, len(prompt))
	}
}

func TestPoolRunsWorkersUntilShutdown(t *testing.T) {
	q := &countingQueue{}
	pool := &Pool{Runner: Runner{Queue: q, Runtime: &fakeRuntime{}, WorkerID: "cp"}, Concurrency: 3,
		IdleInterval: time.Millisecond, ErrorInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not stop")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.workers) != 3 || q.claims < 3 {
		t.Fatalf("workers %v claims %d", q.workers, q.claims)
	}
}

type countingQueue struct {
	fakeQueue
	mu      sync.Mutex
	claims  int
	workers map[string]bool
}

func (c *countingQueue) Claim(_ context.Context, workerID string, _ time.Duration) (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.claims++
	if c.workers == nil {
		c.workers = map[string]bool{}
	}
	c.workers[workerID] = true
	if c.claims%2 == 0 {
		return nil, errors.New("platform unreachable")
	}
	return nil, nil
}
