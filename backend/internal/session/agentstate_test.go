package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"backend/internal/executor/firecracker"
	"backend/internal/plane"
)

// agentStateOutput is what plane.AgentStateCommand prints in a guest whose
// clock reads guestNow; file is the state file's content ("" = missing).
func agentStateOutput(guestNow time.Time, file string) string {
	return fmt.Sprintf("%d\n%s", guestNow.Unix(), file)
}

func agentFile(state string, updatedAt time.Time) string {
	return `{"state":"` + state + `","updated_at":"` + updatedAt.Format(time.RFC3339) + `"}` + "\n"
}

func TestAgentKeepsAwake(t *testing.T) {
	const grace = 15 * time.Minute
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	// A guest restored from a snapshot runs behind the host; ages use its clock.
	guestNow := now.Add(-2 * time.Hour)

	tests := []struct {
		name    string
		stdout  string
		execErr error
		grace   time.Duration
		want    bool
		reason  string
	}{
		{name: "fresh working state", stdout: agentStateOutput(guestNow, agentFile("working", guestNow.Add(-time.Minute))), want: true, reason: "working"},
		{name: "working state exactly at grace", stdout: agentStateOutput(guestNow, agentFile("working", guestNow.Add(-grace))), want: true, reason: "working"},
		{name: "stale working state", stdout: agentStateOutput(guestNow, agentFile("working", guestNow.Add(-grace-time.Second))), reason: "stale"},
		{name: "working state far in the future", stdout: agentStateOutput(guestNow, agentFile("working", guestNow.Add(10*time.Minute))), reason: "future"},
		{name: "working state slightly ahead of the guest clock", stdout: agentStateOutput(guestNow, agentFile("working", guestNow.Add(30*time.Second))), want: true, reason: "working"},
		{name: "waiting for the user", stdout: agentStateOutput(guestNow, agentFile("waiting", guestNow)), reason: "waiting"},
		{name: "done", stdout: agentStateOutput(guestNow, agentFile("done", guestNow)), reason: "done"},
		{name: "idle", stdout: agentStateOutput(guestNow, agentFile("idle", guestNow)), reason: "idle"},
		{name: "missing file", stdout: agentStateOutput(guestNow, ""), reason: "missing"},
		{name: "malformed file", stdout: agentStateOutput(guestNow, `{"state":"working","updated_at":`), reason: "invalid"},
		{name: "unknown state", stdout: agentStateOutput(guestNow, agentFile("busy", guestNow)), reason: "invalid"},
		{name: "updated_at not RFC3339", stdout: agentStateOutput(guestNow, `{"state":"working","updated_at":"yesterday"}`), reason: "invalid"},
		{name: "exec failed", execErr: errors.New("exec failed: read exec response: i/o timeout"), reason: "read_failed"},
		{name: "check turned off", stdout: agentStateOutput(guestNow, agentFile("working", guestNow)), grace: -1, reason: "check_disabled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := grace
			if tt.grace != 0 {
				g = tt.grace
			}
			var state plane.AgentState
			err := tt.execErr
			if err == nil {
				state, err = plane.ParseAgentState(tt.stdout, now)
			}
			got, reason := agentKeepsAwake(state, err, g)
			if got != tt.want || reason != tt.reason {
				t.Fatalf("agentKeepsAwake() = %v, %q; want %v, %q", got, reason, tt.want, tt.reason)
			}
		})
	}
}

// agentGuest answers every exec on conn with stdout and reports each request.
type agentGuestRequest struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	Quiet   bool   `json:"quiet"`
}

func agentGuest(t *testing.T, conn net.Conn, stdout string) <-chan agentGuestRequest {
	t.Helper()
	requests := make(chan agentGuestRequest, 16)
	go func() {
		for {
			var req agentGuestRequest
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				return
			}
			requests <- req
			if err := json.NewEncoder(conn).Encode(firecracker.ExecuteResponse{Stdout: stdout}); err != nil {
				return
			}
		}
	}()
	return requests
}

func idleAgentSession(t *testing.T, m *Manager, now time.Time, conn net.Conn) *Session {
	t.Helper()
	sess := &Session{
		ID:          "sandbox-1",
		State:       StateActive,
		VM:          &firecracker.MicroVM{},
		VsockConn:   conn,
		CreatedAt:   now.Add(-time.Hour),
		LastUsed:    now.Add(-10 * time.Minute),
		IdleTimeout: 5 * time.Minute,
		MaxLifetime: 24 * time.Hour,
	}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}
	return sess
}

// A working agent defers the idle pause. Reading its state is a passive,
// quiet exec, and the reaper waits agentBusyRecheck before reading it again.
func TestReaperDefersIdlePauseWhileAgentWorks(t *testing.T) {
	host, guest := net.Pipe()
	defer host.Close()
	defer guest.Close()
	now := time.Now()
	requests := agentGuest(t, guest, agentStateOutput(now, agentFile("working", now.Add(-2*time.Minute))))

	m := &Manager{store: NewStore(10)}
	m.SetAgentBusyGrace(DefaultAgentBusyGrace)
	sess := idleAgentSession(t, m, now, host)
	lastUsed := sess.LastUsed

	m.reapOnce(context.Background(), now)

	req := <-requests
	if req.Type != "exec" || req.Command != plane.AgentStateCommand || !req.Quiet {
		t.Fatalf("agent state read = %+v, want a quiet exec of plane.AgentStateCommand", req)
	}
	if sess.State != StateActive || !sess.LastUsed.Equal(lastUsed) || sess.RunCount != 0 || sess.LastExitCode != nil {
		t.Fatalf("state %s, last used %v (was %v), runs %d: the read must be passive and must not pause", sess.State, sess.LastUsed, lastUsed, sess.RunCount)
	}
	if want := now.Add(agentBusyRecheck); !sess.agentRecheckAt.Equal(want) {
		t.Fatalf("recheck at %v, want %v", sess.agentRecheckAt, want)
	}

	// Within the recheck window the reaper does not ask the guest again.
	m.reapOnce(context.Background(), now.Add(agentBusyRecheck/2))
	select {
	case req := <-requests:
		t.Fatalf("unexpected guest request inside the recheck window: %+v", req)
	default:
	}
}

// An agent that is not working does not keep an idle sandbox awake, so the
// reaper goes on to pause it.
func TestAgentNotWorkingDoesNotKeepSandboxAwake(t *testing.T) {
	now := time.Now()
	for _, file := range []string{
		"",
		"not json",
		agentFile("done", now),
		agentFile("waiting", now),
		agentFile("working", now.Add(-DefaultAgentBusyGrace-time.Minute)),
	} {
		host, guest := net.Pipe()
		requests := agentGuest(t, guest, agentStateOutput(now, file))
		m := &Manager{store: NewStore(10)}
		m.SetAgentBusyGrace(DefaultAgentBusyGrace)
		sess := idleAgentSession(t, m, now, host)

		if m.agentWorking(context.Background(), sess.ID, now) {
			t.Errorf("file %q kept the sandbox awake", file)
		}
		<-requests
		if !sess.agentRecheckAt.IsZero() {
			t.Errorf("file %q set a recheck time", file)
		}
		host.Close()
		guest.Close()
	}
}

func TestAgentBusyCheckTurnedOffNeverReadsTheGuest(t *testing.T) {
	host, guest := net.Pipe()
	defer host.Close()
	defer guest.Close()
	now := time.Now()
	requests := agentGuest(t, guest, agentStateOutput(now, agentFile("working", now)))
	m := &Manager{store: NewStore(10)}
	m.SetAgentBusyGrace(0)
	sess := idleAgentSession(t, m, now, host)
	if m.agentWorking(context.Background(), sess.ID, now) {
		t.Fatal("agent check turned off kept the sandbox awake")
	}
	select {
	case req := <-requests:
		t.Fatalf("agent check turned off read the guest: %+v", req)
	default:
	}
}
