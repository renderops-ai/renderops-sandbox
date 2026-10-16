package session

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"backend/internal/executor/firecracker"
)

func TestExecActiveOnlyNeverResumesAPausedSession(t *testing.T) {
	m := &Manager{store: NewStore(10)}
	paused := &Session{ID: "paused", State: StatePaused}
	if err := m.store.Add(paused); err != nil {
		t.Fatal(err)
	}
	_, err := m.ExecWithOptions(context.Background(), "paused", "true", 5, ExecOptions{ActiveOnly: true})
	if !errors.Is(err, ErrNotActive) || paused.State != StatePaused {
		t.Fatalf("err = %v, state = %s", err, paused.State)
	}
}

// fakeGuest answers exec requests on the session's vsock connection.
func fakeGuest(t *testing.T, conn net.Conn) {
	t.Helper()
	go func() {
		for {
			var req struct {
				Type    string `json:"type"`
				Command string `json:"command"`
			}
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				return
			}
			if err := json.NewEncoder(conn).Encode(firecracker.ExecuteResponse{Stdout: "hi\n", ExitCode: 7}); err != nil {
				return
			}
		}
	}()
}

// A quiet exec reaches the guest with quiet set; any other exec sends no quiet
// field at all, so guests see the request they always have.
func TestQuietExecAsksTheGuestNotToLog(t *testing.T) {
	host, guest := net.Pipe()
	defer host.Close()
	defer guest.Close()
	quietFlags := make(chan *bool, 1)
	go func() {
		for {
			var req struct {
				Quiet *bool `json:"quiet"`
			}
			if err := json.NewDecoder(guest).Decode(&req); err != nil {
				return
			}
			quietFlags <- req.Quiet
			if err := json.NewEncoder(guest).Encode(firecracker.ExecuteResponse{Stdout: "file contents\n"}); err != nil {
				return
			}
		}
	}()

	m := &Manager{store: NewStore(10)}
	sess := &Session{ID: "active", State: StateActive, VM: &firecracker.MicroVM{}, VsockConn: host}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}

	res, err := m.ExecQuiet(context.Background(), "active", "cat notes.txt", 5)
	if err != nil || res.Stdout != "file contents\n" {
		t.Fatalf("quiet exec = %+v, %v", res, err)
	}
	if quiet := <-quietFlags; quiet == nil || !*quiet {
		t.Fatalf("ExecQuiet sent quiet = %v, want true", quiet)
	}
	if _, err := m.ExecWithOptions(context.Background(), "active", "cat notes.txt", 5, ExecOptions{ActiveOnly: true, Passive: true, Quiet: true}); err != nil {
		t.Fatal(err)
	}
	if quiet := <-quietFlags; quiet == nil || !*quiet {
		t.Fatalf("ExecWithOptions(Quiet) sent quiet = %v, want true", quiet)
	}
	if _, err := m.Exec(context.Background(), "active", "cat notes.txt", 5); err != nil {
		t.Fatal(err)
	}
	if quiet := <-quietFlags; quiet != nil {
		t.Fatalf("Exec sent quiet = %v, want no quiet field", *quiet)
	}
}

func TestPassiveExecLeavesTheIdleTimerAlone(t *testing.T) {
	host, guest := net.Pipe()
	defer host.Close()
	defer guest.Close()
	fakeGuest(t, guest)

	idleSince := time.Now().Add(-time.Hour)
	m := &Manager{store: NewStore(10)}
	sess := &Session{ID: "active", State: StateActive, VM: &firecracker.MicroVM{}, VsockConn: host, LastUsed: idleSince}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}

	res, err := m.ExecWithOptions(context.Background(), "active", "cat x", 5, ExecOptions{ActiveOnly: true, Passive: true})
	if err != nil || res.Stdout != "hi\n" || res.ExitCode != 7 {
		t.Fatalf("passive exec = %+v, %v", res, err)
	}
	if !sess.LastUsed.Equal(idleSince) || sess.RunCount != 0 || sess.LastExitCode != nil {
		t.Fatalf("passive exec counted as activity: last used %v, runs %d", sess.LastUsed, sess.RunCount)
	}

	if _, err := m.ExecWithOptions(context.Background(), "active", "echo > x", 5, ExecOptions{ActiveOnly: true}); err != nil {
		t.Fatal(err)
	}
	if !sess.LastUsed.After(idleSince) || sess.RunCount != 1 || sess.LastExitCode == nil || *sess.LastExitCode != 7 {
		t.Fatalf("active exec not counted: last used %v, runs %d", sess.LastUsed, sess.RunCount)
	}
}
