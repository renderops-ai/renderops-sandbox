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
