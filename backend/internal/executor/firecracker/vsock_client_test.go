package firecracker

import (
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type deadlineTrackingConn struct {
	mu       sync.Mutex
	deadline time.Time
}

func (c *deadlineTrackingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *deadlineTrackingConn) Write(data []byte) (int, error)   { return len(data), nil }
func (c *deadlineTrackingConn) Close() error                     { return nil }
func (c *deadlineTrackingConn) LocalAddr() net.Addr              { return nil }
func (c *deadlineTrackingConn) RemoteAddr() net.Addr             { return nil }
func (c *deadlineTrackingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *deadlineTrackingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *deadlineTrackingConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	return nil
}

func (c *deadlineTrackingConn) currentDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}

// execRequestOnWire runs one exec against a fake guest and returns the request
// line the guest received.
func execRequestOnWire(t *testing.T, opts ExecOptions) map[string]any {
	t.Helper()
	host, guest := net.Pipe()
	defer host.Close()
	received := make(chan map[string]any, 1)
	go func() {
		defer guest.Close()
		var req map[string]any
		if err := json.NewDecoder(guest).Decode(&req); err != nil {
			t.Errorf("decode exec request: %v", err)
			close(received)
			return
		}
		received <- req
		if err := json.NewEncoder(guest).Encode(ExecuteResponse{Stdout: "file contents\n"}); err != nil {
			t.Errorf("encode exec response: %v", err)
		}
	}()
	resp, err := NewVsockClient("unused").ExecOnConnWithOptions(host, "cat notes.txt", 5, opts)
	if err != nil || resp.Stdout != "file contents\n" {
		t.Fatalf("exec = %+v, %v", resp, err)
	}
	return <-received
}

// A quiet exec asks the guest to keep its output out of the output feed; any
// other exec sends exactly the request guests have always received.
func TestExecOnConnSendsQuietOnlyWhenSet(t *testing.T) {
	quiet := execRequestOnWire(t, ExecOptions{Quiet: true})
	if quiet["type"] != "exec" || quiet["command"] != "cat notes.txt" || quiet["quiet"] != true {
		t.Fatalf("quiet exec request = %v", quiet)
	}
	plain := execRequestOnWire(t, ExecOptions{})
	if _, ok := plain["quiet"]; ok || len(plain) != 3 {
		t.Fatalf("plain exec request = %v, want only type, command and timeout", plain)
	}
}

func TestResetRuntimesClearsDeadlineAfterReadFailure(t *testing.T) {
	conn := &deadlineTrackingConn{}

	err := NewVsockClient("unused").ResetRuntimesOnConn(conn)
	if err == nil {
		t.Fatal("expected reset_runtimes read failure")
	}
	if deadline := conn.currentDeadline(); !deadline.IsZero() {
		t.Fatalf("connection deadline was not cleared: %v", deadline)
	}
}
