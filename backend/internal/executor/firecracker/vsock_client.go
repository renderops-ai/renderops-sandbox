package firecracker

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// VsockClient handles communication with the guest agent via vsock
type VsockClient struct {
	socketPath string
	timeout    time.Duration
}

// ExecuteRequest matches the guest agent's ExecutionRequest
type ExecuteRequest struct {
	Code     string `json:"code"`
	Language string `json:"language"`
	Mode     string `json:"mode"`
	Timeout  int    `json:"timeout"` // seconds
}

// ExecuteResponse matches the guest agent's ExecutionResponse
type ExecuteResponse struct {
	Stdout   string  `json:"stdout"`
	Stderr   string  `json:"stderr"`
	ExitCode int     `json:"exit_code"`
	Duration float64 `json:"duration"`
}

type TerminalOpenRequest struct {
	TerminalID string            `json:"terminal_id"`
	Hostname   string            `json:"hostname,omitempty"`
	Shell      string            `json:"shell"`
	Columns    uint16            `json:"columns"`
	Rows       uint16            `json:"rows"`
	Env        map[string]string `json:"env,omitempty"`
}

func NewVsockClient(socketPath string) *VsockClient {
	return &VsockClient{
		socketPath: socketPath,
		timeout:    60 * time.Second,
	}
}

// Ping tests whether the guest agent is ready by doing the vsock handshake
// and checking for an "OK" response. Used by the pool to verify a VM is
// actually accepting connections before it is made available for requests.
func (v *VsockClient) Ping() (ok bool, stage string, elapsed time.Duration) {
	t := time.Now()
	conn, err := net.DialTimeout("unix", v.socketPath, 2*time.Second)
	if err != nil {
		return false, "connect", time.Since(t)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Write([]byte("CONNECT 52\n")); err != nil {
		return false, "write", time.Since(t)
	}
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil || n < 2 {
		return false, "read", time.Since(t)
	}
	if string(buf[:2]) != "OK" {
		return false, "handshake", time.Since(t)
	}
	return true, "ok", time.Since(t)
}

// Connect opens a persistent vsock connection to the guest agent.
// The caller is responsible for closing the connection when done (e.g. on session destroy).
func (v *VsockClient) Connect() (net.Conn, error) {
	return v.dial()
}

// ResetRuntimesOnConn tells the guest agent to tear down all stateful language runtimes
// (Python kernels, Node bridge, bash shell) so the next execution starts fresh. Used on
// resume for a consistent cross-language contract (in-memory interpreter state resets;
// filesystem/env persist) and to avoid ZMQ kernels left degraded by a snapshot restore.
func (v *VsockClient) ResetRuntimesOnConn(conn net.Conn) error {
	req := struct {
		Type string `json:"type"`
	}{Type: "reset_runtimes"}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("send reset_runtimes: %w", err)
	}

	var resp struct {
		Success bool `json:"success"`
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set reset_runtimes deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("read reset_runtimes response: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("reset_runtimes failed")
	}
	return nil
}

// SetEnvOnConn sends a set_env message on an existing persistent connection.
func (v *VsockClient) SetEnvOnConn(conn net.Conn, env map[string]string) error {
	req := struct {
		Type string            `json:"type"`
		Env  map[string]string `json:"env"`
	}{Type: "set_env", Env: env}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("send set_env: %w", err)
	}

	var resp struct {
		Success bool   `json:"success"`
		Error   string `json:"error,omitempty"`
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set set_env deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("read set_env response: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("set_env failed: %s", resp.Error)
	}
	return nil
}

func (v *VsockClient) OpenTerminalOnConn(conn net.Conn, request TerminalOpenRequest) error {
	req := struct {
		Type string `json:"type"`
		TerminalOpenRequest
	}{Type: "terminal_open", TerminalOpenRequest: request}
	return v.terminalRequest(conn, req, "open")
}

func (v *VsockClient) CloseTerminalOnConn(conn net.Conn, terminalID string) error {
	req := struct {
		Type       string `json:"type"`
		TerminalID string `json:"terminal_id"`
	}{Type: "terminal_close", TerminalID: terminalID}
	return v.terminalRequest(conn, req, "close")
}

func (v *VsockClient) CloseAllTerminalsOnConn(conn net.Conn) error {
	req := struct {
		Type string `json:"type"`
	}{Type: "terminal_close_all"}
	return v.terminalRequest(conn, req, "close all")
}

func (v *VsockClient) terminalRequest(conn net.Conn, request any, operation string) error {
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return fmt.Errorf("send terminal %s request: %w", operation, err)
	}
	var response struct {
		Success bool   `json:"success"`
		Error   string `json:"error,omitempty"`
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set terminal %s deadline: %w", operation, err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return fmt.Errorf("read terminal %s response: %w", operation, err)
	}
	if !response.Success {
		return fmt.Errorf("terminal %s failed: %s", operation, response.Error)
	}
	return nil
}

// ExecOptions changes how the guest agent treats one exec. The zero value is
// ExecOnConn's behavior.
type ExecOptions struct {
	// Quiet asks the guest not to copy the command's output into its output
	// feed (the sandbox's log stream); the output is still returned. A guest
	// agent built before this option ignores it and copies the output.
	Quiet bool
}

// ExecOnConn runs a shell command on an existing persistent connection.
func (v *VsockClient) ExecOnConn(conn net.Conn, command string, timeoutSec int) (*ExecuteResponse, error) {
	return v.ExecOnConnWithOptions(conn, command, timeoutSec, ExecOptions{})
}

// ExecOnConnWithOptions is ExecOnConn with guest exec options. Without
// options the request is exactly ExecOnConn's.
func (v *VsockClient) ExecOnConnWithOptions(conn net.Conn, command string, timeoutSec int, opts ExecOptions) (*ExecuteResponse, error) {
	req := struct {
		Type    string `json:"type"`
		Command string `json:"command"`
		Timeout int    `json:"timeout"`
		Quiet   bool   `json:"quiet,omitempty"`
	}{Type: "exec", Command: command, Timeout: timeoutSec, Quiet: opts.Quiet}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("send exec request: %w", err)
	}

	var resp ExecuteResponse
	conn.SetDeadline(time.Now().Add(time.Duration(timeoutSec+10) * time.Second))
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read exec response: %w", err)
	}
	conn.SetDeadline(time.Time{})
	return &resp, nil
}

// ExecuteOnConn runs code on an existing persistent connection.
func (v *VsockClient) ExecuteOnConn(conn net.Conn, code, language, mode string, timeoutSec int) (*ExecuteResponse, error) {
	req := ExecuteRequest{
		Code:     code,
		Language: language,
		Timeout:  timeoutSec,
		Mode:     mode,
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("send execute request: %w", err)
	}

	var resp ExecuteResponse
	conn.SetDeadline(time.Now().Add(time.Duration(timeoutSec+10) * time.Second))
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read execute response: %w", err)
	}
	conn.SetDeadline(time.Time{})
	return &resp, nil
}

// dial performs the vsock handshake and returns a ready connection.
func (v *VsockClient) dial() (net.Conn, error) {
	buf := make([]byte, 32)
	// v.timeout is the overall ceiling. We poll with a SHORT per-attempt read deadline so a
	// not-yet-ready guest listener (e.g. still rebuilding after a snapshot restore) fails fast
	// and we retry — instead of blocking on one long read (which caused ~60s resume stalls).
	// Safety: we only ever return a connection that got "OK" (the guest accepted it); attempts
	// that time out without OK were never accepted, so closing+retrying them does not consume
	// the one-vsock-connection-per-restore budget.
	const perAttemptRead = 3 * time.Second
	deadline := time.Now().Add(v.timeout)
	var lastErr error
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		if attempt > 0 {
			time.Sleep(150 * time.Millisecond)
		}
		conn, err := net.DialTimeout("unix", v.socketPath, 5*time.Second)
		if err != nil {
			lastErr = err
			continue
		}
		conn.SetDeadline(time.Now().Add(perAttemptRead))
		if _, err = conn.Write([]byte("CONNECT 52\n")); err != nil {
			conn.Close()
			lastErr = err
			continue
		}
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			lastErr = err
			continue
		}
		if n < 2 || string(buf[:2]) != "OK" {
			conn.Close()
			lastErr = fmt.Errorf("vsock handshake rejected: %s", string(buf[:n]))
			continue
		}
		conn.SetDeadline(time.Time{}) // clear; callers set their own per-op deadlines
		return conn, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timed out waiting for guest agent")
	}
	return nil, fmt.Errorf("vsock handshake failed: %w", lastErr)
}

func (v *VsockClient) Execute(code, language, mode string, timeoutSec int) (*ExecuteResponse, error) {
	conn, err := v.dial()
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	req := ExecuteRequest{
		Code:     code,
		Language: language,
		Timeout:  timeoutSec,
		Mode:     mode,
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	var resp ExecuteResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return &resp, nil
}
