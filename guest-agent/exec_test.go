package main

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// useTestShell makes a bash in a temporary directory the persistent shell, an
// empty buffer the output feed and a temporary file the exec stderr file.
func useTestShell(t *testing.T) {
	t.Helper()
	cmd := exec.Command("/bin/bash")
	cmd.Dir = t.TempDir()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	shellMu.Lock()
	globalShell = &ShellSession{cmd: cmd, stdin: stdin, stdout: bufio.NewScanner(stdout)}
	shellMu.Unlock()

	oldLogs, oldStderr := outputLogs, execStderrFile
	outputLogs = &outputBuffer{max: 1 << 20}
	execStderrFile = filepath.Join(t.TempDir(), "exec_stderr")
	t.Cleanup(func() {
		evictShell()
		_ = cmd.Wait()
		outputLogs, execStderrFile = oldLogs, oldStderr
	})
}

// feedRows returns what the output feed holds.
func feedRows() []guestLog {
	outputLogs.mu.Lock()
	defer outputLogs.mu.Unlock()
	return append([]guestLog(nil), outputLogs.rows...)
}

// A quiet exec returns its output but leaves the output feed (the sandbox's
// log stream) alone; the same request without the flag, as hosts that predate
// it send, is copied into the feed as before.
func TestQuietExecKeepsOutputOutOfTheFeed(t *testing.T) {
	useTestShell(t)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Non-blocking so the host side takes a deadline; the agent's side stays
	// blocking, as an accepted vsock connection is.
	if err := unix.SetNonblock(fds[1], true); err != nil {
		t.Fatal(err)
	}
	host := os.NewFile(uintptr(fds[1]), "host")
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(fds[0])
		_ = unix.Close(fds[0])
	}()
	defer func() {
		_ = host.Close()
		<-done
	}()
	if err := host.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}

	run := func(request map[string]any) ExecutionResponse {
		t.Helper()
		if err := json.NewEncoder(host).Encode(request); err != nil {
			t.Fatal(err)
		}
		// The agent answers each request with one line and then waits.
		var response ExecutionResponse
		if err := json.NewDecoder(host).Decode(&response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	command := `printf 'file contents\n'; printf 'warning\n' >&2`

	quiet := run(map[string]any{"type": "exec", "command": command, "timeout": 10, "quiet": true})
	if !strings.HasPrefix(quiet.Stdout, "file contents\n") || quiet.Stderr != "warning\n" || quiet.ExitCode != 0 {
		t.Fatalf("quiet exec = %+v", quiet)
	}
	if rows := feedRows(); len(rows) != 0 {
		t.Fatalf("quiet exec reached the output feed: %+v", rows)
	}

	logged := run(map[string]any{"type": "exec", "command": command, "timeout": 10})
	if !strings.HasPrefix(logged.Stdout, "file contents\n") || logged.Stderr != "warning\n" {
		t.Fatalf("exec = %+v", logged)
	}
	feed := map[string]string{}
	for _, row := range feedRows() {
		feed[row.Stream] += row.Content
	}
	if !strings.HasPrefix(feed["stdout"], "file contents\n") || feed["stderr"] != "warning\n" {
		t.Fatalf("exec without quiet must reach the output feed: %q", feed)
	}
}
