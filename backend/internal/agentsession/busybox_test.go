package agentsession

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestGuestScriptsUnderBusybox runs the guest scripts in an Alpine container
// with busybox ash, setsid, timeout and a real /proc: the guest's actual
// toolbox. It needs Docker and is opt-in:
//
//	AGENTSESSION_DOCKER_IMAGE=alpine:3.20 go test ./internal/agentsession -run Busybox
func TestGuestScriptsUnderBusybox(t *testing.T) {
	image := os.Getenv("AGENTSESSION_DOCKER_IMAGE")
	if image == "" {
		t.Skip("set AGENTSESSION_DOCKER_IMAGE (for example alpine:3.20) to run")
	}
	work := t.TempDir()
	p := guestPaths{Base: "/work/agent/" + testSessionID, Repo: "/work/repo", Proc: "/proc", Tmp: "/work/tmp", StartTicks: 50}
	env := "export HOME='/work/home'\nexport TMPDIR='/work/tmp'\n"
	files := map[string]string{
		"setup.cmd":    shellCommand(setupScript(p, env, "system prompt", turnScript(p, testSessionID, 45*time.Minute))),
		"deliver2.cmd": shellCommand(deliverScript(p, 2, "first")),
		"deliver3.cmd": shellCommand(deliverScript(p, 3, "second")),
		"poll.cmd":     shellCommand(pollScript(p, 0, "")),
		"stop.cmd":     shellCommand(stopScript(p)),
		"bin/claude": `#!/bin/sh
mkdir -p "$HOME/.claude/projects/-work-repo"
: > "$HOME/.claude/projects/-work-repo/$7.jsonl"
echo "$*" >> /work/claude-args
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}'
if [ -f /work/slow ]; then sleep 60; fi
printf '%s\n' '{"type":"result","subtype":"success"}'
`,
		"harness.sh": `set -u
export PATH=/work/bin:$PATH
mkdir -p /work/repo /work/home
run() { sh -c "$(cat /work/$1)"; }
run setup.cmd > /work/setup.out
stat -c '%a %U' /work/tmp/review > /work/review.out
stat -c '%a %U' /work/agent/` + testSessionID + `/settings.json > /work/settings.out
run deliver2.cmd > /work/deliver2.out
i=0; while ! grep -q '"event":"turn_end","seq":2' /work/agent/*/events.jsonl && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
run setup.cmd > /work/setup2.out
touch /work/slow
run deliver3.cmd > /work/deliver3.out
sleep 1
run poll.cmd > /work/poll-running.out
run stop.cmd > /work/stop.out
sleep 0.5
pgrep -f "sleep 60" > /work/survivors.out || true
run poll.cmd > /work/poll-stopped.out
echo "999999 4 1" > /work/agent/` + testSessionID + `/turn.pgid
run poll.cmd > /work/poll-lost.out
echo done > /work/harness.out
`,
	}
	for name, content := range files {
		path := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "-v", work+":/work", image, "sh", "/work/harness.sh").CombinedOutput(); err != nil {
		t.Fatalf("docker: %v\n%s", err, out)
	}
	read := func(name string) string {
		raw, err := os.ReadFile(filepath.Join(work, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if read("harness.out") != "done\n" {
		t.Fatal("harness did not finish")
	}
	if report, err := parseSetup(read("setup.out")); err != nil || report.WorkspaceReady || report.Size != 0 || report.LastEnd != nil {
		t.Fatalf("setup = %+v, %v", report, err)
	}
	// The agent's review directory: root's, readable by the platform's exec.
	if got := strings.TrimSpace(read("review.out")); got != "755 root" {
		t.Fatalf("review dir = %q", got)
	}
	if got := strings.TrimSpace(read("settings.out")); got != "600 root" {
		t.Fatalf("settings.json = %q", got)
	}
	if report, err := parseSetup(read("setup2.out")); err != nil || report.LastEnd == nil || *report.LastEnd != (TurnEnd{Seq: 2}) {
		t.Fatalf("setup after a turn = %+v, %v", report, err)
	}
	if strings.TrimSpace(read("deliver2.out")) != "started" || strings.TrimSpace(read("deliver3.out")) != "started" {
		t.Fatalf("deliver = %q %q", read("deliver2.out"), read("deliver3.out"))
	}
	args := strings.Split(strings.TrimSpace(read("claude-args")), "\n")
	if len(args) != 2 || !strings.Contains(args[0], "--session-id "+testSessionID) || !strings.Contains(args[1], "--resume "+testSessionID) {
		t.Fatalf("claude args = %q", args)
	}
	for _, want := range []string{"--permission-prompts none", "--append-system-prompt-file " + p.Base + "/system.txt", "--settings " + p.Base + "/settings.json"} {
		if !strings.Contains(args[0], want) {
			t.Fatalf("claude args %q lack %q", args[0], want)
		}
	}
	running, err := parsePoll(read("poll-running.out"))
	if err != nil || !running.Alive || running.LastTurn != 3 {
		t.Fatalf("poll while running = %+v, %v", running, err)
	}
	if strings.TrimSpace(read("stop.out")) != "stopped" || strings.TrimSpace(read("survivors.out")) != "" {
		t.Fatalf("stop = %q, survivors %q", read("stop.out"), read("survivors.out"))
	}
	stopped, err := parsePoll(read("poll-stopped.out"))
	if err != nil || stopped.Alive {
		t.Fatalf("poll after stop = %+v, %v", stopped, err)
	}
	lost, err := parsePoll(read("poll-lost.out"))
	if err != nil || lost.Alive {
		t.Fatalf("poll with a lost wrapper = %+v, %v", lost, err)
	}
	events, d := decodeAll(t, lost.Data)
	if d.Busy() {
		t.Fatal("journal leaves tool calls open")
	}
	var texts []string
	for _, e := range events {
		if text, ok := e.Payload["text"].(string); ok {
			texts = append(texts, text)
		}
	}
	want := []string{"hello", "hello", "Stopped.", "The agent process exited unexpectedly. Send a message to continue."}
	if strings.Join(texts, "|") != strings.Join(want, "|") {
		t.Fatalf("messages = %q", texts)
	}
}
