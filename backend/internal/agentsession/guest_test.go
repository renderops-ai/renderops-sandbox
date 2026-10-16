package agentsession

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const testSessionID = "5be67472-8ab9-4383-9d01-3aa08f97eb42"

// shells are the POSIX shells available to run guest scripts locally. The
// guest runs busybox ash; dash is the closest common stand-in.
func shells(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, sh := range []string{"/bin/sh", "/bin/dash"} {
		if _, err := os.Stat(sh); err == nil {
			out = append(out, sh)
		}
	}
	if len(out) == 0 {
		t.Skip("no POSIX shell available")
	}
	return out
}

// guestEnv is a fake guest: a temporary root with the agent directory, a
// fake /proc and fake setsid/timeout/claude commands first on PATH.
type guestEnv struct {
	t     *testing.T
	shell string
	root  string
	paths guestPaths
	bin   string
	home  string
}

func newGuest(t *testing.T, shell string) *guestEnv {
	t.Helper()
	root := t.TempDir()
	g := &guestEnv{t: t, shell: shell, root: root, bin: filepath.Join(root, "bin"), home: filepath.Join(root, "home"),
		paths: guestPaths{
			Base: filepath.Join(root, "agent", testSessionID),
			Repo: filepath.Join(root, "repo"),
			Proc: filepath.Join(root, "proc"),
			Tmp:  filepath.Join(root, "tmp"),
			// Long enough for a real wrapper, short for the start_failed case.
			StartTicks: 10,
		}}
	for _, dir := range []string{g.bin, g.home, g.paths.Repo, filepath.Join(g.paths.Proc, "net")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	g.script("setsid", `exec "$@"`)
	g.script("timeout", `shift; exec "$@"`)
	return g
}

func (g *guestEnv) script(name, body string) {
	g.t.Helper()
	if err := os.WriteFile(filepath.Join(g.bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		g.t.Fatal(err)
	}
}

func (g *guestEnv) write(path, content string) {
	g.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		g.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		g.t.Fatal(err)
	}
}

func (g *guestEnv) read(path string) string {
	g.t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		g.t.Fatal(err)
	}
	return string(raw)
}

// run executes script exactly as the guest's persistent shell would.
func (g *guestEnv) run(script string) (stdout, stderr string, code int) {
	g.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, g.shell, "-c", strings.Replace(shellCommand(script), "| sh", "| "+g.shell, 1))
	cmd.Env = append(os.Environ(), "PATH="+g.bin+":"+os.Getenv("PATH"))
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		code = exitErr.ExitCode()
	default:
		g.t.Fatalf("run script: %v", err)
	}
	return out.String(), errOut.String(), code
}

func (g *guestEnv) setup() setupReport {
	g.t.Helper()
	env := "export HOME='" + g.home + "'\nexport TMPDIR='" + g.paths.Tmp + "'\n"
	out, stderr, code := g.run(setupScript(g.paths, env, "system prompt\n", turnScript(g.paths, testSessionID, 45*time.Minute)))
	if code != 0 {
		g.t.Fatalf("setup exit %d: %s", code, stderr)
	}
	report, err := parseSetup(out)
	if err != nil {
		g.t.Fatalf("setup output %q: %v", out, err)
	}
	return report
}

func (g *guestEnv) events() string { return g.read(filepath.Join(g.paths.Base, "events.jsonl")) }

func TestShellCommandUsesTheContractForm(t *testing.T) {
	cmd := shellCommand("echo 'quoted' \"$HOME\"\nexit 3\n")
	b64, ok := strings.CutPrefix(cmd, "printf %s ")
	if !ok || !strings.HasSuffix(cmd, " | base64 -d | sh") {
		t.Fatalf("command = %q", cmd)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(b64, " | base64 -d | sh"))
	if err != nil || !strings.Contains(string(raw), "exit 3") {
		t.Fatalf("payload = %q, %v", raw, err)
	}
}

func TestScriptsAreValidShell(t *testing.T) {
	p := defaultPaths(testSessionID)
	scripts := map[string]string{
		"setup":     setupScript(p, envFile(map[string]string{"ANTHROPIC_AUTH_TOKEN": "x'y"}), "it's", turnScript(p, testSessionID, time.Hour)),
		"turn":      turnScript(p, testSessionID, 45*time.Minute),
		"deliver":   deliverScript(p, 7, "fix it; rm -rf / `whoami` $(id) 'quote'"),
		"poll":      pollScript(p, 1234, "4294967295"),
		"stop":      stopScript(p),
		"workspace": workspaceScript(p, "spawn/5be67472"),
		"env":       envScript(p, envFile(nil)),
		"ports":     portsScript(p, []Port{{5173, "0.0.0.0"}}),
		"published": publishedScript(p),
	}
	for _, sh := range shells(t) {
		for name, script := range scripts {
			if out, err := exec.Command(sh, "-n", "-c", script).CombinedOutput(); err != nil {
				t.Errorf("%s: %s script: %v\n%s", sh, name, err, out)
			}
		}
	}
}

// The turn wrapper must run exactly the approved Claude Code invocation.
func TestTurnScriptInvocation(t *testing.T) {
	script := turnScript(defaultPaths(testSessionID), testSessionID, 45*time.Minute)
	for _, want := range []string{
		`timeout "$turn_s" claude -p "$(cat "$base/prompts/$seq.txt")" --output-format stream-json --verbose "$mode" "$sid" --permission-mode auto --max-turns 200 --append-system-prompt "$(cat "$base/system.txt")" </dev/null >>"$ev" 2>>"$base/stderr.log"`,
		"turn_s=2700",
		"base='/var/lib/spawn/agent/" + testSessionID + "'",
		"repo='/workspace/repo'",
		`mode=--resume; else mode=--session-id`,
		`. "$base/env"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("turn script lacks %q", want)
		}
	}
	if strings.Contains(script, "dangerously") {
		t.Fatal("turn script skips permissions")
	}
}

func TestEnvFile(t *testing.T) {
	env := envFile(map[string]string{"ANTHROPIC_AUTH_TOKEN": "tok'en", "ANTHROPIC_BASE_URL": "http://172.16.0.1:7081"})
	for _, want := range []string{
		"export TMPDIR='/var/tmp/spawn'\n", "export GOTMPDIR='/var/tmp/spawn'\n", "export GOTOOLCHAIN='auto'\n",
		"export HOME='/root'\n", "export ANTHROPIC_AUTH_TOKEN='tok'\\''en'\n", "export ANTHROPIC_BASE_URL='http://172.16.0.1:7081'\n",
	} {
		if !strings.Contains(env, want) {
			t.Errorf("env lacks %q:\n%s", want, env)
		}
	}
}

func TestSystemPrompt(t *testing.T) {
	s := &Session{ID: testSessionID, SourceKind: "issue", SourceNumber: 2, Title: "Validate\n events"}
	prompt := systemPrompt(s, "spawn/5be67472", "dev-sandbox.renderops.com")
	for _, want := range []string{
		"/workspace/repo on branch spawn/5be67472", "GitHub issue #2: Validate events", "cannot push",
		"setsid nohup", "0.0.0.0", "server.allowedHosts: ['.dev-sandbox.renderops.com']", "Port 3001", "TMPDIR",
		"Never print environment variables",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("system prompt lacks %q", want)
		}
	}
	pr := systemPrompt(&Session{ID: testSessionID, SourceKind: "pull_request", SourceNumber: 9, Title: "Add x"}, "spawn/5be67472", "d")
	if !strings.Contains(pr, "/review/pr.diff") {
		t.Fatal("PR system prompt does not point at the diff")
	}
}

func TestSetupScriptWritesPrivateFilesIdempotently(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		report := g.setup()
		if report.WorkspaceReady || report.Size != 0 {
			t.Fatalf("%s: first setup = %+v", sh, report)
		}
		for name, mode := range map[string]os.FileMode{"env": 0o600, "system.txt": 0o600, "turn.sh": 0o700} {
			info, err := os.Stat(filepath.Join(g.paths.Base, name))
			if err != nil || info.Mode().Perm() != mode {
				t.Fatalf("%s: %s mode %v, err %v", sh, name, info.Mode().Perm(), err)
			}
		}
		if info, err := os.Stat(g.paths.Base); err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("%s: agent dir mode %v", sh, info.Mode().Perm())
		}
		if g.read(filepath.Join(g.paths.Base, "system.txt")) != "system prompt\n" {
			t.Fatalf("%s: system.txt", sh)
		}

		g.write(filepath.Join(g.paths.Base, "events.jsonl"), "{}\n")
		g.write(filepath.Join(g.paths.Base, "workspace.ok"), "abc\n")
		if err := os.MkdirAll(filepath.Join(g.paths.Repo, ".git"), 0o755); err != nil {
			t.Fatal(err)
		}
		if report := g.setup(); !report.WorkspaceReady || report.Size != 3 {
			t.Fatalf("%s: second setup = %+v", sh, report)
		}
		if g.events() != "{}\n" {
			t.Fatalf("%s: setup touched the journal", sh)
		}
	}
}

// Ports the runner published survive in the guest for the next claim.
func TestPublishedPortsRoundTrip(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		if report := g.setup(); len(report.Published) != 0 {
			t.Fatalf("%s: fresh guest has ports %v", sh, report.Published)
		}
		ports := []Port{{5173, "0.0.0.0"}, {8080, "::"}, {3000, "127.0.0.1"}}
		if _, stderr, code := g.run(portsScript(g.paths, ports)); code != 0 {
			t.Fatalf("%s: record exit %d: %s", sh, code, stderr)
		}
		if info, err := os.Stat(filepath.Join(g.paths.Base, "ports.published")); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s: ports file %v %v", sh, info, err)
		}
		if report := g.setup(); fmt.Sprint(report.Published) != fmt.Sprint(ports) {
			t.Fatalf("%s: setup reports %v", sh, report.Published)
		}
		out, _, code := g.run(publishedScript(g.paths))
		if code != 0 || fmt.Sprint(parsePublished(out)) != fmt.Sprint(ports) {
			t.Fatalf("%s: published = %q %d", sh, out, code)
		}
		if _, _, code := g.run(portsScript(g.paths, nil)); code != 0 {
			t.Fatalf("%s: clearing failed", sh)
		}
		if out, _, _ := g.run(publishedScript(g.paths)); len(parsePublished(out)) != 0 {
			t.Fatalf("%s: cleared ports still reported: %q", sh, out)
		}
	}
}

// fakeClaude records its arguments, writes a stream-json turn and, like the
// real CLI, creates the session transcript the wrapper checks for --resume.
const fakeClaude = `
printf '%s\n' "$*" >> "$HOME/claude-args"
[ -t 0 ] && echo "stdin is a terminal" >&2
read -r line && echo "stdin was not empty" >&2
mkdir -p "$HOME/.claude/projects/-workspace-repo"
: > "$HOME/.claude/projects/-workspace-repo/$7.jsonl"
printf '%s\n' '{"type":"system","subtype":"init","model":"claude-opus-5-5"}'
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"Done."}]}}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false}'
`

func TestDeliverRunsOneTurnPerSeqAndResumes(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		g.script("claude", fakeClaude)
		g.setup()

		out, stderr, code := g.run(deliverScript(g.paths, 2, "Fix issue #2"))
		if code != 0 || strings.TrimSpace(out) != "started" {
			t.Fatalf("%s: deliver = %q %q %d", sh, out, stderr, code)
		}
		waitFor(t, func() bool { return strings.Contains(g.events(), `"event":"turn_end","seq":2`) })
		if g.read(filepath.Join(g.paths.Base, "prompts", "2.txt")) != "Fix issue #2" {
			t.Fatalf("%s: prompt file", sh)
		}
		// The same seq never starts a second turn.
		if out, _, _ := g.run(deliverScript(g.paths, 2, "Fix issue #2")); strings.TrimSpace(out) != "already" {
			t.Fatalf("%s: redeliver = %q", sh, out)
		}

		if out, _, _ := g.run(deliverScript(g.paths, 5, "-also add a test")); strings.TrimSpace(out) != "started" {
			t.Fatalf("%s: second deliver = %q", sh, out)
		}
		waitFor(t, func() bool { return strings.Contains(g.events(), `"event":"turn_end","seq":5`) })

		args := strings.Split(strings.TrimSpace(g.read(filepath.Join(g.home, "claude-args"))), "\n")
		if len(args) != 2 {
			t.Fatalf("%s: claude ran %d times: %q", sh, len(args), args)
		}
		if !strings.Contains(args[0], "--session-id "+testSessionID) || !strings.Contains(args[1], "--resume "+testSessionID) {
			t.Fatalf("%s: args = %q", sh, args)
		}
		for _, a := range args {
			for _, want := range []string{"-p", "--output-format stream-json --verbose", "--permission-mode auto", "--max-turns 200", "--append-system-prompt system prompt"} {
				if !strings.Contains(a, want) {
					t.Fatalf("%s: args %q lack %q", sh, a, want)
				}
			}
		}
		// fakeClaude complains on stderr unless stdin is /dev/null.
		if log := g.read(filepath.Join(g.paths.Base, "stderr.log")); log != "" {
			t.Fatalf("%s: claude stderr: %q", sh, log)
		}
		if _, err := os.Stat(filepath.Join(g.paths.Base, "turn.pgid")); !os.IsNotExist(err) {
			t.Fatalf("%s: turn.pgid left behind", sh)
		}

		events, d := decodeAll(t, []byte(g.events()))
		if d.Busy() || d.Offset() != int64(len(g.events())) {
			t.Fatalf("%s: journal does not decode cleanly", sh)
		}
		var kinds []string
		for _, e := range events {
			kinds = append(kinds, e.Type)
		}
		want := "status step message status step message"
		if got := strings.Join(kinds, " "); got != want {
			t.Fatalf("%s: events %q, want %q", sh, got, want)
		}
	}
}

func TestTurnScriptReportsTimeoutAndStop(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		g.script("claude", "exit 124")
		g.setup()
		if out, _, _ := g.run(deliverScript(g.paths, 3, "slow")); strings.TrimSpace(out) != "started" {
			t.Fatalf("%s: deliver = %q", sh, out)
		}
		waitFor(t, func() bool { return strings.Contains(g.events(), `"seq":3,"exit":124,"reason":"timeout"`) })

		// A turn the user stopped reports "interrupted" even when the
		// wrapper itself gets to write turn_end.
		g.script("claude", "exit 143")
		if err := os.MkdirAll(filepath.Join(g.paths.Base, "turns", "4"), 0o700); err != nil {
			t.Fatal(err)
		}
		g.write(filepath.Join(g.paths.Base, "turns", "4", "stopped"), "")
		g.write(filepath.Join(g.paths.Base, "prompts", "4.txt"), "x")
		cmd := exec.Command(sh, filepath.Join(g.paths.Base, "turn.sh"), "4")
		cmd.Env = append(os.Environ(), "PATH="+g.bin+":"+os.Getenv("PATH"))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: turn.sh: %v %s", sh, err, out)
		}
		if !strings.Contains(g.events(), `"seq":4,"exit":143,"reason":"interrupted"`) {
			t.Fatalf("%s: journal %q", sh, g.events())
		}
	}
}

func TestDeliverReportsAWrapperThatNeverStarts(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		g.setup()
		g.script("setsid", "exit 0") // the wrapper never runs
		out, _, code := g.run(deliverScript(g.paths, 2, "hi"))
		if code != 0 || strings.TrimSpace(out) != "start_failed" {
			t.Fatalf("%s: deliver = %q %d", sh, out, code)
		}
		if !strings.Contains(g.events(), `"seq":2,"exit":127,"reason":"start_failed"`) {
			t.Fatalf("%s: journal %q", sh, g.events())
		}
	}
}

// procStat writes a fake /proc/<pid>/stat line (comm with spaces and parens,
// like the kernel's), with state, session and starttime (field 22).
func (g *guestEnv) procStat(pid int, state string, session, start int) {
	fields := []string{state, "1", fmt.Sprint(session), fmt.Sprint(session)}
	for len(fields) < 19 {
		fields = append(fields, "0")
	}
	fields = append(fields, fmt.Sprint(start))
	g.write(filepath.Join(g.paths.Proc, fmt.Sprint(pid), "stat"), fmt.Sprintf("%d (sh (x) y) %s\n", pid, strings.Join(fields, " ")))
}

const tcpHeader = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"

func TestPollReportsChangesAndExits10WhenIdle(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		g.setup()
		journal := `{"type":"spawn","event":"turn_start","seq":2}` + "\n" + `{"partial`
		g.write(filepath.Join(g.paths.Base, "events.jsonl"), journal)
		g.write(filepath.Join(g.paths.Proc, "net", "tcp"), tcpHeader+
			"   0: 00000000:1435 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1\n"+
			"   1: 0100007F:0BB9 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 2\n"+
			"   2: 0100007F:1F90 0100007F:D1A2 01 00000000:00000000 00:00000000 00000000     0        0 3\n")
		g.write(filepath.Join(g.paths.Proc, "net", "tcp6"), tcpHeader+
			"   0: 00000000000000000000000000000000:1435 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 4\n")
		g.procStat(4242, "S", 4242, 777)
		g.write(filepath.Join(g.paths.Base, "turn.pgid"), "4242 2 777\n")
		if err := os.MkdirAll(filepath.Join(g.paths.Base, "turns", "2"), 0o700); err != nil {
			t.Fatal(err)
		}

		out, stderr, code := g.run(pollScript(g.paths, 0, ""))
		if code != 0 {
			t.Fatalf("%s: poll exit %d: %s", sh, code, stderr)
		}
		res, err := parsePoll(out)
		if err != nil {
			t.Fatalf("%s: %v\n%s", sh, err, out)
		}
		if !res.Changed || !res.Alive || res.Size != int64(len(journal)) || res.LastTurn != 2 || res.From != 0 ||
			string(res.Data) != journal || res.Full {
			t.Fatalf("%s: poll = %+v", sh, res)
		}
		// Listening sockets only (state 0A), in any order.
		got := map[Port]bool{}
		for _, p := range res.Ports {
			got[p] = true
		}
		want := map[Port]bool{{5173, "0.0.0.0"}: true, {3001, "127.0.0.1"}: true, {5173, "::"}: true}
		if len(res.Ports) != 3 || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: ports = %v", sh, res.Ports)
		}

		// Same size and ports: nothing to report.
		out, _, code = g.run(pollScript(g.paths, res.Size, res.PortsSig))
		if code != exitNoChange || out != "" {
			t.Fatalf("%s: idle poll = %d %q", sh, code, out)
		}
		// Only the ports changed.
		g.write(filepath.Join(g.paths.Proc, "net", "tcp6"), tcpHeader)
		out, _, code = g.run(pollScript(g.paths, res.Size, res.PortsSig))
		res2, err := parsePoll(out)
		if code != 0 || err != nil || len(res2.Data) != 0 || len(res2.Ports) != 2 || res2.PortsSig == res.PortsSig {
			t.Fatalf("%s: ports-only poll = %d %+v %v", sh, code, res2, err)
		}
	}
}

func TestPollSynthesizesTurnEndForADeadWrapper(t *testing.T) {
	cases := map[string]func(g *guestEnv){
		"zombie":        func(g *guestEnv) { g.procStat(4242, "Z", 4242, 777) },
		"gone":          func(g *guestEnv) {},
		"pid reused":    func(g *guestEnv) { g.procStat(4242, "S", 4242, 999) },
		"garbage pgid":  func(g *guestEnv) { g.write(filepath.Join(g.paths.Base, "turn.pgid"), "x;rm 3 777\n") },
		"dead (status)": func(g *guestEnv) { g.procStat(4242, "X", 4242, 777) },
	}
	for _, sh := range shells(t) {
		for name, prepare := range cases {
			g := newGuest(t, sh)
			g.setup()
			g.write(filepath.Join(g.paths.Base, "events.jsonl"), `{"type":"spawn","event":"turn_start","seq":3}`+"\n")
			g.write(filepath.Join(g.paths.Base, "turn.pgid"), "4242 3 777\n")
			prepare(g)
			out, stderr, code := g.run(pollScript(g.paths, 0, ""))
			res, err := parsePoll(out)
			if code != 0 || err != nil {
				t.Fatalf("%s/%s: poll %d %v %s", sh, name, code, err, stderr)
			}
			if res.Alive {
				t.Fatalf("%s/%s: dead wrapper reported alive", sh, name)
			}
			if _, err := os.Stat(filepath.Join(g.paths.Base, "turn.pgid")); !os.IsNotExist(err) {
				t.Fatalf("%s/%s: turn.pgid kept", sh, name)
			}
			events, _ := decodeAll(t, res.Data)
			if len(events) != 2 || events[1].Payload["text"] != "The agent process exited unexpectedly. Send a message to continue." {
				t.Fatalf("%s/%s: events = %+v (journal %q)", sh, name, events, g.events())
			}
		}
	}
}

func TestPollReturnsAtMostOneChunkIn76ColumnLines(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		g.setup()
		line := strings.Repeat("a", 999) + "\n"
		journal := strings.Repeat(line, 300) // 300 KB
		g.write(filepath.Join(g.paths.Base, "events.jsonl"), journal)
		out, _, code := g.run(pollScript(g.paths, 1000, ""))
		if code != 0 {
			t.Fatalf("%s: exit %d", sh, code)
		}
		for _, l := range strings.Split(out, "\n") {
			if len(l) > 76 {
				t.Fatalf("%s: %d-column output line", sh, len(l))
			}
		}
		res, err := parsePoll(out)
		if err != nil {
			t.Fatal(err)
		}
		if res.From != 1000 || len(res.Data) != pollChunk || !res.Full || string(res.Data) != journal[1000:1000+pollChunk] {
			t.Fatalf("%s: chunk from %d, %d bytes, full %v", sh, res.From, len(res.Data), res.Full)
		}
	}
}

func TestStopKillsTheTurnAndMarksIt(t *testing.T) {
	for _, sh := range shells(t) {
		g := newGuest(t, sh)
		g.setup()
		// A real process group standing in for the wrapper and Claude Code.
		turn := exec.Command(sh, "-c", "sleep 60 & sleep 60; wait")
		turn.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := turn.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- turn.Wait() }()
		pid := turn.Process.Pid
		g.write(filepath.Join(g.paths.Base, "turn.pgid"), fmt.Sprintf("%d 6 \n", pid))

		out, stderr, code := g.run(stopScript(g.paths))
		if code != 0 || strings.TrimSpace(out) != "stopped" {
			t.Fatalf("%s: stop = %q %q %d", sh, out, stderr, code)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			t.Fatalf("%s: the turn survived stop", sh)
		}
		if _, err := os.Stat(filepath.Join(g.paths.Base, "turns", "6", "stopped")); err != nil {
			t.Fatalf("%s: turn not marked stopped", sh)
		}
		if !strings.Contains(g.events(), `"seq":6,"exit":130,"reason":"interrupted"`) {
			t.Fatalf("%s: journal %q", sh, g.events())
		}
		if out, _, _ := g.run(stopScript(g.paths)); strings.TrimSpace(out) != "idle" {
			t.Fatalf("%s: second stop = %q", sh, out)
		}
	}
}

func TestStopFindsSessionMembersThroughProc(t *testing.T) {
	sh := shells(t)[0]
	g := newGuest(t, sh)
	g.setup()
	g.write(filepath.Join(g.paths.Base, "turn.pgid"), "4242 8 1\n")
	// A zombie member is not waited for; the stop completes at once.
	g.procStat(4243, "Z", 4242, 5)
	start := time.Now()
	out, _, code := g.run(stopScript(g.paths))
	if code != 0 || strings.TrimSpace(out) != "stopped" || time.Since(start) > 5*time.Second {
		t.Fatalf("stop = %q %d after %s", out, code, time.Since(start))
	}
}

func TestWorkspaceScriptCreatesTheSessionBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	sh := shells(t)[0]
	g := newGuest(t, sh)
	g.setup()
	gitRun := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", g.paths.Repo, "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	gitRun("init", "-q")
	g.write(filepath.Join(g.paths.Repo, "README.md"), "hi\n")
	gitRun("add", ".")
	gitRun("commit", "-q", "-m", "init")
	head := gitRun("rev-parse", "HEAD")
	gitRun("checkout", "-q", "--detach", head)

	for i := 0; i < 2; i++ { // idempotent
		out, stderr, code := g.run(workspaceScript(g.paths, "spawn/5be67472"))
		if code != 0 || strings.TrimSpace(out) != "head "+head {
			t.Fatalf("run %d: %q %q %d", i, out, stderr, code)
		}
	}
	if branch := gitRun("symbolic-ref", "--short", "HEAD"); branch != "spawn/5be67472" {
		t.Fatalf("branch = %q", branch)
	}
	if name, err := exec.Command("git", "-C", g.paths.Repo, "config", "--local", "user.name").Output(); err != nil || strings.TrimSpace(string(name)) != "Spawn Agent" {
		t.Fatalf("user.name = %q, %v", name, err)
	}
	if ok := g.read(filepath.Join(g.paths.Base, "workspace.ok")); strings.TrimSpace(ok) != head {
		t.Fatalf("workspace.ok = %q", ok)
	}
}

func TestParseListenAddr(t *testing.T) {
	cases := map[string]Port{
		"00000000:1435":                         {5173, "0.0.0.0"},
		"0100007F:0BB9":                         {3001, "127.0.0.1"},
		"0A00020F:0050":                         {80, "15.2.0.10"},
		"00000000000000000000000000000000:1F90": {8080, "::"},
		"00000000000000000000000001000000:1F90": {8080, "::1"},
		"0000000000000000FFFF00000100007F:1F90": {8080, "127.0.0.1"},
	}
	for in, want := range cases {
		got, ok := parseListenAddr(in)
		if !ok || got != want {
			t.Errorf("%s = %+v %v, want %+v", in, got, ok, want)
		}
	}
	for _, bad := range []string{"", "zz:1", "00000000:0000", "000000:0050", "00000000"} {
		if _, ok := parseListenAddr(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestParsePollRejectsBrokenOutput(t *testing.T) {
	for _, out := range []string{
		"", "size 1\n", "spawn-poll 1\nsize x\nsig 1\n", "spawn-poll 1\nsig 1\ndata 0\nQUJD\n",
		"spawn-poll 1\nsig 1\ndata 0\n!!!\nend\n",
	} {
		if _, err := parsePoll(out); err == nil {
			t.Errorf("accepted %q", out)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the guest")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
