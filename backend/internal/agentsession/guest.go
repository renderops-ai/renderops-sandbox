package agentsession

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Guest layout (all inside the session's sandbox):
//
//	/var/lib/spawn/agent/<sid>/   0700, root
//	  env            0600  model-access env + TMPDIR/GOTMPDIR/GOTOOLCHAIN
//	  system.txt           appended system prompt
//	  turn.sh        0700  the turn wrapper
//	  prompts/<seq>.txt    the prompt of the turn for user message <seq>
//	  turns/<seq>/         mkdir = the turn's start lock; pid, stopped
//	  events.jsonl         the journal: stream-json + spawn markers
//	  stderr.log           Claude Code's stderr (never published)
//	  turn.pgid            "<pid> <seq> <starttime>" while a turn runs
//	  workspace.ok         the checkout is done (HEAD at checkout)
//	  ports.published      "<port> <bind>" the runner last published as open
//
// Every script runs as `printf %s <base64> | base64 -d | sh`: quoting and
// `exit` never reach the guest's persistent shell. Each script body runs as a
// function with stdin from /dev/null, so no command can read the rest of the
// script from sh's stdin.
const (
	agentRoot   = "/var/lib/spawn/agent"
	guestHome   = "/root"
	guestTmpDir = "/var/tmp/spawn"
	maxTurns    = 200
	// pollChunk is the most journal one poll returns (base64 in 76-column
	// lines: one long line would overflow the guest shell's 64 KB scanner).
	pollChunk = 256 << 10
	// exitNoChange is the poll script's "nothing changed" exit status.
	exitNoChange = 10
)

// guestPaths are the guest locations scripts use. Tests point them at a
// temporary directory and a fake /proc.
type guestPaths struct {
	Base string // agentRoot/<sid>
	Repo string
	Proc string
	Tmp  string
	// StartTicks is how many 0.2 s ticks Deliver waits for the wrapper.
	StartTicks int
}

func defaultPaths(sessionID string) guestPaths {
	return guestPaths{Base: agentRoot + "/" + sessionID, Repo: defaultRepoDir, Proc: "/proc", Tmp: guestTmpDir, StartTicks: 50}
}

// shellCommand wraps a script for the guest's persistent shell.
func shellCommand(script string) string {
	return "printf %s " + base64.StdEncoding.EncodeToString([]byte(script)) + " | base64 -d | sh"
}

// scriptBody makes body a function run with stdin from /dev/null.
func scriptBody(body string) string {
	return "main() {\n" + body + "\n}\nmain </dev/null\n"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// setupScript writes the session's guest files (idempotent; env is rewritten
// on every claim so a new grant takes effect at the next turn) and reports
// whether the workspace is ready and the journal's size.
func setupScript(p guestPaths, env, system, turn string) string {
	return scriptBody(fmt.Sprintf(`set -e
umask 077
base=%s
mkdir -p "$base/prompts" "$base/turns" %s
chmod 700 "$base"
put() { printf %%s "$2" | base64 -d > "$1.tmp"; chmod "$3" "$1.tmp"; mv -f "$1.tmp" "$1"; }
put "$base/env" %s 600
put "$base/system.txt" %s 600
put "$base/turn.sh" %s 700
[ -f "$base/events.jsonl" ] || : > "$base/events.jsonl"
if [ -f "$base/workspace.ok" ] && [ -d %s/.git ]; then echo "workspace ok"; else echo "workspace missing"; fi
echo "size $(wc -c < "$base/events.jsonl" | tr -d ' ')"
%s`,
		shellQuote(p.Base), shellQuote(p.Tmp),
		b64(env), b64(system), b64(turn),
		shellQuote(p.Repo), printPublished))
}

// printPublished prints the ports the runner last published as open, so a
// later claim knows what the session shows (see portsScript).
const printPublished = `if [ -f "$base/ports.published" ]; then
	while read -r port bind; do [ -n "$port" ] && echo "published $port $bind"; done < "$base/ports.published"
fi
exit 0`

// portsScript records the ports the runner published as open.
func portsScript(p guestPaths, ports []Port) string {
	var b strings.Builder
	for _, port := range ports {
		fmt.Fprintf(&b, "%d %s\n", port.Port, port.Bind)
	}
	return scriptBody(fmt.Sprintf("umask 077\nf=%s\nprintf %%s %s | base64 -d > \"$f.tmp\" && mv -f \"$f.tmp\" \"$f\"",
		shellQuote(p.Base+"/ports.published"), b64(b.String())))
}

// publishedScript prints the recorded ports (Finish reads them before it
// pauses the sandbox).
func publishedScript(p guestPaths) string {
	return scriptBody(fmt.Sprintf("base=%s\n%s", shellQuote(p.Base), printPublished))
}

// parsePublished reads "published <port> <bind>" lines.
func parsePublished(stdout string) []Port {
	var ports []Port
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "published" {
			continue
		}
		n, err := strconv.Atoi(fields[1])
		if err != nil || n <= 0 || n > 65535 || net.ParseIP(fields[2]) == nil {
			continue
		}
		ports = append(ports, Port{Port: n, Bind: fields[2]})
	}
	return ports
}

// envScript replaces the env file only (a grant changed between turns).
func envScript(p guestPaths, env string) string {
	return scriptBody(fmt.Sprintf("umask 077\nf=%s\nprintf %%s %s | base64 -d > \"$f.tmp\" && mv -f \"$f.tmp\" \"$f\"",
		shellQuote(p.Base+"/env"), b64(env)))
}

// envFile is the turn's environment. The model token in it works only from
// inside this sandbox (the worker's model proxy), and is never printed.
func envFile(model map[string]string) string {
	vars := map[string]string{
		"HOME":                guestHome,
		"TMPDIR":              guestTmpDir,
		"GOTMPDIR":            guestTmpDir,
		"GOTOOLCHAIN":         "auto",
		"DISABLE_AUTOUPDATER": "1",
	}
	for k, v := range model {
		vars[k] = v
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString("export " + k + "=" + shellQuote(vars[k]) + "\n")
	}
	return b.String()
}

// turnScript is the turn wrapper, started detached (setsid) per turn with the
// turn's seq. It records itself in turn.pgid, writes turn_start, runs Claude
// Code with stdin from /dev/null (claude -p otherwise waits for input) under
// timeout, then writes turn_end with the exit status. --resume continues the
// session's conversation once it exists.
func turnScript(p guestPaths, sessionID string, turnTimeout time.Duration) string {
	return fmt.Sprintf(`#!/bin/sh
base=%s
sid=%s
repo=%s
proc=%s
turn_s=%d
seq=$1
ev="$base/events.jsonl"
start=
st=$(cat "$proc/$$/stat" 2>/dev/null || true)
if [ -n "$st" ]; then st=${st##*) }; set -- $st; start=${20:-}; fi
printf '%%s %%s %%s\n' "$$" "$seq" "$start" > "$base/turn.pgid.tmp"
mv -f "$base/turn.pgid.tmp" "$base/turn.pgid"
printf '\n{"type":"spawn","event":"turn_start","seq":%%s}\n' "$seq" >> "$ev"
echo "$$" > "$base/turns/$seq/pid"
. "$base/env"
if cd "$repo" 2>>"$base/stderr.log"; then
	if ls "$HOME"/.claude/projects/*/"$sid.jsonl" >/dev/null 2>&1; then mode=--resume; else mode=--session-id; fi
	timeout "$turn_s" claude -p "$(cat "$base/prompts/$seq.txt")" --output-format stream-json --verbose "$mode" "$sid" --permission-mode auto --max-turns %d --append-system-prompt "$(cat "$base/system.txt")" </dev/null >>"$ev" 2>>"$base/stderr.log"
	code=$?
else
	code=126
fi
reason=
if [ -f "$base/turns/$seq/stopped" ]; then reason=interrupted; elif [ "$code" = 124 ] || [ "$code" = 143 ]; then reason=timeout; fi
printf '\n{"type":"spawn","event":"turn_end","seq":%%s,"exit":%%s,"reason":"%%s"}\n' "$seq" "$code" "$reason" >> "$ev"
rm -f "$base/turn.pgid"
`, shellQuote(p.Base), shellQuote(sessionID), shellQuote(p.Repo), shellQuote(p.Proc),
		int(turnTimeout/time.Second), maxTurns)
}

// deliverScript starts the turn for user message seq, at most once: the
// turns/<seq> mkdir is the lock. It waits until the wrapper has recorded
// itself, so the next poll sees the turn.
func deliverScript(p guestPaths, seq int64, prompt string) string {
	return scriptBody(fmt.Sprintf(`base=%s
seq=%d
mkdir -p "$base/prompts" "$base/turns"
if ! mkdir "$base/turns/$seq" 2>/dev/null; then echo "already"; exit 0; fi
printf %%s %s | base64 -d > "$base/prompts/$seq.txt"
setsid sh "$base/turn.sh" "$seq" </dev/null >/dev/null 2>&1 &
i=0
while [ ! -f "$base/turns/$seq/pid" ] && [ "$i" -lt %d ]; do sleep 0.2 2>/dev/null || sleep 1; i=$((i + 1)); done
if [ -f "$base/turns/$seq/pid" ]; then echo "started"; exit 0; fi
printf '\n{"type":"spawn","event":"turn_end","seq":%%s,"exit":127,"reason":"start_failed"}\n' "$seq" >> "$base/events.jsonl"
echo "start_failed"`, shellQuote(p.Base), seq, b64(prompt), max(p.StartTicks, 1)))
}

// pollScript reports what changed since cursor and the last ports signature.
// It exits 10 with no output when nothing did. A turn whose wrapper died
// without writing turn_end gets a synthesized turn_end (reason "lost"); guest
// PID 1 does not reap, so a dead wrapper is a zombie, not a missing pid.
func pollScript(p guestPaths, cursor int64, lastSig string) string {
	return scriptBody(fmt.Sprintf(`base=%s
proc=%s
cursor=%d
lastsig=%s
ev="$base/events.jsonl"
[ -d "$base" ] || { echo "agent directory is missing" >&2; exit 3; }
alive=0
if [ -f "$base/turn.pgid" ]; then
	pgid= seq= start=
	read -r pgid seq start < "$base/turn.pgid" || true
	case "$seq" in ''|*[!0-9]*) seq=0;; esac
	st=
	case "$pgid" in ''|*[!0-9]*) ;; *) st=$(cat "$proc/$pgid/stat" 2>/dev/null || true);; esac
	if [ -n "$st" ]; then
		st=${st##*) }
		set -- $st
		if [ "$1" != Z ] && [ "$1" != X ] && { [ -z "$start" ] || [ "${20:-}" = "$start" ]; }; then alive=1; fi
	fi
	if [ "$alive" = 0 ]; then
		printf '\n{"type":"spawn","event":"turn_end","seq":%%s,"exit":-1,"reason":"lost"}\n' "$seq" >> "$ev"
		rm -f "$base/turn.pgid"
	fi
fi
size=0
[ -f "$ev" ] && size=$(wc -c < "$ev" | tr -d ' ')
ports=$(for f in "$proc/net/tcp" "$proc/net/tcp6"; do [ -r "$f" ] && awk 'FNR > 1 && $4 == "0A" { print $2 }' "$f"; done | sort -u)
sig=$(printf '%%s\n' "$ports" | cksum | awk '{ print $1 }')
if [ "$size" = "$cursor" ] && [ "$sig" = "$lastsig" ]; then exit %d; fi
last=$(ls "$base/turns" 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -n 1)
echo "spawn-poll 1"
echo "size $size"
echo "alive $alive"
echo "turn ${last:-0}"
echo "sig $sig"
for p in $ports; do echo "port $p"; done
if [ "$size" -gt "$cursor" ]; then
	echo "data $cursor"
	tail -c +$((cursor + 1)) "$ev" | head -c %d | base64 | tr -d '\n' | fold -w 76
	echo
	echo "end"
fi
exit 0`, shellQuote(p.Base), shellQuote(p.Proc), cursor, shellQuote(lastSig), exitNoChange, pollChunk))
}

// stopScript interrupts the running turn: TERM to the wrapper's process group
// and every process of its session (GNU timeout moves itself and Claude Code
// into a group of their own), 3 s, then KILL. Servers the agent started with
// setsid are in their own session and keep running. It marks the turn
// stopped so the wrapper (or this script) writes turn_end "interrupted".
func stopScript(p guestPaths) string {
	return scriptBody(fmt.Sprintf(`base=%s
proc=%s
[ -f "$base/turn.pgid" ] || { echo "idle"; exit 0; }
pgid= seq= start=
read -r pgid seq start < "$base/turn.pgid" || true
case "$pgid" in ''|*[!0-9]*) rm -f "$base/turn.pgid"; echo "idle"; exit 0;; esac
case "$seq" in ''|*[!0-9]*) seq=0;; esac
mkdir -p "$base/turns/$seq"
: > "$base/turns/$seq/stopped"
members() {
	for d in "$proc"/[0-9]*; do
		s=$(cat "$d/stat" 2>/dev/null) || continue
		s=${s##*) }
		set -- $s
		if [ "$1" != Z ] && [ "$4" = "$pgid" ]; then echo "${d##*/}"; fi
	done
}
signal() {
	kill -s "$1" -- "-$pgid" 2>/dev/null
	for m in $(members); do kill -s "$1" "$m" 2>/dev/null; done
}
signal TERM
i=0
while [ -n "$(members)" ] && [ "$i" -lt 15 ]; do sleep 0.2 2>/dev/null || sleep 1; i=$((i + 1)); done
signal KILL
if [ -f "$base/turn.pgid" ]; then
	printf '\n{"type":"spawn","event":"turn_end","seq":%%s,"exit":130,"reason":"interrupted"}\n' "$seq" >> "$base/events.jsonl"
	rm -f "$base/turn.pgid"
fi
echo "stopped"`, shellQuote(p.Base), shellQuote(p.Proc)))
}

// workspaceScript puts the fresh checkout on the session's branch, sets a
// commit identity and marks the workspace ready.
func workspaceScript(p guestPaths, branch string) string {
	return scriptBody(fmt.Sprintf(`set -e
base=%s
branch=%s
cd %s
g() { git -c core.hooksPath=/dev/null -c advice.detachedHead=false "$@"; }
if g rev-parse -q --verify "refs/heads/$branch" >/dev/null; then
	[ "$(g symbolic-ref -q --short HEAD || true)" = "$branch" ] || g switch -q "$branch"
else
	g switch -q -c "$branch"
fi
g config user.name 'Spawn Agent'
g config user.email 'agent@spawn.local'
head=$(g rev-parse HEAD)
printf '%%s\n' "$head" > "$base/workspace.ok"
echo "head $head"`, shellQuote(p.Base), shellQuote(branch), shellQuote(p.Repo)))
}

func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// systemPrompt is appended to Claude Code's system prompt for every turn.
func systemPrompt(s *Session, branch, previewDomain string) string {
	var b strings.Builder
	b.WriteString("You are a coding agent working inside a Spawn sandbox: an isolated Firecracker microVM (Alpine Linux, 2 vCPU, 4 GB RAM) created for this session only.\n\n")
	b.WriteString("Workspace\n")
	fmt.Fprintf(&b, "- The repository is checked out at %s on branch %s.\n", defaultRepoDir, branch)
	switch s.SourceKind {
	case "issue":
		fmt.Fprintf(&b, "- You are working on GitHub issue #%d: %s\n", s.SourceNumber, oneLine(s.Title))
	case "pull_request":
		fmt.Fprintf(&b, "- This session is about pull request #%d: %s. The checkout is the pull request's head; its diff against the base is in /review/pr.diff and /review/changed-files.txt lists the changed files.\n", s.SourceNumber, oneLine(s.Title))
	}
	b.WriteString("- You may edit files, run commands and install tools (apk add, npm, pip, go). You run as root.\n")
	fmt.Fprintf(&b, "- /tmp is a small 64 MB tmpfs. TMPDIR and GOTMPDIR point to %s; keep large temporary files on disk. GOTOOLCHAIN=auto lets go download the toolchain go.mod asks for.\n\n", guestTmpDir)
	b.WriteString("Limits\n")
	fmt.Fprintf(&b, "- There are no GitHub credentials and you cannot push. When the work is done, commit it on the current branch (%s) with a clear message. Stay on that branch: don't create, switch to or rebase onto other branches.\n", branch)
	fmt.Fprintf(&b, "- The user publishes your commits on %s with the Create PR button on the session page, which opens the pull request. Don't give them git push, gh or other manual publishing steps.\n", branch)
	b.WriteString("- No secrets are available to you. Never print environment variables or the files under /var/lib/spawn.\n")
	fmt.Fprintf(&b, "- Port %d is reserved for the user's VS Code. Never use it or stop what listens on it.\n\n", idePort)
	b.WriteString("Servers and previews\n")
	b.WriteString("- Start long-running servers detached so they outlive your command: setsid nohup <command> > /var/log/<name>.log 2>&1 &\n")
	fmt.Fprintf(&b, "- Listen on 0.0.0.0, not localhost, so the user's Preview tab can reach them. For Vite set server.host: true and server.allowedHosts: ['.%s'].\n\n", previewDomain)
	b.WriteString("Working style\n")
	b.WriteString("- Keep changes focused on the task. Run the relevant tests or checks before you finish and report exactly what you ran and the results.\n")
	return b.String()
}

func oneLine(s string) string {
	return clean(strings.Join(strings.Fields(s), " "), 300)
}

// setupReport is the setup script's answer.
type setupReport struct {
	WorkspaceReady bool
	Size           int64
	Published      []Port
}

func parseSetup(stdout string) (setupReport, error) {
	var r setupReport
	sawWorkspace, sawSize := false, false
	for _, line := range strings.Split(stdout, "\n") {
		switch {
		case line == "workspace ok":
			r.WorkspaceReady, sawWorkspace = true, true
		case line == "workspace missing":
			sawWorkspace = true
		case strings.HasPrefix(line, "size "):
			n, err := strconv.ParseInt(strings.TrimPrefix(line, "size "), 10, 64)
			if err != nil {
				return r, fmt.Errorf("setup reported a bad journal size %q", line)
			}
			r.Size, sawSize = n, true
		}
	}
	if !sawWorkspace || !sawSize {
		return r, errors.New("setup script output is incomplete")
	}
	r.Published = parsePublished(stdout)
	return r, nil
}

// parsePoll reads the poll script's report.
func parsePoll(stdout string) (*PollResult, error) {
	sc := bufio.NewScanner(strings.NewReader(stdout))
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	res := &PollResult{Changed: true}
	header := false
	var data strings.Builder
	inData, ended := false, false
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if inData {
			if line == "end" {
				inData, ended = false, true
				continue
			}
			data.WriteString(line)
			continue
		}
		key, value, _ := strings.Cut(line, " ")
		var err error
		switch key {
		case "spawn-poll":
			header = value == "1"
		case "size":
			res.Size, err = strconv.ParseInt(value, 10, 64)
		case "alive":
			res.Alive = value == "1"
		case "turn":
			res.LastTurn, err = strconv.ParseInt(value, 10, 64)
		case "sig":
			res.PortsSig = value
		case "port":
			if port, ok := parseListenAddr(value); ok {
				res.Ports = append(res.Ports, port)
			}
		case "data":
			res.From, err = strconv.ParseInt(value, 10, 64)
			inData = true
		}
		if err != nil {
			return nil, fmt.Errorf("poll reported a bad %s line %q", key, line)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read poll output: %w", err)
	}
	if !header || res.PortsSig == "" {
		return nil, errors.New("poll output has no header")
	}
	if inData {
		return nil, errors.New("poll output was cut off")
	}
	if ended {
		raw, err := base64.StdEncoding.DecodeString(data.String())
		if err != nil {
			return nil, fmt.Errorf("decode poll data: %w", err)
		}
		res.Data = raw
		res.Full = len(raw) >= pollChunk
	}
	return res, nil
}

// parseListenAddr reads a /proc/net/tcp{,6} local_address ("0100007F:0BB8").
func parseListenAddr(s string) (Port, bool) {
	host, portHex, ok := strings.Cut(s, ":")
	if !ok {
		return Port{}, false
	}
	port, err := strconv.ParseUint(portHex, 16, 16)
	if err != nil || port == 0 {
		return Port{}, false
	}
	raw, err := hex.DecodeString(host)
	if err != nil || (len(raw) != 4 && len(raw) != 16) {
		return Port{}, false
	}
	// The kernel prints each 32-bit word in host (little-endian) order.
	ip := make(net.IP, len(raw))
	for w := 0; w < len(raw); w += 4 {
		ip[w], ip[w+1], ip[w+2], ip[w+3] = raw[w+3], raw[w+2], raw[w+1], raw[w]
	}
	return Port{Port: int(port), Bind: ip.String()}, true
}
