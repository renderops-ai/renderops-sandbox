package sandboxfiles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/plane"
)

// The guest scripts run end to end, through Client, in two places:
//
//   - TestGuestScriptsLocal: the host's sh (dash on CI, bash elsewhere), GNU
//     or BSD tools and git, in a temporary directory. Always on.
//   - TestGuestScriptsAlpine: an Alpine container with busybox, bash and git,
//     the guest's toolbox. Each command is fed to `bash -s` on stdin, as the
//     guest agent feeds its persistent shell, and the real /workspace and
//     staging directory are used. It needs Docker and is opt-in:
//
//	SANDBOXFILES_DOCKER_IMAGE=alpine:3.20 go test ./internal/sandboxfiles -run Alpine
//
// (AGENTSESSION_DOCKER_IMAGE, which enables the agent session's busybox
// test, enables this one too.)

// harness runs guest commands and fixture scripts in one place.
type harness struct {
	ws      string
	staging string
	// shell runs stdin with a shell and returns its result.
	shell func(ctx context.Context, stdin string) (plane.ExecResult, error)
	// fixture runs a setup script with WS set to the workspace.
	fixture func(script string) (string, error)
	// serve makes files downloadable by the guest over http and returns
	// their base URL (extract's downloads).
	serve func(t *testing.T, files map[string][]byte) string
}

func TestGuestScriptsLocal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the guest scripts need a POSIX shell")
	}
	for _, tool := range []string{"sh", "git", "base64", "realpath"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is required: %v", tool, err)
		}
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(base, "workspace")
	home := filepath.Join(base, "home")
	for _, dir := range []string{ws, home} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{"HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"GIT_CONFIG_NOSYSTEM=1", "LC_ALL=C", "WS=" + ws}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") && !strings.HasPrefix(kv, "HOME=") && !strings.HasPrefix(kv, "XDG_CONFIG_HOME=") {
			env = append(env, kv)
		}
	}
	run := func(ctx context.Context, stdin string) (plane.ExecResult, error) {
		cmd := exec.CommandContext(ctx, "sh", "-s")
		cmd.Dir = base
		cmd.Env = env
		cmd.Stdin = strings.NewReader(stdin + "\n")
		return runCommand(cmd)
	}
	h := &harness{
		ws:      ws,
		staging: filepath.Join(base, "staging"),
		shell:   run,
		fixture: func(script string) (string, error) { return fixture(run, script) },
		serve:   serveLocal,
	}
	runScenarios(t, h)
}

func TestGuestScriptsAlpine(t *testing.T) {
	image := os.Getenv("SANDBOXFILES_DOCKER_IMAGE")
	if image == "" {
		image = os.Getenv("AGENTSESSION_DOCKER_IMAGE")
	}
	if image == "" {
		t.Skip("set SANDBOXFILES_DOCKER_IMAGE (for example alpine:3.20) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", image, "sleep", "1800").CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v\n%s", err, raw)
	}
	container := strings.TrimSpace(string(raw))
	t.Cleanup(func() {
		if out, err := exec.Command("docker", "rm", "-f", container).CombinedOutput(); err != nil {
			t.Logf("docker rm: %v\n%s", err, out)
		}
	})
	// The sandbox image's toolbox: busybox plus bash, git, python3 and curl.
	install := "apk add --no-cache git bash python3 curl >/dev/null; mkdir -p /workspace"
	if out, err := exec.CommandContext(ctx, "docker", "exec", container, "sh", "-c", install).CombinedOutput(); err != nil {
		t.Fatalf("install git and bash: %v\n%s", err, out)
	}
	run := func(shell string) func(context.Context, string) (plane.ExecResult, error) {
		return func(ctx context.Context, stdin string) (plane.ExecResult, error) {
			cmd := exec.CommandContext(ctx, "docker", "exec", "-i", "-e", "WS=/workspace", container, shell, "-s")
			cmd.Stdin = strings.NewReader(stdin + "\n")
			return runCommand(cmd)
		}
	}
	h := &harness{
		ws:      Workspace,
		staging: StagingDir,
		// The guest agent writes each command to its persistent bash's stdin.
		shell:   run("bash"),
		fixture: func(script string) (string, error) { return fixture(run("sh"), script) },
		serve:   serveInContainer(container),
	}
	runScenarios(t, h)
}

func runCommand(cmd *exec.Cmd) (plane.ExecResult, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	res := plane.ExecResult{Stdout: stdout.String(), Stderr: stderr.String()}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		res.ExitCode = int64(exitErr.ExitCode())
		return res, nil
	}
	return res, err
}

// fixturePrelude: fail fast, and commit as a test identity.
const fixturePrelude = `set -e
g() { git -c user.name=test -c user.email=test@example.com -c init.defaultBranch=main -c core.hooksPath=/dev/null "$@"; }
`

func fixture(run func(context.Context, string) (plane.ExecResult, error), script string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := run(ctx, fixturePrelude+script)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("fixture exited %d: %s", res.ExitCode, res.Stderr)
	}
	return res.Stdout, nil
}

// recorder is the Client's ExecFunc in these tests: it records every call and
// can fail or act (as the agent would) before a given call.
type recorder struct {
	h     *harness
	mu    sync.Mutex
	calls []recordedCall
	// before runs before call n (0-based); an error fails that call.
	before func(n int) error
}

type recordedCall struct {
	size    int
	passive bool
}

func (r *recorder) exec(ctx context.Context, command string, timeoutS int, opts plane.ExecOptions) (plane.ExecResult, error) {
	r.mu.Lock()
	n := len(r.calls)
	r.calls = append(r.calls, recordedCall{size: len(command), passive: opts.Passive})
	before := r.before
	r.mu.Unlock()
	if before != nil {
		if err := before(n); err != nil {
			return plane.ExecResult{}, err
		}
	}
	if timeoutS <= 0 {
		return plane.ExecResult{}, errors.New("every files command needs a timeout")
	}
	// Files output is file contents: it must never reach the log stream.
	if !opts.Quiet {
		return plane.ExecResult{}, errors.New("every files command must be quiet")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutS)*time.Second)
	defer cancel()
	return r.h.shell(ctx, command)
}

// reset returns the calls so far and starts counting again, without a hook.
func (r *recorder) reset() []recordedCall {
	return r.arm(nil)
}

// arm is reset that installs a hook for the calls that follow.
func (r *recorder) arm(before func(n int) error) []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := r.calls
	r.calls, r.before = nil, before
	return calls
}

func (h *harness) client(t *testing.T) (*Client, *recorder) {
	t.Helper()
	rec := &recorder{h: h}
	cfg := defaultConfig()
	cfg.Workspace, cfg.Staging, cfg.AllowHTTP = h.ws, h.staging, true
	return &Client{exec: rec.exec, cfg: cfg, log: slog.New(slog.DiscardHandler)}, rec
}

func (h *harness) mustFixture(t *testing.T, script string) string {
	t.Helper()
	out, err := h.fixture(script)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return out
}

func sum(content []byte) string {
	s := sha256.Sum256(content)
	return hex.EncodeToString(s[:])
}

func wantCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	var fe *Error
	if !errors.As(err, &fe) || fe.Code != code {
		t.Fatalf("err = %v, want code %s", err, code)
	}
	return fe
}

func runScenarios(t *testing.T, h *harness) {
	// A directory next to the workspace that symlinks must not reach.
	h.mustFixture(t, `out=$(dirname "$WS")/outside
mkdir -p "$out/dir"
printf 'secret\n' > "$out/secret.txt"
printf 'secret\n' > "$out/dir/secret.txt"`)

	t.Run("tree", func(t *testing.T) { treeScenario(t, h) })
	t.Run("read", func(t *testing.T) { readScenario(t, h) })
	t.Run("read rev", func(t *testing.T) { readRevScenario(t, h) })
	t.Run("write", func(t *testing.T) { writeScenario(t, h) })
	t.Run("large write", func(t *testing.T) { largeWriteScenario(t, h) })
	t.Run("changes", func(t *testing.T) { changesScenario(t, h) })
	t.Run("patch", func(t *testing.T) { patchScenario(t, h) })
	t.Run("extract", func(t *testing.T) { extractScenario(t, h) })
	t.Run("extract malicious", func(t *testing.T) { maliciousExtractScenario(t, h) })
}

func treeScenario(t *testing.T, h *harness) {
	h.mustFixture(t, `d=$WS/tree-git
mkdir -p "$d/src" "$d/node_modules/x"
cd "$d"
g init -q
printf a > README.md
printf b > src/main.go
printf 'debug.log\nnode_modules/\n' > .gitignore
g add .
g commit -qm init
printf ignored > debug.log
printf new > src/new.go
printf x > node_modules/x/i.js
rm README.md
mkdir nested
g -C nested init -q
printf n > nested/n.txt
p=$WS/tree-plain
mkdir -p "$p/sub" "$p/node_modules" "$p/build" "$p/dist" "$p/.venv" "$p/__pycache__"
printf a > "$p/a.txt"
printf b > "$p/sub/b.txt"
printf c > "$p/sub/build"
printf x > "$p/node_modules/x.js"
printf x > "$p/build/x.js"
printf x > "$p/dist/x.js"
printf x > "$p/.venv/x"
printf x > "$p/__pycache__/x.pyc"
ln -s a.txt "$p/link.txt"
ln -s "$(dirname "$WS")/outside" "$WS/tree-escape"`)
	c, rec := h.client(t)
	ctx := context.Background()

	tree, err := c.Tree(ctx, h.ws+"/tree-git")
	if err != nil {
		t.Fatal(err)
	}
	if !tree.Git || tree.Truncated || strings.Join(tree.Files, ",") != ".gitignore,src/main.go,src/new.go" {
		t.Fatalf("git tree = %+v", tree)
	}
	for _, call := range rec.reset() {
		if !call.passive {
			t.Fatal("tree must not count as sandbox activity")
		}
	}
	sub, err := c.Tree(ctx, h.ws+"/tree-git/src")
	if err != nil || strings.Join(sub.Files, ",") != "main.go,new.go" || sub.Root != h.ws+"/tree-git/src" {
		t.Fatalf("subdirectory tree = %+v, %v", sub, err)
	}

	plain, err := c.Tree(ctx, h.ws+"/tree-plain")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(plain.Files)
	if plain.Git || plain.Truncated || strings.Join(plain.Files, ",") != "a.txt,link.txt,sub/b.txt,sub/build" {
		t.Fatalf("plain tree = %+v", plain)
	}

	// Caps: the deleted README.md does not use up the git listing's room.
	c.cfg.TreeMax = 4
	if tree, err := c.Tree(ctx, h.ws+"/tree-git"); err != nil || tree.Truncated || len(tree.Files) != 3 {
		t.Fatalf("git tree at the cap = %+v, %v", tree, err)
	}
	c.cfg.TreeMax = 2
	if tree, err := c.Tree(ctx, h.ws+"/tree-git"); err != nil || !tree.Truncated || strings.Join(tree.Files, ",") != ".gitignore,src/main.go" {
		t.Fatalf("truncated git tree (tracked files first) = %+v, %v", tree, err)
	}
	if tree, err := c.Tree(ctx, h.ws+"/tree-plain"); err != nil || !tree.Truncated || len(tree.Files) != 2 {
		t.Fatalf("truncated plain tree = %+v, %v", tree, err)
	}
	c.cfg.TreeMax = MaxTreeEntries

	_, err = c.Tree(ctx, h.ws+"/tree-missing")
	wantCode(t, err, CodeNotFound)
	_, err = c.Tree(ctx, h.ws+"/tree-plain/a.txt")
	wantCode(t, err, CodeNotADirectory)
	_, err = c.Tree(ctx, h.ws+"/tree-escape")
	wantCode(t, err, CodeInvalidPath)
	if _, err := c.Tree(ctx, h.ws); err != nil {
		t.Fatalf("the workspace itself is a valid root: %v", err)
	}
}

func readScenario(t *testing.T, h *harness) {
	h.mustFixture(t, `d=$WS/read
out=$(dirname "$WS")/outside
mkdir -p "$d/sub"
cd "$d"
printf 'hello\n' > text.txt
printf '#!/bin/sh\n' > run.sh
chmod 755 run.sh
printf 'a\000b' > bin.dat
printf 'caf\351\n' > latin1.txt
: > empty.txt
head -c 2097152 /dev/zero | tr '\000' a > big-ok.txt
head -c 2097153 /dev/zero | tr '\000' a > big-no.txt
mkfifo pipe
ln -s text.txt link-in
ln -s "$out/secret.txt" link-out
ln -s "$out/dir" dirlink-out
ln -s "$out/nothere" dangling-out
printf q > "quote ' \" \$(id) *.txt"
nl=$(printf 'new\nline_'); printf n > "${nl%_}x"
tn=$(printf 'trailing\n_'); printf t > "${tn%_}"`)
	c, rec := h.client(t)
	ctx := context.Background()
	dir := h.ws + "/read/"

	text, err := c.Read(ctx, dir+"text.txt", "")
	if err != nil {
		t.Fatal(err)
	}
	if text.Content == nil || *text.Content != "hello\n" || text.Size != 6 || text.SHA256 != sum([]byte("hello\n")) ||
		text.Mode != "100644" || text.Binary || text.Path != dir+"text.txt" {
		t.Fatalf("text = %+v", text)
	}
	for _, call := range rec.reset() {
		if !call.passive {
			t.Fatal("reads must not count as sandbox activity")
		}
	}
	if f, err := c.Read(ctx, dir+"run.sh", ""); err != nil || f.Mode != "100755" {
		t.Fatalf("executable = %+v, %v", f, err)
	}
	for _, name := range []string{"bin.dat", "latin1.txt"} {
		f, err := c.Read(ctx, dir+name, "")
		if err != nil || !f.Binary || f.Content != nil || f.Size == 0 || len(f.SHA256) != 64 {
			t.Fatalf("%s = %+v, %v", name, f, err)
		}
	}
	if f, err := c.Read(ctx, dir+"bin.dat", ""); err != nil || f.SHA256 != sum([]byte("a\x00b")) {
		t.Fatalf("binary sha = %+v, %v", f, err)
	}
	if f, err := c.Read(ctx, dir+"empty.txt", ""); err != nil || f.Content == nil || *f.Content != "" || f.SHA256 != sum(nil) {
		t.Fatalf("empty = %+v, %v", f, err)
	}
	big := bytes.Repeat([]byte("a"), MaxFileBytes)
	if f, err := c.Read(ctx, dir+"big-ok.txt", ""); err != nil || f.Size != MaxFileBytes || f.SHA256 != sum(big) {
		t.Fatalf("2 MiB file = size %d, %v", f.Size, err)
	}
	_, err = c.Read(ctx, dir+"big-no.txt", "")
	if fe := wantCode(t, err, CodeFileTooLarge); fe.Size != MaxFileBytes+1 {
		t.Fatalf("too large size = %d", fe.Size)
	}

	_, err = c.Read(ctx, dir+"missing.txt", "")
	wantCode(t, err, CodeFileNotFound)
	for _, name := range []string{"sub", "pipe"} {
		_, err = c.Read(ctx, dir+name, "")
		wantCode(t, err, CodeNotAFile)
	}
	if f, err := c.Read(ctx, dir+"link-in", ""); err != nil || *f.Content != "hello\n" {
		t.Fatalf("symlink inside the workspace = %+v, %v", f, err)
	}
	for _, name := range []string{"link-out", "dirlink-out/secret.txt", "dangling-out"} {
		_, err = c.Read(ctx, dir+name, "")
		wantCode(t, err, CodeInvalidPath)
	}
	for name, want := range map[string]string{
		"quote ' \" $(id) *.txt": "q",
		"new\nlinex":             "n",
		"trailing\n":             "t",
	} {
		if f, err := c.Read(ctx, dir+name, ""); err != nil || *f.Content != want {
			t.Fatalf("%q = %+v, %v", name, f, err)
		}
	}
}

func readRevScenario(t *testing.T, h *harness) {
	base := strings.TrimSpace(h.mustFixture(t, `d=$WS/rev
mkdir -p "$d/gone"
cd "$d"
g init -q
printf 'v1\n' > f.txt
printf 'old\n' > gone/x.txt
printf '#!/bin/sh\n' > run.sh
chmod 755 run.sh
printf s > 'star*[x].txt'
g add .
g commit -qm one
base=$(git rev-parse HEAD)
printf 'v2\n' > f.txt
rm -r gone
printf n > new.txt
g add -A
g commit -qm two
echo "$base"`))
	if !ValidRev(base) {
		t.Fatalf("base = %q", base)
	}
	c, _ := h.client(t)
	ctx := context.Background()
	dir := h.ws + "/rev/"

	if f, err := c.Read(ctx, dir+"f.txt", base); err != nil || *f.Content != "v1\n" || f.Mode != "100644" {
		t.Fatalf("f.txt at base = %+v, %v", f, err)
	}
	if f, err := c.Read(ctx, dir+"f.txt", ""); err != nil || *f.Content != "v2\n" {
		t.Fatalf("f.txt now = %+v, %v", f, err)
	}
	if f, err := c.Read(ctx, dir+"gone/x.txt", base); err != nil || *f.Content != "old\n" {
		t.Fatalf("file of a deleted directory at base = %+v, %v", f, err)
	}
	if f, err := c.Read(ctx, dir+"run.sh", base); err != nil || f.Mode != "100755" {
		t.Fatalf("executable at base = %+v, %v", f, err)
	}
	if f, err := c.Read(ctx, dir+"star*[x].txt", base); err != nil || *f.Content != "s" || f.Mode != "100644" {
		t.Fatalf("glob characters at base = %+v, %v", f, err)
	}
	_, err := c.Read(ctx, dir+"new.txt", base)
	wantCode(t, err, CodeNotInRevision)
	_, err = c.Read(ctx, dir+"gone", base)
	wantCode(t, err, CodeNotAFile)
	_, err = c.Read(ctx, dir+"f.txt", strings.Repeat("0", 40))
	wantCode(t, err, CodeRevNotFound)
	_, err = c.Read(ctx, h.ws+"/read/text.txt", base)
	wantCode(t, err, CodeNotAGitRepo)
}

func writeScenario(t *testing.T, h *harness) {
	h.mustFixture(t, `d=$WS/write
mkdir -p "$d/sub" "$d/repo"
cd "$d"
printf 'old\n' > existing.txt
chmod 640 existing.txt
printf '#!/bin/sh\n' > run.sh
chmod 755 run.sh
g -C repo init -q
ln -s repo/.git gitlink
ln -s "$(dirname "$WS")/outside" outlink`)
	c, rec := h.client(t)
	ctx := context.Background()
	dir := h.ws + "/write/"
	perm := func(name string) string {
		return strings.TrimSpace(h.mustFixture(t, `f="$WS/write/`+name+`"; stat -c %a "$f" 2>/dev/null || stat -f %Lp "$f"`))
	}
	read := func(name string) string {
		f, err := c.Read(ctx, dir+name, "")
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return *f.Content
	}

	w, err := c.Write(ctx, dir+"new.txt", []byte("fresh\n"), nil)
	if err != nil || w.SHA256 != sum([]byte("fresh\n")) || w.Size != 6 || w.Path != dir+"new.txt" {
		t.Fatalf("create = %+v, %v", w, err)
	}
	for _, call := range rec.reset() {
		if call.passive {
			t.Fatal("writes count as sandbox activity")
		}
	}
	if read("new.txt") != "fresh\n" || perm("new.txt") != "644" {
		t.Fatalf("created file = %q mode %s", read("new.txt"), perm("new.txt"))
	}
	_, err = c.Write(ctx, dir+"new.txt", []byte("again"), nil)
	wantCode(t, err, CodeExists)

	old := sum([]byte("old\n"))
	if _, err := c.Write(ctx, dir+"existing.txt", []byte("new\n"), &old); err != nil {
		t.Fatalf("update: %v", err)
	}
	if read("existing.txt") != "new\n" || perm("existing.txt") != "640" {
		t.Fatalf("updated file = %q mode %s", read("existing.txt"), perm("existing.txt"))
	}
	_, err = c.Write(ctx, dir+"existing.txt", []byte("lost update\n"), &old)
	if fe := wantCode(t, err, CodeConflict); fe.SHA256 == nil || *fe.SHA256 != sum([]byte("new\n")) {
		t.Fatalf("conflict sha = %v", fe.SHA256)
	}
	if read("existing.txt") != "new\n" {
		t.Fatal("a conflicting write changed the file")
	}
	_, err = c.Write(ctx, dir+"missing.txt", []byte("x"), &old)
	if fe := wantCode(t, err, CodeConflict); fe.SHA256 != nil {
		t.Fatalf("conflict on a deleted file has sha %q", *fe.SHA256)
	}
	runSum := sum([]byte("#!/bin/sh\n"))
	if _, err := c.Write(ctx, dir+"run.sh", []byte("#!/bin/sh\necho hi\n"), &runSum); err != nil || perm("run.sh") != "755" {
		t.Fatalf("executable keeps its mode: %s, %v", perm("run.sh"), err)
	}
	upper := strings.ToUpper(sum([]byte("#!/bin/sh\necho hi\n")))
	if _, err := c.Write(ctx, dir+"run.sh", []byte("#!/bin/sh\n"), &upper); err != nil {
		t.Fatalf("upper-case expected sha: %v", err)
	}

	_, err = c.Write(ctx, dir+"sub", []byte("x"), nil)
	wantCode(t, err, CodeNotAFile)
	_, err = c.Write(ctx, dir+"nodir/x.txt", []byte("x"), nil)
	wantCode(t, err, CodeParentNotFound)
	bad := "zz"
	_, err = c.Write(ctx, dir+"new.txt", []byte("x"), &bad)
	wantCode(t, err, CodeInvalidSHA256)
	rec.reset()
	for _, p := range []string{dir + "repo/.git/config", dir + "repo/.git", dir + "gitlink/config", dir + "gitlink/hooks/pre-commit", dir + "outlink/evil.txt", dir + "outlink/dir/secret.txt"} {
		_, err = c.Write(ctx, p, []byte("evil"), nil)
		wantCode(t, err, CodeInvalidPath)
	}
	if calls := rec.reset(); len(calls) != 4 {
		t.Fatalf("lexical .git paths must be refused before any exec; %d execs", len(calls))
	}
	if out := h.mustFixture(t, `o=$(dirname "$WS")/outside; ls "$o"; cat "$o/dir/secret.txt"; ls "$WS/write/repo/.git/hooks" | grep -c pre-commit$ || true`); out != "dir\nsecret.txt\nsecret\n0\n" {
		t.Fatalf("outside the workspace or .git changed: %q", out)
	}
	if out := h.mustFixture(t, `ls -A "$WS/write" | grep spawn-write || true`); out != "" {
		t.Fatalf("temporary files left behind: %q", out)
	}
}

func largeWriteScenario(t *testing.T, h *harness) {
	h.mustFixture(t, `mkdir -p "$WS/large"`)
	c, rec := h.client(t)
	ctx := context.Background()
	target := h.ws + "/large/big.txt"
	content := func(seed string) []byte {
		var b bytes.Buffer
		for i := 0; b.Len() < MaxFileBytes; i++ {
			fmt.Fprintf(&b, "%s line %d: the quick brown fox ' \" $(x) `y` \\ jumps\n", seed, i)
		}
		return b.Bytes()[:MaxFileBytes]
	}
	stagingEmpty := func() {
		t.Helper()
		if out := h.mustFixture(t, `ls -A "`+h.staging+`" 2>/dev/null || true`); out != "" {
			t.Fatalf("staging files left behind: %q", out)
		}
	}
	first := content("first")
	w, err := c.Write(ctx, target, first, nil)
	if err != nil || w.SHA256 != sum(first) || w.Size != MaxFileBytes {
		t.Fatalf("2 MiB create = %+v, %v", w, err)
	}
	calls := rec.reset()
	encoded := (len(first) + 2) / 3 * 4
	chunkCount := (encoded + WriteChunkSize - 1) / WriteChunkSize
	if len(calls) != chunkCount+2 {
		t.Fatalf("%d execs, want check + %d chunks + write", len(calls), chunkCount)
	}
	for i, call := range calls {
		// A chunk is WriteChunkSize of base64 in a base64-wrapped script.
		if call.size > WriteChunkSize*4/3+8<<10 {
			t.Fatalf("exec %d carries %d bytes", i, call.size)
		}
	}
	if f, err := c.Read(ctx, target, ""); err != nil || f.SHA256 != sum(first) {
		t.Fatalf("read back = %v, %v", f.SHA256, err)
	}
	stagingEmpty()

	second := content("second")
	firstSum := sum(first)
	if _, err := c.Write(ctx, target, second, &firstSum); err != nil {
		t.Fatalf("2 MiB update: %v", err)
	}
	stagingEmpty()

	// The agent rewrites the file while the chunks upload: the final check
	// catches it and the agent's version stays.
	secondSum := sum(second)
	rec.arm(func(n int) error {
		if n == 1 {
			_, err := h.fixture(`printf 'agent\n' > "$WS/large/big.txt"`)
			return err
		}
		return nil
	})
	_, err = c.Write(ctx, target, first, &secondSum)
	if fe := wantCode(t, err, CodeConflict); fe.SHA256 == nil || *fe.SHA256 != sum([]byte("agent\n")) {
		t.Fatalf("late conflict sha = %v", fe.SHA256)
	}
	rec.reset()
	if f, err := c.Read(ctx, target, ""); err != nil || *f.Content != "agent\n" {
		t.Fatalf("after a late conflict = %+v, %v", f, err)
	}
	stagingEmpty()
	if out := h.mustFixture(t, `ls -A "$WS/large"`); out != "big.txt\n" {
		t.Fatalf("temporary files left behind: %q", out)
	}

	// A chunk that fails: the staging file is removed.
	agentSum := sum([]byte("agent\n"))
	rec.arm(func(n int) error {
		if n == 3 {
			return errors.New("worker went away")
		}
		return nil
	})
	if _, err := c.Write(ctx, target, first, &agentSum); err == nil {
		t.Fatal("a failed chunk must fail the write")
	}
	// check, two chunks, the failed one, then the cleanup.
	if calls := rec.reset(); len(calls) != 5 {
		t.Fatalf("%d execs for a failed upload", len(calls))
	}
	stagingEmpty()
	if f, err := c.Read(ctx, target, ""); err != nil || *f.Content != "agent\n" {
		t.Fatalf("after a failed upload = %+v, %v", f, err)
	}
	rec.reset()

	// Just one chunk of base64 goes in one exec; one byte more is staged.
	oneChunk := bytes.Repeat([]byte("x"), WriteChunkSize/4*3)
	if _, err := c.Write(ctx, h.ws+"/large/one.txt", oneChunk, nil); err != nil || len(rec.reset()) != 1 {
		t.Fatalf("single-chunk write: %v", err)
	}
	oneSum := sum(oneChunk)
	if _, err := c.Write(ctx, h.ws+"/large/one.txt", append(oneChunk, 'y'), &oneSum); err != nil || len(rec.reset()) != 4 {
		t.Fatalf("two-chunk write: %v", err)
	}
	if f, err := c.Read(ctx, h.ws+"/large/one.txt", ""); err != nil || f.SHA256 != sum(append(oneChunk, 'y')) {
		t.Fatalf("two-chunk read back: %v", err)
	}

	rec.reset()
	_, err = c.Write(ctx, target, append(first, 'z'), &agentSum)
	if fe := wantCode(t, err, CodeFileTooLarge); fe.Size != MaxFileBytes+1 || len(rec.reset()) != 0 {
		t.Fatalf("oversize write = %+v", fe)
	}
}

func changesScenario(t *testing.T, h *harness) {
	out := h.mustFixture(t, `d=$WS/changes
mkdir -p "$d/lib"
cd "$d"
g init -q
printf 'keep\n' > keep.txt
printf 'one\ntwo\nthree\n' > mod.txt
printf 'gone\n' > del.txt
printf 'a\nb\nc\nd\ne\nf\n' > old-name.txt
printf '\000\001' > img.bin
printf 'l\n' > lib/l.txt
printf 'ignored.txt\n' > .gitignore
g add .
g commit -qm base
git rev-parse HEAD
printf 'one\nTWO\nthree\nfour\n' > mod.txt
rm del.txt
g mv old-name.txt new-name.txt
printf '\000\002' > img.bin
printf 'added\n' > added.txt
g add added.txt
g commit -qm work
printf 'L\n' > lib/l.txt
printf u > u.txt
printf i > ignored.txt
mkdir sub
printf s > sub/s.txt
git rev-parse HEAD`)
	revs := strings.Fields(out)
	if len(revs) != 2 {
		t.Fatalf("fixture = %q", out)
	}
	base, head := revs[0], revs[1]
	c, rec := h.client(t)
	ctx := context.Background()

	changes, err := c.Changes(ctx, h.ws+"/changes", base)
	if err != nil {
		t.Fatal(err)
	}
	if changes.Base != base || changes.Head != head || changes.Truncated {
		t.Fatalf("changes = %+v", changes)
	}
	got := map[string]Change{}
	for _, change := range changes.Files {
		got[change.Path] = change
	}
	want := map[string]Change{
		"mod.txt":      {Path: "mod.txt", Status: "M", Additions: 2, Deletions: 1},
		"del.txt":      {Path: "del.txt", Status: "D", Deletions: 1},
		"new-name.txt": {Path: "new-name.txt", From: "old-name.txt", Status: "R"},
		"img.bin":      {Path: "img.bin", Status: "M"},
		"added.txt":    {Path: "added.txt", Status: "A", Additions: 1},
		"lib/l.txt":    {Path: "lib/l.txt", Status: "M", Additions: 1, Deletions: 1},
		"u.txt":        {Path: "u.txt", Status: "?"},
		"sub/s.txt":    {Path: "sub/s.txt", Status: "?"},
	}
	if len(got) != len(want) || len(changes.Files) != len(want) {
		t.Fatalf("changes = %+v", changes.Files)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %+v, want %+v", name, got[name], w)
		}
	}
	for _, call := range rec.reset() {
		if !call.passive {
			t.Fatal("changes must not count as sandbox activity")
		}
	}

	lib, err := c.Changes(ctx, h.ws+"/changes/lib", base)
	if err != nil || len(lib.Files) != 1 || lib.Files[0].Path != "l.txt" {
		t.Fatalf("changes under lib = %+v, %v", lib, err)
	}
	c.cfg.ChangesMax = 3
	if capped, err := c.Changes(ctx, h.ws+"/changes", base); err != nil || !capped.Truncated || len(capped.Files) != 3 {
		t.Fatalf("capped changes = %+v, %v", capped, err)
	}
	c.cfg.ChangesMax = MaxChanges

	_, err = c.Changes(ctx, h.ws+"/changes", strings.Repeat("0", 40))
	wantCode(t, err, CodeRevNotFound)
	_, err = c.Changes(ctx, h.ws+"/read", base)
	wantCode(t, err, CodeNotAGitRepo)
	_, err = c.Changes(ctx, h.ws+"/changes", "HEAD")
	wantCode(t, err, CodeInvalidRev)
}
