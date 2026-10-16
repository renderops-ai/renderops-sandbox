package gitworkspace

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"backend/internal/plane"
)

const (
	headSHA = "1111111111111111111111111111111111111111"
	baseSHA = "2222222222222222222222222222222222222222"
	remote  = "http://172.16.0.1:7080/acme/widgets.git"
)

func validSpec() Spec {
	return Spec{RemoteURL: remote, HeadSHA: headSHA, BaseSHA: baseSHA, Dir: DefaultDir, ReviewDir: DefaultReviewDir}
}

func TestSpecValidation(t *testing.T) {
	if err := validSpec().Validate(); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Spec){
		"github remote":    func(s *Spec) { s.RemoteURL = "https://github.com/acme/widgets.git" },
		"remote with cred": func(s *Spec) { s.RemoteURL = "http://x:y@172.16.0.1:7080/acme/widgets.git" },
		"short sha":        func(s *Spec) { s.HeadSHA = "abc123" },
		"branch name":      func(s *Spec) { s.BaseSHA = "main" },
		"root dir":         func(s *Spec) { s.Dir = "/" },
		"system dir":       func(s *Spec) { s.ReviewDir = "/etc" },
		"relative dir":     func(s *Spec) { s.Dir = "workspace/repo" },
		"traversal":        func(s *Spec) { s.Dir = "/workspace/../etc" },
		"shell chars":      func(s *Spec) { s.Dir = "/workspace/$(reboot)" },
		"nested review":    func(s *Spec) { s.ReviewDir = "/workspace/repo/review" },
		"same dirs":        func(s *Spec) { s.ReviewDir = s.Dir },
	} {
		spec := validSpec()
		mutate(&spec)
		if err := spec.Validate(); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("%s: err = %v, want ErrInvalidSpec", name, err)
		}
	}
}

func TestCommandRunsScriptInChildShell(t *testing.T) {
	command, err := Command(validSpec())
	if err != nil {
		t.Fatal(err)
	}
	encoded, ok := strings.CutPrefix(command, "printf '%s' '")
	encoded, ok2 := strings.CutSuffix(encoded, "' | base64 -d | bash")
	if !ok || !ok2 {
		t.Fatalf("command not wrapped for a child shell: %s", command)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	script, _ := Script(validSpec())
	if string(decoded) != script {
		t.Fatal("decoded command differs from the script")
	}
	for _, want := range []string{"GIT_CONFIG_GLOBAL=/dev/null", "credential.helper=", "--filter=blob:none", "--detach", remote} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q", want)
		}
	}
}

// TestScriptChecksOutPinnedCommitAndWritesDiff runs the real script with git
// against a local repository standing in for the proxy remote.
func TestScriptChecksOutPinnedCommitAndWritesDiff(t *testing.T) {
	for _, tool := range []string{"git", "bash"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	root := t.TempDir()
	src := filepath.Join(root, "src")
	gitRun(t, "", "init", "-q", "-b", "main", src)
	writeFile(t, filepath.Join(src, "a.txt"), "line one\n")
	gitRun(t, src, "add", ".")
	gitRun(t, src, "commit", "-q", "-m", "base")
	base := gitRun(t, src, "rev-parse", "HEAD")
	gitRun(t, src, "checkout", "-q", "-b", "feature")
	writeFile(t, filepath.Join(src, "a.txt"), "line one\nline two\n")
	writeFile(t, filepath.Join(src, "b.txt"), "new file\n")
	gitRun(t, src, "add", ".")
	gitRun(t, src, "commit", "-q", "-m", "head")
	head := gitRun(t, src, "rev-parse", "HEAD")
	gitRun(t, src, "checkout", "-q", "main") // head is only reachable via the feature ref

	bare := filepath.Join(root, "remote.git")
	gitRun(t, "", "clone", "-q", "--bare", src, bare)
	gitRun(t, bare, "config", "uploadpack.allowFilter", "true")
	gitRun(t, bare, "config", "uploadpack.allowAnySHA1InWant", "true")

	spec := Spec{RemoteURL: "file://" + bare, HeadSHA: head, BaseSHA: base,
		Dir: filepath.Join(root, "workspace", "repo"), ReviewDir: filepath.Join(root, "review")}
	if err := spec.validateLocal(); err != nil {
		t.Fatal(err)
	}
	out, code := runScript(t, buildScript(spec))
	if code != 0 {
		t.Fatalf("script exit %d: %s", code, out)
	}
	if got := gitRun(t, spec.Dir, "rev-parse", "HEAD"); got != head {
		t.Fatalf("HEAD = %s, want pinned %s", got, head)
	}
	diff := readFile(t, filepath.Join(spec.ReviewDir, "pr.diff"))
	if !strings.Contains(diff, "+line two") || !strings.Contains(diff, "+new file") {
		t.Fatalf("pr.diff missing changes:\n%s", diff)
	}
	changed := readFile(t, filepath.Join(spec.ReviewDir, "changed-files.txt"))
	if !strings.Contains(changed, "M\ta.txt") || !strings.Contains(changed, "A\tb.txt") {
		t.Fatalf("changed-files.txt = %q", changed)
	}
	if commits := readFile(t, filepath.Join(spec.ReviewDir, "commits.txt")); !strings.Contains(commits, "merge-base "+base) {
		t.Fatalf("commits.txt = %q", commits)
	}

	// Idempotent re-run.
	if out, code := runScript(t, buildScript(spec)); code != 0 || !strings.Contains(out, "already prepared") {
		t.Fatalf("re-run exit %d: %s", code, out)
	}

	// A commit the repository cannot supply fails closed with exit 3.
	missing := spec
	missing.HeadSHA = strings.Repeat("a", 40)
	missing.ReviewDir = filepath.Join(root, "review-missing")
	missing.Dir = filepath.Join(root, "workspace", "missing")
	if out, code := runScript(t, buildScript(missing)); code != exitCommitUnavailable {
		t.Fatalf("missing commit exit %d, want %d: %s", code, exitCommitUnavailable, out)
	}
}

type fakeWorker struct {
	grantReq plane.GitAccessRequest
	grantErr error
	exec     plane.ExecResult
	execErr  error
	command  string
	quiet    bool
	revokes  int
}

func (f *fakeWorker) GrantGitAccess(_ context.Context, _ string, req plane.GitAccessRequest) (plane.GitAccessResponse, error) {
	f.grantReq = req
	if f.grantErr != nil {
		return plane.GitAccessResponse{}, f.grantErr
	}
	return plane.GitAccessResponse{RemoteURL: "http://172.16.0.1:7080/" + req.Repository + ".git", ExpiresAt: req.ExpiresAt}, nil
}

func (f *fakeWorker) RevokeGitAccess(context.Context, string) error {
	f.revokes++
	return nil
}

func (f *fakeWorker) Exec(_ context.Context, _ string, req plane.ExecRequest) (plane.ExecResult, error) {
	f.command = req.Command
	f.quiet = req.Quiet
	return f.exec, f.execErr
}

func TestPrepareAlwaysRevokesAndMapsFailures(t *testing.T) {
	pin := Pin{Repository: "acme/widgets", HeadSHA: headSHA, BaseSHA: baseSHA, Token: "ghs_x", TokenExpiresAt: time.Now().Add(time.Hour)}

	ok := &fakeWorker{}
	result, err := Prepare(context.Background(), ok, "sbx-1", pin)
	if err != nil || result.HeadSHA != headSHA || result.Dir != DefaultDir {
		t.Fatalf("Prepare = %+v, %v", result, err)
	}
	if ok.grantReq.Repository != "acme/widgets" || ok.grantReq.Token != "ghs_x" || ok.revokes != 1 {
		t.Fatalf("grant=%+v revokes=%d", ok.grantReq, ok.revokes)
	}
	if strings.Contains(ok.command, "ghs_x") {
		t.Fatal("token leaked into the guest command")
	}
	if !ok.quiet {
		t.Fatal("the checkout must run quietly, out of the sandbox's log stream")
	}

	unavailable := &fakeWorker{exec: plane.ExecResult{ExitCode: exitCommitUnavailable, Stderr: "not available"}}
	if _, err := Prepare(context.Background(), unavailable, "sbx-1", pin); !errors.Is(err, ErrCommitUnavailable) || unavailable.revokes != 1 {
		t.Fatalf("unavailable: err=%v revokes=%d", err, unavailable.revokes)
	}

	execFailed := &fakeWorker{execErr: errors.New("vsock closed")}
	if _, err := Prepare(context.Background(), execFailed, "sbx-1", pin); err == nil || execFailed.revokes != 1 {
		t.Fatalf("exec error: err=%v revokes=%d", err, execFailed.revokes)
	}

	bad := pin
	bad.HeadSHA = "main"
	noCall := &fakeWorker{}
	if _, err := Prepare(context.Background(), noCall, "sbx-1", bad); !errors.Is(err, ErrInvalidSpec) || noCall.grantReq.Repository != "" {
		t.Fatalf("invalid pin reached the worker: err=%v", err)
	}
}

func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func runScript(t *testing.T, script string) (string, int) {
	t.Helper()
	cmd := exec.Command("bash", "-c", script)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out), exitErr.ExitCode()
	}
	if err != nil {
		t.Fatalf("run script: %v", err)
	}
	return string(out), 0
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
