package modelaccess

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"backend/internal/plane"
)

// envFileToken stands in for a proxy token; every test checks it never shows
// in plain text outside the file.
const envFileToken = "proxy-token-0123456789abcdef0123456789abcdef"

// trickyEnv has values that would break naive quoting.
func trickyEnv() map[string]string {
	env := ClaudeCodeEnv("http://172.16.0.1:7081", envFileToken, "claude-opus-5-5")
	env["SPAWN_TRICKY"] = "it's $HOME `id` \"$(id)\" \\ ; done\nline two"
	return env
}

func TestValidateEnvFilePath(t *testing.T) {
	for _, ok := range []string{"/var/lib/spawn/cli/env", "/var/lib/spawn/env", "/var/lib/spawn/a..b/env"} {
		if err := ValidateEnvFilePath(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"", "env", "var/lib/spawn/cli/env", "/var/lib/spawn", "/var/lib/spawn/", "/var/lib/spawn/cli/",
		"/var/lib/spawn/../spawn/env", "/var/lib/spawn/cli/../env", "/var/lib/spawn/./env", "/var/lib/spawn//env",
		"/var/lib/spawnx/env", "/etc/profile", "/workspace/.env", "/var/lib/spawn/cli/env\x00",
		"/var/lib/spawn/" + strings.Repeat("a", 4096),
	} {
		if err := ValidateEnvFilePath(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestEnvFileContentQuotesEveryValueAndAddsHome(t *testing.T) {
	got, err := EnvFileContent(map[string]string{"B_KEY": "it's", "A_KEY": "plain", "_X1": ""})
	if err != nil {
		t.Fatal(err)
	}
	want := "export A_KEY='plain'\nexport B_KEY='it'\\''s'\nexport HOME='/root'\nexport _X1=''\n"
	if got != want {
		t.Fatalf("content = %q, want %q", got, want)
	}
	// HOME is always /root.
	if got, err := EnvFileContent(map[string]string{"HOME": "/home/x"}); err != nil || got != "export HOME='/root'\n" {
		t.Fatalf("HOME override: %q, %v", got, err)
	}
	for _, key := range []string{"", "lower", "1ABC", "A-B", "A B", "A=B", "Ä", "A\n"} {
		if _, err := EnvFileContent(map[string]string{key: "v"}); !errors.Is(err, ErrInvalidEnvKey) {
			t.Errorf("key %q: err = %v, want ErrInvalidEnvKey", key, err)
		}
	}
	if _, err := EnvFileContent(map[string]string{"A": "x\x00y"}); err == nil {
		t.Error("NUL value accepted")
	}
}

func TestWriteEnvFileChecksBeforeAndAfterTheExec(t *testing.T) {
	var commands []string
	answer := plane.ExecResult{Stdout: envFileOK + "\n"}
	var answerErr error
	run := func(_ context.Context, command string, timeoutS int) (plane.ExecResult, error) {
		if timeoutS <= 0 {
			t.Fatalf("timeout = %d", timeoutS)
		}
		commands = append(commands, command)
		return answer, answerErr
	}
	ctx := context.Background()
	if err := WriteEnvFile(ctx, run, "/etc/profile", trickyEnv()); err == nil || len(commands) != 0 {
		t.Fatalf("bad path: err %v after %d execs", err, len(commands))
	}
	if err := WriteEnvFile(ctx, run, "/var/lib/spawn/cli/env", map[string]string{"bad-key": "v"}); err == nil || len(commands) != 0 {
		t.Fatalf("bad key: err %v after %d execs", err, len(commands))
	}
	if err := WriteEnvFile(ctx, run, "/var/lib/spawn/cli/env", trickyEnv()); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || strings.Contains(commands[0], envFileToken) {
		t.Fatalf("the token is plain text in the guest command: %q", commands)
	}

	for name, c := range map[string]struct {
		res plane.ExecResult
		err error
	}{
		"exec error":   {err: plane.ErrSandboxNotActive},
		"exit status":  {res: plane.ExecResult{ExitCode: 1, Stdout: envFileOK, Stderr: "mkdir: Permission denied"}},
		"no ok marker": {res: plane.ExecResult{}},
	} {
		answer, answerErr = c.res, c.err
		err := WriteEnvFile(ctx, run, "/var/lib/spawn/cli/env", trickyEnv())
		if !errors.Is(err, ErrEnvFile) || strings.Contains(err.Error(), envFileToken) {
			t.Errorf("%s: err = %v", name, err)
		}
		if c.err != nil && !errors.Is(err, c.err) {
			t.Errorf("%s: err %v does not wrap %v", name, err, c.err)
		}
	}
}

// runGuest runs a guest command as the guest agent does: written to a
// shell's stdin.
type runGuest func(ctx context.Context, command string) (plane.ExecResult, error)

func shellRunner(name string, args ...string) runGuest {
	return func(ctx context.Context, command string) (plane.ExecResult, error) {
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Stdin = strings.NewReader(command + "\n")
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
}

// The write script, in the host's sh and bash, against a temporary
// directory: modes, content, sourcing, replacement and failure cleanup.
func TestEnvFileScriptLocal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the guest script needs a POSIX shell")
	}
	for _, shell := range []string{"sh", "bash"} {
		if _, err := exec.LookPath(shell); err != nil {
			t.Logf("%s not found: %v", shell, err)
			continue
		}
		t.Run(shell, func(t *testing.T) {
			run := shellRunner(shell, "-s")
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			base := t.TempDir()
			dir := filepath.Join(base, "spawn", "cli")
			file := filepath.Join(dir, "env")
			content, err := EnvFileContent(trickyEnv())
			if err != nil {
				t.Fatal(err)
			}

			res, err := run(ctx, envFileCommand(file, content))
			if err != nil || res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != envFileOK {
				t.Fatalf("write: %+v, %v", res, err)
			}
			if strings.Contains(res.Stdout+res.Stderr, envFileToken) {
				t.Fatal("the script printed the token")
			}
			checkEnvFile(t, file, content)
			sourced := sourceEnvFile(t, ctx, run, file)
			for key, want := range map[string]string{
				"ANTHROPIC_AUTH_TOKEN": envFileToken, "HOME": "/root", "SPAWN_TRICKY": trickyEnv()["SPAWN_TRICKY"],
			} {
				if sourced[key] != want {
					t.Errorf("sourced %s = %q, want %q", key, sourced[key], want)
				}
			}

			// A second write replaces the file and tightens an existing
			// directory.
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(file, 0o644); err != nil {
				t.Fatal(err)
			}
			if res, err := run(ctx, envFileCommand(file, "export A='2'\n")); err != nil || res.ExitCode != 0 {
				t.Fatalf("rewrite: %+v, %v", res, err)
			}
			checkEnvFile(t, file, "export A='2'\n")

			// A directory in the file's place is refused; nothing is left
			// behind.
			taken := filepath.Join(dir, "taken")
			if err := os.Mkdir(taken, 0o700); err != nil {
				t.Fatal(err)
			}
			if res, err := run(ctx, envFileCommand(taken, content)); err != nil || res.ExitCode == 0 || strings.Contains(res.Stdout, envFileOK) {
				t.Fatalf("directory target: %+v, %v", res, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			if strings.Join(names, ",") != "env,taken" {
				t.Fatalf("directory holds %v, want env and taken only", names)
			}
		})
	}
}

func checkEnvFile(t *testing.T, file, content string) {
	t.Helper()
	info, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Errorf("file mode = %v, want 0600", info.Mode())
	}
	dirInfo, err := os.Stat(filepath.Dir(file))
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Errorf("directory mode = %v, want 0700", dirInfo.Mode())
	}
	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("file = %q, want %q", got, content)
	}
	if _, err := os.Stat(file + ".spawn-tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temporary file left behind: %v", err)
	}
}

// sourceEnvFile sources file in a clean shell, as the terminal does, and
// returns the variables it sets (NUL-separated, so newlines survive).
func sourceEnvFile(t *testing.T, ctx context.Context, run runGuest, file string) map[string]string {
	t.Helper()
	keys := []string{"ANTHROPIC_AUTH_TOKEN", "HOME", "SPAWN_TRICKY"}
	script := "set -a; . " + shellQuote(file) + "; set +a; for k in " + strings.Join(keys, " ") +
		`; do eval "v=\${$k}"; printf '%s=%s\036' "$k" "$v"; done`
	res, err := run(ctx, script)
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("source: %+v, %v", res, err)
	}
	vars := map[string]string{}
	for _, kv := range strings.Split(strings.TrimSuffix(res.Stdout, "\036"), "\036") {
		k, v, _ := strings.Cut(kv, "=")
		vars[k] = v
	}
	return vars
}

// TestWriteEnvFileAlpine writes the real /var/lib/spawn/cli/env through
// WriteEnvFile in an Alpine container (busybox, bash as the guest's
// persistent shell). It needs Docker and is opt-in, like the files API's
// guest tests:
//
//	SANDBOXFILES_DOCKER_IMAGE=alpine:3.20 go test ./internal/modelaccess -run Alpine
func TestWriteEnvFileAlpine(t *testing.T) {
	image := os.Getenv("SANDBOXFILES_DOCKER_IMAGE")
	if image == "" {
		image = os.Getenv("AGENTSESSION_DOCKER_IMAGE")
	}
	if image == "" {
		t.Skip("set SANDBOXFILES_DOCKER_IMAGE (for example alpine:3.20) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", image, "sleep", "600").CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v\n%s", err, raw)
	}
	container := strings.TrimSpace(string(raw))
	t.Cleanup(func() {
		if out, err := exec.Command("docker", "rm", "-f", container).CombinedOutput(); err != nil {
			t.Logf("docker rm: %v\n%s", err, out)
		}
	})
	if out, err := exec.CommandContext(ctx, "docker", "exec", container, "sh", "-c", "apk add --no-cache bash >/dev/null").CombinedOutput(); err != nil {
		t.Fatalf("install bash: %v\n%s", err, out)
	}
	guest := shellRunner("docker", "exec", "-i", container, "bash", "-s")
	var outputs []string
	run := func(ctx context.Context, command string, _ int) (plane.ExecResult, error) {
		res, err := guest(ctx, command)
		outputs = append(outputs, res.Stdout, res.Stderr)
		return res, err
	}

	const file = "/var/lib/spawn/cli/env"
	if err := WriteEnvFile(ctx, run, file, trickyEnv()); err != nil {
		t.Fatalf("WriteEnvFile: %v (output %q)", err, outputs)
	}
	if strings.Contains(strings.Join(outputs, ""), envFileToken) {
		t.Fatal("the guest printed the token")
	}
	busybox := shellRunner("docker", "exec", "-i", container, "sh", "-s")
	res, err := busybox(ctx, "stat -c '%a %U' "+file+" /var/lib/spawn/cli; ls -a /var/lib/spawn/cli | tr '\\n' ' '")
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("stat: %+v, %v", res, err)
	}
	if want := "600 root\n700 root\n. .. env "; res.Stdout != want {
		t.Fatalf("stat = %q, want %q", res.Stdout, want)
	}
	sourced := sourceEnvFile(t, ctx, busybox, file)
	if sourced["ANTHROPIC_AUTH_TOKEN"] != envFileToken || sourced["HOME"] != "/root" || sourced["SPAWN_TRICKY"] != trickyEnv()["SPAWN_TRICKY"] {
		t.Fatalf("sourced = %q", sourced)
	}
	// Rewriting (the next grant) replaces the token.
	env := trickyEnv()
	env["ANTHROPIC_AUTH_TOKEN"] = "second-token"
	if err := WriteEnvFile(ctx, run, file, env); err != nil {
		t.Fatal(err)
	}
	if got := sourceEnvFile(t, ctx, busybox, file)["ANTHROPIC_AUTH_TOKEN"]; got != "second-token" {
		t.Fatalf("after rewrite token = %q", got)
	}
}
