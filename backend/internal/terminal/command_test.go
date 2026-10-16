package terminal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommandValidate(t *testing.T) {
	valid := []Command{
		{},
		{Argv: []string{"claude"}},
		{Argv: []string{"claude", "--continue"}, Cwd: "/workspace/repo", EnvFile: "/var/lib/spawn/cli/env"},
		{Cwd: "/workspace"},
		{Argv: []string{"/usr/local/bin/claude", "it's \"$(fine)\"\n", ""}},
		{Argv: make([]string, 0), EnvFile: "/var/lib/spawn/x"},
		{Argv: append([]string{"a"}, make([]string, MaxCommandArgs-1)...)},
		{Argv: []string{"a", strings.Repeat("x", MaxCommandBytes-1)}},
	}
	for _, c := range valid {
		if err := c.Validate(); err != nil {
			t.Errorf("%+v: %v", c, err)
		}
	}
	invalid := map[string]Command{
		"empty program":       {Argv: []string{""}},
		"option as program":   {Argv: []string{"-c", "x"}},
		"NUL in argument":     {Argv: []string{"a", "b\x00c"}},
		"invalid UTF-8":       {Argv: []string{"a", "\xff"}},
		"too many arguments":  {Argv: append([]string{"a"}, make([]string, MaxCommandArgs)...)},
		"too many bytes":      {Argv: []string{"a", strings.Repeat("x", MaxCommandBytes)}},
		"relative cwd":        {Cwd: "workspace/repo"},
		"cwd outside":         {Cwd: "/etc"},
		"cwd prefix trick":    {Cwd: "/workspaces/x"},
		"cwd not clean":       {Cwd: "/workspace/repo/../../etc"},
		"cwd trailing slash":  {Cwd: "/workspace/repo/"},
		"cwd NUL":             {Cwd: "/workspace/a\x00b"},
		"cwd too long":        {Cwd: "/workspace/" + strings.Repeat("a", MaxCommandPathBytes)},
		"env file outside":    {EnvFile: "/etc/profile"},
		"env file dir itself": {EnvFile: "/var/lib/spawn"},
		"env file not clean":  {EnvFile: "/var/lib/spawn/../../etc/passwd"},
		"env file relative":   {EnvFile: "var/lib/spawn/env"},
		"env file workspace":  {EnvFile: "/workspace/.env"},
	}
	for name, c := range invalid {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: %+v was accepted", name, c)
		}
	}
}

func TestCommandStartupEnv(t *testing.T) {
	if env := (Command{}).StartupEnv(); env != nil {
		t.Fatalf("a plain shell gets startup env %v", env)
	}
	c := Command{Argv: []string{"sh", "-c", "echo '$(id)'; `id`"}, Cwd: "/workspace/my repo", EnvFile: "/var/lib/spawn/env"}
	env := c.StartupEnv()
	if len(env) != 1 || env["PROMPT_COMMAND"] == "" {
		t.Fatalf("startup env = %v", env)
	}
	// User values never appear as shell text, not even in the decoded script.
	script := c.startupScript()
	for _, value := range []string{"$(id)", "`id`", "my repo", "/var/lib/spawn/env"} {
		if strings.Contains(env["PROMPT_COMMAND"], value) || strings.Contains(script, value) {
			t.Errorf("%q appears as shell text", value)
		}
	}
}

// shellResult is a terminal run: what it printed and how it exited.
type shellResult struct {
	output string
	code   int
}

// The startup scripts run end to end in an interactive bash, in two places:
//
//   - TestCommandInteractiveBashLocal: the host's bash, forced interactive
//     with -i and fed on stdin. Always on.
//
//   - TestCommandPTYAlpine: Alpine's bash as the guest starts it, a login
//     shell on a real PTY (util-linux script). Opt-in:
//
//     TERMINAL_DOCKER_IMAGE=alpine:3.22 go test ./internal/terminal -run Alpine
func TestCommandInteractiveBashLocal(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skipf("bash is required: %v", err)
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run := func(t *testing.T, env map[string]string, stdin string) shellResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, bash, "--noprofile", "--norc", "-i")
		cmd.Dir = base
		cmd.Env = append(os.Environ(), "PS1=$ ", "HISTFILE=/dev/null")
		for key, value := range env {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
		cmd.Stdin = strings.NewReader(stdin)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			t.Fatalf("run bash: %v", err)
		}
		return shellResult{output: out.String(), code: cmd.ProcessState.ExitCode()}
	}
	runCommandScenarios(t, base, run, func(t *testing.T, script string) {
		t.Helper()
		if out, err := exec.Command("sh", "-c", script).CombinedOutput(); err != nil {
			t.Fatalf("fixture: %v\n%s", err, out)
		}
	})
}

func TestCommandPTYAlpine(t *testing.T) {
	image := os.Getenv("TERMINAL_DOCKER_IMAGE")
	if image == "" {
		t.Skip("set TERMINAL_DOCKER_IMAGE (for example alpine:3.22) to run")
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
	if out, err := exec.CommandContext(ctx, "docker", "exec", container, "sh", "-c",
		"apk add --no-cache bash util-linux >/dev/null && mkdir -p /workspace").CombinedOutput(); err != nil {
		t.Fatalf("install bash and script: %v\n%s", err, out)
	}
	run := func(t *testing.T, env map[string]string, stdin string) shellResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// As the guest agent starts a terminal: `bash -l` on a PTY, in
		// /workspace, TERM set, plus the startup env.
		args := []string{"exec", "-i", "-w", "/workspace", "-e", "TERM=xterm-256color"}
		for key, value := range env {
			args = append(args, "-e", key+"="+value)
		}
		args = append(args, container, "script", "-qec", "bash -l", "/dev/null")
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Stdin = strings.NewReader(stdin)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			t.Fatalf("run bash on a PTY: %v", err)
		}
		// The PTY turns "\n" into "\r\n".
		return shellResult{output: strings.ReplaceAll(out.String(), "\r\n", "\n"), code: cmd.ProcessState.ExitCode()}
	}
	runCommandScenarios(t, "/workspace", run, func(t *testing.T, script string) {
		t.Helper()
		if out, err := exec.Command("docker", "exec", container, "sh", "-c", script).CombinedOutput(); err != nil {
			t.Fatalf("fixture: %v\n%s", err, out)
		}
	})
}

// runCommandScenarios runs startup scripts in a shell started by run. The
// paths validated for the guest (/workspace, /var/lib/spawn) are rewritten
// under root for the local run.
func runCommandScenarios(t *testing.T, root string, run func(*testing.T, map[string]string, string) shellResult, fixture func(*testing.T, string)) {
	dir := root + "/my 'repo' $(id)"
	envFile := root + "/spawn-env"
	fixture(t, "mkdir -p \""+root+"/my 'repo' \\$(id)\" && printf '%s\\n' \"export SECRET_TOKEN='s3cr3t'\" 'PLAIN=yes' > \""+envFile+"\"")
	// Validate checks the guest paths; the scripts take any path.
	start := func(c Command) map[string]string {
		guest := c
		if guest.Cwd != "" {
			guest.Cwd = CommandWorkspace + "/x"
		}
		if guest.EnvFile != "" {
			guest.EnvFile = EnvFileDir + "/x"
		}
		if err := guest.Validate(); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		return c.StartupEnv()
	}

	t.Run("command", func(t *testing.T) {
		tricky := "it's \"$(id)\" `id` $HOME *\n"
		res := run(t, start(Command{
			Argv: []string{"sh", "-c", `printf '[%s]' "$PWD" "$SECRET_TOKEN" "$PLAIN" "${PROMPT_COMMAND-unset}" "$1" "$2"; exit 7`, "sh", tricky, ""},
			Cwd:  dir, EnvFile: envFile,
		}), "echo typed-input-is-not-run\n")
		want := "[" + dir + "][s3cr3t][yes][unset][" + tricky + "][]"
		if res.code != 7 || !strings.Contains(res.output, want) {
			t.Fatalf("exit %d, output %q, want %q", res.code, res.output, want)
		}
		if hasLine(res.output, "typed-input-is-not-run") {
			t.Fatalf("the shell ran input after the command: %q", res.output)
		}
		for _, leak := range []string{"s3cr3t'", "SECRET_TOKEN=", "__spawn", "base64", "PROMPT_COMMAND"} {
			if strings.Contains(res.output, leak) {
				t.Fatalf("the terminal shows %q: %q", leak, res.output)
			}
		}
	})
	t.Run("shell in cwd", func(t *testing.T) {
		res := run(t, start(Command{Cwd: dir, EnvFile: envFile}), "printf '<%s|%s|%s>' \"$PWD\" \"$SECRET_TOKEN\" \"${PROMPT_COMMAND-unset}\"; exit 3\n")
		if res.code != 3 || !strings.Contains(res.output, "<"+dir+"|s3cr3t|unset>") {
			t.Fatalf("exit %d, output %q", res.code, res.output)
		}
	})
	t.Run("missing directory", func(t *testing.T) {
		res := run(t, start(Command{Argv: []string{"true"}, Cwd: root + "/missing"}), "")
		if res.code != ExitStartupFailed || !strings.Contains(res.output, "spawn: cannot enter the directory "+root+"/missing") {
			t.Fatalf("exit %d, output %q", res.code, res.output)
		}
	})
	t.Run("missing env file", func(t *testing.T) {
		res := run(t, start(Command{Argv: []string{"true"}, EnvFile: root + "/no-env"}), "")
		if res.code != ExitStartupFailed || !strings.Contains(res.output, "spawn: cannot read the env file") {
			t.Fatalf("exit %d, output %q", res.code, res.output)
		}
	})
	t.Run("command not found", func(t *testing.T) {
		res := run(t, start(Command{Argv: []string{"spawn-no-such-command"}}), "echo still-a-shell\n")
		if res.code != ExitCommandFailed || hasLine(res.output, "still-a-shell") {
			t.Fatalf("exit %d, output %q", res.code, res.output)
		}
	})
}

// hasLine reports whether output has a line that is exactly line (typed
// input a PTY echoes back is "echo <line>", not <line>).
func hasLine(output, line string) bool {
	for _, l := range strings.Split(output, "\n") {
		if strings.TrimSpace(l) == line {
			return true
		}
	}
	return false
}
