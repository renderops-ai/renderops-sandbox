package modelaccess

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"

	"backend/internal/plane"
	"backend/internal/terminal"
)

// The spawn CLI's terminal sources an env file (terminal.Command.EnvFile)
// that holds the proxy token. Writing that file through the public exec API
// would record the token in the run history (sandbox_runs.command), so the
// model-access endpoint writes it itself (WriteEnvFile): one quiet guest exec
// that the control plane never records, and the token never returns to the
// caller.

// envFileTimeoutS bounds the guest exec that writes an env file.
const envFileTimeoutS = 15

// envFileHome is the HOME every env file sets (tools keep state in ~; the
// terminal's login shell otherwise has the guest's HOME).
const envFileHome = "/root"

// envFileOK is the line the write script prints on success.
const envFileOK = "spawn-env-file-ok"

var (
	// ErrInvalidEnvKey means an env name is not [A-Z_][A-Z0-9_]*.
	ErrInvalidEnvKey = errors.New("env names must match [A-Z_][A-Z0-9_]*")
	// ErrEnvFile means the guest did not write the env file.
	ErrEnvFile = errors.New("failed to write the env file in the sandbox")
)

var envKeyPattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// ValidateEnvFilePath accepts the env file paths a terminal can load (the
// same rule as its env_file): an absolute, clean path (so no ".." component)
// below /var/lib/spawn/. Its errors are safe to show the caller.
func ValidateEnvFilePath(p string) error {
	if p == "" {
		return fmt.Errorf("env_file must be an absolute, clean path under %s/", terminal.EnvFileDir)
	}
	return terminal.Command{EnvFile: p}.Validate()
}

// EnvFileContent renders env plus HOME=/root as `export KEY='value'` lines,
// sorted by name. Every value is single-quoted, so the file is safe to source
// whatever the values hold; names must match [A-Z_][A-Z0-9_]* and values
// must not contain NUL bytes.
func EnvFileContent(env map[string]string) (string, error) {
	vars := make(map[string]string, len(env)+1)
	for k, v := range env {
		vars[k] = v
	}
	vars["HOME"] = envFileHome
	keys := make([]string, 0, len(vars))
	for k := range vars {
		if !envKeyPattern.MatchString(k) {
			return "", fmt.Errorf("%w: %q", ErrInvalidEnvKey, k)
		}
		if strings.IndexByte(vars[k], 0) >= 0 {
			return "", fmt.Errorf("env value of %s contains a NUL byte", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString("export " + k + "=" + shellQuote(vars[k]) + "\n")
	}
	return b.String(), nil
}

// GuestExec runs one command in the sandbox's guest shell. It must be quiet
// (the output stays out of the sandbox's log stream) and must not be recorded
// as a run.
type GuestExec func(ctx context.Context, command string, timeoutS int) (plane.ExecResult, error)

// WriteEnvFile writes env (plus HOME=/root, see EnvFileContent) to file in
// the guest, mode 0600 in a 0700 directory, replacing it atomically.
func WriteEnvFile(ctx context.Context, exec GuestExec, file string, env map[string]string) error {
	if err := ValidateEnvFilePath(file); err != nil {
		return fmt.Errorf("env file path: %w", err)
	}
	content, err := EnvFileContent(env)
	if err != nil {
		return fmt.Errorf("env file content: %w", err)
	}
	res, err := exec(ctx, envFileCommand(file, content), envFileTimeoutS)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrEnvFile, err)
	}
	if res.ExitCode != 0 || !strings.Contains(res.Stdout, envFileOK) {
		// The script prints nothing of the content; stderr is the shell's own
		// messages (a path, a permission error).
		return fmt.Errorf("%w: exit %d: %s", ErrEnvFile, res.ExitCode, tail(res.Stderr, 512))
	}
	return nil
}

// envFileCommand is the guest command that writes content to file. The
// content travels base64-encoded inside the base64-encoded script, so it is
// never shell text, and the script never prints it. The temporary file is
// created under umask 077 (0600 from the first byte) next to file, then
// renamed over it; it is removed if any step fails. file must not be an
// existing directory (or a link to one), where mv would move into it.
func envFileCommand(file, content string) string {
	script := fmt.Sprintf(`main() {
set -e
umask 077
f=%s
d=%s
t="$f.spawn-tmp"
trap 'rm -f "$t"' EXIT
mkdir -p "$d"
chmod 700 "$d"
if [ -d "$f" ]; then echo "spawn: $f is a directory" >&2; exit 1; fi
rm -f "$t"
printf %%s %s | base64 -d > "$t"
chmod 600 "$t"
mv -f "$t" "$f"
echo %s
}
main </dev/null
`, shellQuote(file), shellQuote(path.Dir(file)), base64.StdEncoding.EncodeToString([]byte(content)), envFileOK)
	return "printf %s " + base64.StdEncoding.EncodeToString([]byte(script)) + " | base64 -d | sh"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// tail returns the last n bytes of s.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
