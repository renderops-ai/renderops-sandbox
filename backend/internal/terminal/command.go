package terminal

import (
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode/utf8"
)

// A terminal can start in a directory, load an env file and run a command
// instead of leaving the user at a prompt (the spawn CLI opens `claude` this
// way). The guest agent's PTY always starts `/bin/bash -l` and takes no
// command, so the worker passes the startup as the shell's PROMPT_COMMAND: an
// interactive bash runs it once, after the login profile and before the first
// prompt. Nothing is typed into the PTY, so nothing is echoed.
//
// PROMPT_COMMAND is a fixed wrapper around a server-built script. User values
// (the directory, the env file path, every argument) reach the guest only as
// base64 literals, decoded into variables by the script, so none of them is
// ever shell text. The script unsets PROMPT_COMMAND first, so neither the
// command nor the shells it starts inherit it.
const (
	// CommandWorkspace is the only tree a terminal may start in.
	CommandWorkspace = "/workspace"
	// EnvFileDir holds the env files a terminal may load (outside the
	// workspace, so they are never committed).
	EnvFileDir = "/var/lib/spawn"
	// MaxCommandArgs and MaxCommandBytes bound the argv; the startup env
	// value stays far below the kernel's 128 KiB limit for one variable.
	MaxCommandArgs  = 256
	MaxCommandBytes = 16 << 10
	// MaxCommandPathBytes bounds Cwd and EnvFile.
	MaxCommandPathBytes = 4096

	// ExitStartupFailed is the terminal's exit code when the directory or the
	// env file cannot be used; ExitCommandFailed when the command cannot be
	// started (not found, not executable).
	ExitStartupFailed = 125
	ExitCommandFailed = 127

	promptCommandEnv = "PROMPT_COMMAND"
)

// ErrCommandUnsupported means the sandbox's worker predates terminal commands:
// it would have opened a plain shell, so the terminal was closed instead.
var ErrCommandUnsupported = errors.New("the worker does not support terminal commands")

// Command is how a terminal starts. The zero value is a plain login shell.
type Command struct {
	// Argv replaces the shell (exec); empty keeps the interactive shell.
	Argv []string
	// Cwd is an absolute directory in CommandWorkspace; empty keeps the
	// guest's default (/workspace).
	Cwd string
	// EnvFile is an absolute file under EnvFileDir, sourced with every
	// assignment exported before the command starts.
	EnvFile string
}

// IsZero reports whether c asks for nothing beyond a plain shell.
func (c Command) IsZero() bool {
	return len(c.Argv) == 0 && c.Cwd == "" && c.EnvFile == ""
}

// Validate checks every field. Its errors are safe to show the caller.
func (c Command) Validate() error {
	if len(c.Argv) > MaxCommandArgs {
		return fmt.Errorf("command must have at most %d arguments", MaxCommandArgs)
	}
	total := 0
	for _, arg := range c.Argv {
		if strings.IndexByte(arg, 0) >= 0 || !utf8.ValidString(arg) {
			return errors.New("command arguments must be valid UTF-8 without NUL bytes")
		}
		total += len(arg)
	}
	if total > MaxCommandBytes {
		return fmt.Errorf("command must be at most %d bytes", MaxCommandBytes)
	}
	if len(c.Argv) > 0 && (c.Argv[0] == "" || strings.HasPrefix(c.Argv[0], "-")) {
		return errors.New("command[0] must name a program and must not start with '-'")
	}
	if c.Cwd != "" && !cleanPathUnder(c.Cwd, CommandWorkspace, true) {
		return fmt.Errorf("cwd must be an absolute, clean path inside %s", CommandWorkspace)
	}
	if c.EnvFile != "" && !cleanPathUnder(c.EnvFile, EnvFileDir, false) {
		return fmt.Errorf("env_file must be an absolute, clean path under %s/", EnvFileDir)
	}
	return nil
}

// cleanPathUnder reports whether p is absolute, already clean, NUL-free, at
// most MaxCommandPathBytes and below root (or root itself when allowRoot).
func cleanPathUnder(p, root string, allowRoot bool) bool {
	if len(p) > MaxCommandPathBytes || strings.IndexByte(p, 0) >= 0 || !utf8.ValidString(p) ||
		!strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return false
	}
	if p == root {
		return allowRoot
	}
	return strings.HasPrefix(p, root+"/")
}

// StartupEnv is the environment the worker adds to the terminal's shell to
// apply c (call Validate first). It is empty for the zero Command.
func (c Command) StartupEnv() map[string]string {
	if c.IsZero() {
		return nil
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(c.startupScript()))
	return map[string]string{
		promptCommandEnv: "unset PROMPT_COMMAND; eval \"$(printf %s '" + encoded + "' | base64 -d)\"",
	}
}

// startupScript runs in the interactive shell before its first prompt:
// change directory, source the env file with every assignment exported, then
// exec the command (the terminal ends when it exits). Each user value is a
// base64 literal; dec prints it followed by "x" so command substitution keeps
// trailing newlines (the x is stripped).
func (c Command) startupScript() string {
	var b strings.Builder
	b.WriteString("unset PROMPT_COMMAND\n")
	b.WriteString("__spawn_dec() { printf %s \"$1\" | base64 -d && printf x; }\n")
	fail := func(message string) string {
		return fmt.Sprintf("{ printf 'spawn: %s %%s\\n' \"${__spawn_v%%x}\" >&2; exit %d; }", message, ExitStartupFailed)
	}
	if c.Cwd != "" {
		fmt.Fprintf(&b, "__spawn_v=$(__spawn_dec %s) || exit %d\n", quotedBase64(c.Cwd), ExitStartupFailed)
		b.WriteString("cd -- \"${__spawn_v%x}\" || " + fail("cannot enter the directory") + "\n")
	}
	if c.EnvFile != "" {
		fmt.Fprintf(&b, "__spawn_v=$(__spawn_dec %s) || exit %d\n", quotedBase64(c.EnvFile), ExitStartupFailed)
		b.WriteString("[ -f \"${__spawn_v%x}\" ] && [ -r \"${__spawn_v%x}\" ] || " + fail("cannot read the env file") + "\n")
		b.WriteString("set -a\n. \"${__spawn_v%x}\"\n__spawn_rc=$?\nset +a\n")
		b.WriteString("[ \"$__spawn_rc\" = 0 ] || " + fail("failed to load the env file") + "\n")
	}
	if len(c.Argv) > 0 {
		b.WriteString("set --\n")
		for _, arg := range c.Argv {
			fmt.Fprintf(&b, "__spawn_v=$(__spawn_dec %s) || exit %d\n", quotedBase64(arg), ExitStartupFailed)
			b.WriteString("set -- \"$@\" \"${__spawn_v%x}\"\n")
		}
	}
	b.WriteString("unset -v __spawn_v __spawn_rc\nunset -f __spawn_dec\n")
	if len(c.Argv) > 0 {
		// An interactive shell survives a failed exec; end the terminal.
		fmt.Fprintf(&b, "exec \"$@\"\nexit %d\n", ExitCommandFailed)
	}
	return b.String()
}

func quotedBase64(value string) string {
	return "'" + base64.StdEncoding.EncodeToString([]byte(value)) + "'"
}
