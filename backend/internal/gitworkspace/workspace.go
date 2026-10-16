// Package gitworkspace prepares an agent session's pinned pull-request
// checkout inside its sandbox. The guest fetches through the worker's
// read-only git proxy (internal/gitproxy), so no credential is ever placed in
// the guest. This package builds the guest script and sequences
// grant -> checkout -> revoke; the platform pins the commits and mints the
// repository-scoped token.
package gitworkspace

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"strings"
	"time"

	"backend/internal/plane"
)

const (
	// DefaultDir is the checkout; DefaultReviewDir holds the diff artifacts the
	// read-only agent reads instead of running git. They must not nest.
	DefaultDir       = "/workspace/repo"
	DefaultReviewDir = "/review"

	exitCommitUnavailable = 3
	exitHeadMismatch      = 4
	checkoutTimeoutS      = 900
	maxErrorOutput        = 2000
)

var (
	ErrInvalidSpec = errors.New("invalid workspace spec")
	// ErrCommitUnavailable means the base repository can no longer supply a
	// pinned commit (force-pushed PR, deleted fork). Fail closed: never
	// substitute another commit.
	ErrCommitUnavailable = errors.New("pinned commit is not available from the repository")
	ErrHeadMismatch      = errors.New("checkout does not match the pinned head commit")

	shaPattern    = regexp.MustCompile(`^[0-9a-f]{40}$|^[0-9a-f]{64}$`)
	remotePattern = regexp.MustCompile(`^http://172\.16\.0\.1:[0-9]{1,5}/[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9._-]{1,100}\.git$`)
	pathPattern   = regexp.MustCompile(`^(/[A-Za-z0-9._-]+)+$`)

	protectedDirs = map[string]bool{
		"/bin": true, "/boot": true, "/dev": true, "/etc": true, "/home": true, "/lib": true, "/lib64": true,
		"/opt": true, "/proc": true, "/root": true, "/run": true, "/sbin": true, "/sys": true, "/tmp": true,
		"/usr": true, "/var": true, "/workspace": true,
	}
)

// Spec is everything the guest needs to check out a pinned pull request. It
// carries no credentials.
type Spec struct {
	RemoteURL string // from plane.GitAccessResponse
	HeadSHA   string // PR head: the checkout
	BaseSHA   string // PR base: the comparison baseline
	Dir       string
	ReviewDir string
}

func (s Spec) Validate() error {
	if !remotePattern.MatchString(s.RemoteURL) {
		return fmt.Errorf("%w: remote must be the worker git proxy", ErrInvalidSpec)
	}
	return s.validateLocal()
}

func (s Spec) validateLocal() error {
	if !shaPattern.MatchString(s.HeadSHA) || !shaPattern.MatchString(s.BaseSHA) {
		return fmt.Errorf("%w: head and base must be exact commit SHAs", ErrInvalidSpec)
	}
	for _, dir := range []string{s.Dir, s.ReviewDir} {
		if !pathPattern.MatchString(dir) || path.Clean(dir) != dir || strings.Contains(dir, "/..") || protectedDirs[dir] {
			return fmt.Errorf("%w: %q is not a safe workspace directory", ErrInvalidSpec, dir)
		}
	}
	if s.Dir == s.ReviewDir || strings.HasPrefix(s.Dir, s.ReviewDir+"/") || strings.HasPrefix(s.ReviewDir, s.Dir+"/") {
		return fmt.Errorf("%w: checkout and review directories must not nest", ErrInvalidSpec)
	}
	return nil
}

// Script returns an idempotent bash script that checks out HeadSHA detached,
// verifies it, and writes pr.diff, changed-files.txt, diffstat.txt and
// commits.txt into ReviewDir (diffs are against the merge base). Global and
// system git config are ignored so nothing in the guest (credential helpers,
// URL rewrites, hooks) takes part. Exit 3: a pinned commit is unavailable.
// Exit 4: the checkout does not match the pin.
func Script(s Spec) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	return buildScript(s), nil
}

func buildScript(s Spec) string {
	return fmt.Sprintf(`set -euo pipefail
export GIT_TERMINAL_PROMPT=0 GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_ASKPASS=/bin/false SSH_ASKPASS=/bin/false
remote=%s
head=%s
base=%s
dir=%s
review=%s
g() { git -c protocol.version=2 -c credential.helper= -c core.hooksPath=/dev/null -c advice.detachedHead=false "$@"; }
if [ "$(cat "$review/.pin" 2>/dev/null || true)" = "$head $base" ] && [ "$(g -C "$dir" rev-parse HEAD 2>/dev/null || true)" = "$head" ]; then
	echo "workspace already prepared at $head"
	exit 0
fi
rm -rf -- "$dir" "$review"
mkdir -p -- "$dir" "$review"
g init -q "$dir"
cd -- "$dir"
g remote add origin "$remote"
g config remote.origin.promisor true
g config remote.origin.partialclonefilter blob:none
if ! g fetch -q --no-tags --filter=blob:none origin "$head" "$base"; then
	echo "pinned commits are not available from the repository; the pull request may have changed or its fork was removed" >&2
	exit %d
fi
g checkout -q --detach "$head"
if [ "$(g rev-parse HEAD)" != "$head" ]; then
	echo "checkout does not match the pinned head commit" >&2
	exit %d
fi
mb="$(g merge-base "$base" "$head" || true)"
[ -n "$mb" ] || mb="$base"
g diff --no-color --no-ext-diff --no-textconv "$mb" "$head" > "$review/pr.diff"
g diff --no-color --no-ext-diff --name-status -M "$mb" "$head" > "$review/changed-files.txt"
g diff --no-color --no-ext-diff --stat "$mb" "$head" > "$review/diffstat.txt"
printf 'head %%s\nbase %%s\nmerge-base %%s\n' "$head" "$base" "$mb" > "$review/commits.txt"
printf '%%s %%s' "$head" "$base" > "$review/.pin"
echo "workspace prepared at $head"
`, shellQuote(s.RemoteURL), shellQuote(s.HeadSHA), shellQuote(s.BaseSHA), shellQuote(s.Dir), shellQuote(s.ReviewDir),
		exitCommitUnavailable, exitHeadMismatch)
}

// Command wraps Script for the guest's persistent shell. It runs in a child
// bash so `set -e` and `exit` never end the persistent session, and is
// base64-encoded so no quoting survives into the outer shell.
func Command(s Spec) (string, error) {
	script, err := Script(s)
	if err != nil {
		return "", err
	}
	return "printf '%s' '" + base64.StdEncoding.EncodeToString([]byte(script)) + "' | base64 -d | bash", nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// Pin is a session's immutable checkout input plus the platform-minted grant.
type Pin struct {
	Repository     string // base repository owner/name
	HeadSHA        string
	BaseSHA        string
	Token          string // repository-scoped, contents:read; never logged
	TokenExpiresAt time.Time
}

// Worker is the part of the worker API Prepare needs; agent.Client satisfies it.
type Worker interface {
	GrantGitAccess(ctx context.Context, sandboxID string, req plane.GitAccessRequest) (plane.GitAccessResponse, error)
	RevokeGitAccess(ctx context.Context, sandboxID string) error
	Exec(ctx context.Context, sandboxID string, req plane.ExecRequest) (plane.ExecResult, error)
}

type Result struct {
	Dir, ReviewDir   string
	HeadSHA, BaseSHA string
}

// Prepare checks out pin inside the sandbox through the git proxy and always
// revokes the grant afterwards, so the token lives on the worker only for the
// checkout. Re-running after success is a no-op in the guest.
func Prepare(ctx context.Context, worker Worker, sandboxID string, pin Pin) (Result, error) {
	spec := Spec{HeadSHA: pin.HeadSHA, BaseSHA: pin.BaseSHA, Dir: DefaultDir, ReviewDir: DefaultReviewDir}
	if err := spec.validateLocal(); err != nil {
		return Result{}, err
	}
	grant, err := worker.GrantGitAccess(ctx, sandboxID, plane.GitAccessRequest{
		Repository: pin.Repository, Token: pin.Token, ExpiresAt: pin.TokenExpiresAt,
	})
	if err != nil {
		return Result{}, fmt.Errorf("grant git access to sandbox %s: %w", sandboxID, err)
	}
	defer func() {
		revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := worker.RevokeGitAccess(revokeCtx, sandboxID); err != nil {
			slog.Warn("revoke git access failed", "sandbox_id", sandboxID, "err", err)
		}
	}()

	spec.RemoteURL = grant.RemoteURL
	command, err := Command(spec)
	if err != nil {
		return Result{}, err
	}
	// Quiet: the checkout is platform-internal and its output stays out of
	// the sandbox's log stream.
	res, err := worker.Exec(ctx, sandboxID, plane.ExecRequest{Command: command, TimeoutS: checkoutTimeoutS, Quiet: true})
	if err != nil {
		return Result{}, fmt.Errorf("run checkout in sandbox %s: %w", sandboxID, err)
	}
	switch res.ExitCode {
	case 0:
		return Result{Dir: spec.Dir, ReviewDir: spec.ReviewDir, HeadSHA: pin.HeadSHA, BaseSHA: pin.BaseSHA}, nil
	case exitCommitUnavailable:
		return Result{}, fmt.Errorf("%w: %s", ErrCommitUnavailable, tail(res.Stderr))
	case exitHeadMismatch:
		return Result{}, fmt.Errorf("%w: %s", ErrHeadMismatch, tail(res.Stderr))
	default:
		return Result{}, fmt.Errorf("checkout in sandbox %s failed (exit %d): %s", sandboxID, res.ExitCode, tail(res.Stderr))
	}
}

func tail(output string) string {
	output = strings.TrimSpace(output)
	if len(output) > maxErrorOutput {
		output = "…" + output[len(output)-maxErrorOutput:]
	}
	return output
}
