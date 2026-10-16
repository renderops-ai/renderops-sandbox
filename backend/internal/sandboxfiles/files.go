// Package sandboxfiles reads and writes files in a sandbox's /workspace for
// the dashboard's Code tab. Every operation is a short script run through the
// guest's exec path (see scripts.go); the guest agent has no file operations.
package sandboxfiles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"backend/internal/plane"

	"github.com/google/uuid"
)

const (
	// Workspace is the only guest directory the files API reads or writes.
	Workspace = "/workspace"
	// StagingDir holds a large write's chunks until the final rename. It is
	// on the sandbox's disk, outside the workspace, so it never shows in the
	// file tree.
	StagingDir = "/var/tmp/spawn-files"

	// MaxFileBytes is the largest file read or written.
	MaxFileBytes = 2 << 20
	// MaxPathBytes is the longest path accepted.
	MaxPathBytes = 4096
	// MaxTreeEntries caps the tree listing.
	MaxTreeEntries = 20000
	// MaxChanges caps the changes listing.
	MaxChanges = 2000
	// WriteChunkSize is the most base64 content text one exec carries. The
	// guest's persistent shell reads the whole command (this text, in a
	// base64-wrapped script: about 4/3 of it) before running it, so a 2 MiB
	// file goes as 11 commands of about 350 KB instead of one of 3.7 MB.
	// Content that fits in one chunk is written by a single exec.
	WriteChunkSize = 256 << 10

	// maxListBytes bounds each git diff listing the changes script returns.
	maxListBytes = 2 << 20
	// binarySniffBytes is how much of a file is checked for NUL bytes.
	binarySniffBytes = 8 << 10
	// staleStagingMinutes: staging files this old are left over from failed
	// writes and are removed when the next large write starts.
	staleStagingMinutes = 60

	readTimeoutS  = 30
	writeTimeoutS = 60
	chunkTimeoutS = 30
)

// Error codes. The handler maps them to HTTP statuses.
const (
	CodeInvalidPath    = "invalid_path"
	CodeInvalidRev     = "invalid_rev"
	CodeNotFound       = "not_found"        // tree/changes root missing
	CodeNotADirectory  = "not_a_directory"  // tree/changes root is a file
	CodeFileNotFound   = "file_not_found"   // content read of a missing file
	CodeNotAFile       = "not_a_file"       // directory, device, fifo, ...
	CodeParentNotFound = "parent_not_found" // write into a missing directory
	CodeNotAGitRepo    = "not_a_git_repo"
	CodeRevNotFound    = "rev_not_found"
	CodeNotInRevision  = "not_in_revision"
	CodeFileTooLarge   = "file_too_large"
	CodeConflict       = "conflict"
	CodeExists         = "exists"
	CodeInvalidSHA256  = "invalid_sha256"
	// CodeGuestFailed covers everything else: the exec or the script failed
	// unexpectedly (the scripts' own write_failed, write_corrupt,
	// stage_failed and read_failed codes are logged and reported as this).
	CodeGuestFailed     = "files_failed"
	defaultGuestMessage = "the sandbox could not complete the files request"
)

// Error is a files API error with its code (see the Code constants).
type Error struct {
	Code    string
	Message string
	// Size is the file's size for CodeFileTooLarge (0 when unknown).
	Size int64
	// SHA256 is the file's current sha256 for CodeConflict; nil when the file
	// no longer exists.
	SHA256 *string
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

var errorMessages = map[string]string{
	CodeInvalidPath:    "path must be absolute, clean, without NUL bytes, at most 4096 bytes and inside /workspace",
	CodeInvalidRev:     "rev must be a 40-character hex commit sha",
	CodeNotFound:       "directory not found",
	CodeNotADirectory:  "root is not a directory",
	CodeFileNotFound:   "file not found",
	CodeNotAFile:       "path is not a regular file",
	CodeParentNotFound: "the file's directory does not exist",
	CodeNotAGitRepo:    "path is not inside a git repository",
	CodeRevNotFound:    "commit not found in the repository",
	CodeNotInRevision:  "file does not exist in that commit",
	CodeFileTooLarge:   "file is larger than 2 MiB",
	CodeConflict:       "file changed since it was loaded",
	CodeExists:         "file already exists",
	CodeInvalidSHA256:  "expected_sha256 must be a hex sha256 or null",
}

// newError returns the error for a code; codes the API does not define
// become CodeGuestFailed.
func newError(code string) *Error {
	message, ok := errorMessages[code]
	if !ok {
		return &Error{Code: CodeGuestFailed, Message: defaultGuestMessage}
	}
	return &Error{Code: code, Message: message}
}

// ExecFunc runs one command in the sandbox's guest shell. Passive commands
// must not count as sandbox activity.
type ExecFunc func(ctx context.Context, command string, timeoutS int, passive bool) (plane.ExecResult, error)

// guestConfig is the guest locations and listing caps scripts use; tests
// point the locations at a temporary directory and lower the caps.
type guestConfig struct {
	Workspace  string
	Staging    string
	TreeMax    int
	ChangesMax int
}

func defaultConfig() guestConfig {
	return guestConfig{Workspace: Workspace, Staging: StagingDir, TreeMax: MaxTreeEntries, ChangesMax: MaxChanges}
}

// Client runs files operations in one sandbox.
type Client struct {
	exec ExecFunc
	cfg  guestConfig
	log  *slog.Logger
}

// New returns a client that runs its scripts through exec.
func New(exec ExecFunc, log *slog.Logger) *Client {
	return &Client{exec: exec, cfg: defaultConfig(), log: log}
}

// Tree is the file listing of a directory.
type Tree struct {
	Root      string   `json:"root"`
	Git       bool     `json:"git"`
	Files     []string `json:"files"`
	Truncated bool     `json:"truncated"`
}

// File is a file's content. Content is nil for binary files.
type File struct {
	Path    string  `json:"path"`
	Size    int64   `json:"size"`
	SHA256  string  `json:"sha256"`
	Binary  bool    `json:"binary"`
	Content *string `json:"content,omitempty"`
	Mode    string  `json:"mode"`
}

// Written is the result of a write.
type Written struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Change is one changed file. From is the old path of a rename.
type Change struct {
	Path      string `json:"path"`
	From      string `json:"from,omitempty"`
	Status    string `json:"status"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

// Changes is what changed under a root since a base commit.
type Changes struct {
	Base      string   `json:"base"`
	Head      string   `json:"head"`
	Files     []Change `json:"files"`
	Truncated bool     `json:"truncated"`
}

var (
	revPattern  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	shaPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	modePattern = regexp.MustCompile(`^[0-7]{6}$`)
)

// ValidRev reports whether rev is a full lower-case commit sha.
func ValidRev(rev string) bool {
	return revPattern.MatchString(rev)
}

// ValidSHA256 reports whether sum is a lower-case hex sha256.
func ValidSHA256(sum string) bool {
	return shaPattern.MatchString(sum)
}

// checkPath validates a path before it is sent to the guest: absolute,
// already clean, NUL-free, at most MaxPathBytes, and lexically inside the
// workspace (the workspace itself only when allowRoot). The guest checks it
// again after resolving symlinks.
func (c *Client) checkPath(p string, allowRoot bool) error {
	ws := c.cfg.Workspace
	if p == "" || len(p) > MaxPathBytes || strings.IndexByte(p, 0) >= 0 ||
		!strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return newError(CodeInvalidPath)
	}
	if p == ws {
		if allowRoot {
			return nil
		}
		return newError(CodeNotAFile)
	}
	if !strings.HasPrefix(p, ws+"/") {
		return newError(CodeInvalidPath)
	}
	return nil
}

// checkWritePath also refuses any path with a .git component.
func (c *Client) checkWritePath(p string) error {
	if err := c.checkPath(p, false); err != nil {
		return err
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".git" {
			return newError(CodeInvalidPath)
		}
	}
	return nil
}

// run executes a script and parses its answer. A script error becomes *Error.
func (c *Client) run(ctx context.Context, op, script string, timeoutS int, passive bool) (*output, error) {
	res, err := c.exec(ctx, shellCommand(script), timeoutS, passive)
	if err != nil {
		return nil, fmt.Errorf("files %s: %w", op, err)
	}
	out, parseErr := parseOutput(res.Stdout)
	if parseErr == nil && out.errCode != "" {
		fe := out.err()
		if fe.Code == CodeGuestFailed {
			c.log.Warn("files script failed", "op", op, "guest_code", out.errCode,
				"exit_code", res.ExitCode, "stderr", truncate(res.Stderr, 2048))
		}
		return nil, fe
	}
	if parseErr == nil && !out.ok {
		parseErr = errors.New("script output is incomplete")
	}
	if parseErr != nil {
		c.log.Warn("files script failed", "op", op, "exit_code", res.ExitCode,
			"stderr", truncate(res.Stderr, 2048), "err", parseErr)
		return nil, &Error{Code: CodeGuestFailed, Message: defaultGuestMessage}
	}
	return out, nil
}

// Tree lists the files under root (see treeScript).
func (c *Client) Tree(ctx context.Context, root string) (Tree, error) {
	if err := c.checkPath(root, true); err != nil {
		return Tree{}, err
	}
	out, err := c.run(ctx, "tree", treeScript(c.cfg, root), readTimeoutS, true)
	if err != nil {
		return Tree{}, err
	}
	isGit := out.fields["git"] == "1"
	deletedNames := splitNUL(out.data["deleted"])
	deleted := make(map[string]bool, len(deletedNames))
	for _, name := range deletedNames {
		deleted[name] = true
	}
	listed := splitNUL(out.data["files"])
	files := make([]string, 0, min(len(listed), c.cfg.TreeMax))
	seen := map[string]bool{}
	// The script stops listing at TreeMax+1 entries plus the deleted ones:
	// reaching that means there are more files than the tree shows.
	truncated := len(listed) >= c.cfg.TreeMax+1+len(deletedNames)
	for _, name := range listed {
		if !isGit {
			name = strings.TrimPrefix(name, "./")
		}
		// "dir/" is an untracked nested repository; deleted files are gone.
		if name == "" || strings.HasSuffix(name, "/") || deleted[name] || seen[name] {
			continue
		}
		if len(files) == c.cfg.TreeMax {
			truncated = true
			break
		}
		seen[name] = true
		files = append(files, name)
	}
	sort.Strings(files)
	return Tree{Root: root, Git: isGit, Files: files, Truncated: truncated}, nil
}

// Read returns a file's content from the work tree, or from commit rev when
// rev is set.
func (c *Client) Read(ctx context.Context, p, rev string) (File, error) {
	if err := c.checkPath(p, false); err != nil {
		return File{}, err
	}
	if rev != "" && !ValidRev(rev) {
		return File{}, newError(CodeInvalidRev)
	}
	out, err := c.run(ctx, "read", readScript(c.cfg, p, rev), readTimeoutS, true)
	if err != nil {
		return File{}, err
	}
	content := out.data["content"]
	if len(content) > MaxFileBytes {
		// The file grew between the size check and the read.
		e := newError(CodeFileTooLarge)
		e.Size = max(out.size(), int64(len(content)))
		return File{}, e
	}
	sum := sha256.Sum256(content)
	file := File{
		Path:   p,
		Size:   int64(len(content)),
		SHA256: hex.EncodeToString(sum[:]),
		Binary: isBinary(content),
		Mode:   out.fields["mode"],
	}
	if !modePattern.MatchString(file.Mode) {
		file.Mode = "100644"
	}
	if !file.Binary {
		text := string(content)
		file.Content = &text
	}
	return file, nil
}

// isBinary: a NUL byte in the first 8 KiB, or invalid UTF-8.
func isBinary(content []byte) bool {
	return bytes.IndexByte(content[:min(len(content), binarySniffBytes)], 0) >= 0 || !utf8.Valid(content)
}

// Write replaces the file at p with content when its current sha256 is
// expected, or creates it when expected is nil (see writeScript). Content
// larger than WriteChunkSize as base64 is uploaded in chunks to a staging
// file first.
func (c *Client) Write(ctx context.Context, p string, content []byte, expected *string) (Written, error) {
	if err := c.checkWritePath(p); err != nil {
		return Written{}, err
	}
	if len(content) > MaxFileBytes {
		e := newError(CodeFileTooLarge)
		e.Size = int64(len(content))
		return Written{}, e
	}
	exp := ""
	if expected != nil {
		exp = strings.ToLower(*expected)
		if !ValidSHA256(exp) {
			return Written{}, newError(CodeInvalidSHA256)
		}
	}
	sum := sha256.Sum256(content)
	want := hex.EncodeToString(sum[:])
	encoded := base64.StdEncoding.EncodeToString(content)
	written := Written{Path: p, Size: int64(len(content)), SHA256: want}

	if len(encoded) <= WriteChunkSize {
		_, err := c.run(ctx, "write", writeScript(c.cfg, p, exp, want, writeInline, encoded, ""), writeTimeoutS, false)
		if err != nil {
			return Written{}, err
		}
		return written, nil
	}

	// Large content: check first (no upload when it would conflict), stage
	// the chunks, then write from the staging file, checking again.
	if _, err := c.run(ctx, "write check", writeScript(c.cfg, p, exp, want, writeCheck, "", ""), writeTimeoutS, false); err != nil {
		return Written{}, err
	}
	staging := c.cfg.Staging + "/" + uuid.NewString() + ".b64"
	for i, chunk := range chunks(encoded, WriteChunkSize) {
		if _, err := c.run(ctx, "write chunk", chunkScript(c.cfg, staging, chunk, i == 0), chunkTimeoutS, false); err != nil {
			c.removeStaging(ctx, staging)
			return Written{}, err
		}
	}
	// The final script removes the staging file however it ends.
	if _, err := c.run(ctx, "write", writeScript(c.cfg, p, exp, want, writeStaged, "", staging), writeTimeoutS, false); err != nil {
		return Written{}, err
	}
	return written, nil
}

// removeStaging deletes a failed upload's staging file, best effort: a file
// left behind is removed by the next large write after staleStagingMinutes.
func (c *Client) removeStaging(ctx context.Context, staging string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := c.run(cleanupCtx, "remove staging", removeScript(c.cfg, staging), chunkTimeoutS, false); err != nil {
		c.log.Warn("removing a staged upload failed", "staging", staging, "err", err)
	}
}

// chunks splits s into pieces of at most size bytes.
func chunks(s string, size int) []string {
	var out []string
	for len(s) > size {
		out = append(out, s[:size])
		s = s[size:]
	}
	return append(out, s)
}

// Changes lists what changed under root since commit base.
func (c *Client) Changes(ctx context.Context, root, base string) (Changes, error) {
	if err := c.checkPath(root, true); err != nil {
		return Changes{}, err
	}
	if !ValidRev(base) {
		return Changes{}, newError(CodeInvalidRev)
	}
	out, err := c.run(ctx, "changes", changesScript(c.cfg, root, base), readTimeoutS, true)
	if err != nil {
		return Changes{}, err
	}
	head := out.fields["head"]
	if !ValidRev(head) {
		head = "" // unborn branch
	}
	files, truncated := parseChanges(out.data["status"], out.data["numstat"], out.data["untracked"], c.cfg.ChangesMax)
	return Changes{Base: base, Head: head, Files: files, Truncated: truncated}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
