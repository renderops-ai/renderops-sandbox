package sandboxfiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"time"
)

// A patch is everything that changed under a root since a base commit, as
// `git diff --binary <base>` of the work tree: committed and uncommitted
// changes, and untracked (not ignored) files as new files. Untracked files
// are added with `git add -N` to a copy of the index in the staging
// directory, so the repository's own index is never written.
//
// The patch is written to a staging file first (capped at MaxPatchBytes),
// then read back in PatchChunkBytes pieces, one exec each, so no single guest
// answer is large; the caller streams the pieces to its client.
const (
	// MaxPatchBytes caps a patch.
	MaxPatchBytes = 50 << 20
	// PatchChunkBytes is how much of the patch one exec returns.
	PatchChunkBytes = 2 << 20

	patchTimeoutS = 120
)

// Patch is a patch prepared in the guest, ready to read (WritePatch) and
// then remove (RemovePatch).
type Patch struct {
	Base   string
	Head   string // "" on an unborn branch
	Size   int64
	SHA256 string
	file   string
}

// PreparePatch writes the patch of root against base to a staging file.
// Like the other reads it does not count as sandbox activity.
func (c *Client) PreparePatch(ctx context.Context, root, base string) (*Patch, error) {
	if err := c.checkPath(root, true); err != nil {
		return nil, err
	}
	if !ValidRev(base) {
		return nil, newError(CodeInvalidRev)
	}
	file := c.cfg.Staging + "/patch-" + newStagingID() + ".diff"
	out, err := c.run(ctx, "patch", patchScript(c.cfg, root, base, file), patchTimeoutS, true)
	if err != nil {
		return nil, err
	}
	sum := out.fields["sha256"]
	size, sizeErr := strconv.ParseInt(out.fields["size"], 10, 64)
	if sizeErr != nil || size < 0 || size > c.cfg.PatchMax || !ValidSHA256(sum) {
		c.log.Warn("patch script answered without a usable size or sha256", "size", out.fields["size"])
		c.removeStaging(ctx, file)
		return nil, &Error{Code: CodeGuestFailed, Message: defaultGuestMessage}
	}
	head := out.fields["head"]
	if !ValidRev(head) {
		head = ""
	}
	return &Patch{Base: base, Head: head, Size: size, SHA256: sum, file: file}, nil
}

// WritePatch copies the prepared patch to w, one chunk per exec, and checks
// it against the patch's size and sha256. An error after the first write
// leaves w with a partial patch; the caller must make that visible (a short
// body against its Content-Length).
func (c *Client) WritePatch(ctx context.Context, p *Patch, w io.Writer) error {
	hash := sha256.New()
	for offset := int64(0); offset < p.Size; {
		want := min(c.cfg.PatchChunk, p.Size-offset)
		out, err := c.run(ctx, "patch chunk", patchChunkScript(c.cfg, p.file, offset, want), readTimeoutS, true)
		if err != nil {
			return err
		}
		chunk := out.data["chunk"]
		if int64(len(chunk)) != want {
			return fmt.Errorf("patch chunk at %d has %d bytes, want %d", offset, len(chunk), want)
		}
		hash.Write(chunk)
		if _, err := w.Write(chunk); err != nil {
			return fmt.Errorf("write patch chunk: %w", err)
		}
		offset += want
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != p.SHA256 {
		return fmt.Errorf("patch changed while it was read: sha256 %s, want %s", got, p.SHA256)
	}
	return nil
}

// RemovePatch deletes the staging file, best effort (a file left behind is
// removed after staleStagingMinutes by the next staging script).
func (c *Client) RemovePatch(ctx context.Context, p *Patch) {
	if p == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := c.run(cleanupCtx, "remove patch", removeScript(c.cfg, p.file), chunkTimeoutS, true); err != nil {
		c.log.Warn("removing a patch staging file failed", "err", err)
	}
}

// patchScript writes the patch to out and prints its head, size and sha256.
//
//   - Untracked files: `git ls-files -o` lists them (relative to root);
//     untracked nested repositories ("dir/") are dropped, NUL-safely, by a
//     shell `case` over xargs' arguments; the rest are added as intent-to-add
//     to the index copy (literal pathspecs, so names are never globs).
//   - The diff ignores configuration that would change its format: external
//     diff and textconv drivers, colour, prefixes, submodule logs. Renames are
//     detected (git apply understands them). --relative keeps it to root.
//   - head -c stops at max+1 bytes, so a huge diff never fills the disk; git's
//     own status is kept to tell its failure from that cut.
func patchScript(cfg guestConfig, root, base, out string) string {
	return script(cfg, `r=$(dec @ROOT@) || fail invalid_path
r=${r%x}
base=@BASE@
max=@MAX@
st=@STAGE@
out=@OUT@
idx=
keep=
cleanup() {
	[ -z "$idx" ] || rm -rf "$idx"
	[ -n "$keep" ] || rm -f "$out"
}
trap cleanup EXIT
resolve "$r" || fail invalid_path
inside "$R" || fail invalid_path
[ -e "$R" ] || fail not_found
[ -d "$R" ] || fail not_a_directory
cd "$R" || fail not_found
[ "$(git rev-parse --is-inside-work-tree 2>/dev/null)" = true ] || fail not_a_git_repo
git cat-file -e "$base^{commit}" 2>/dev/null || fail rev_not_found
echo "head $(git rev-parse -q --verify HEAD 2>/dev/null)"
umask 077
mkdir -p "$st" || fail stage_failed
find "$st" -mindepth 1 -maxdepth 1 -mmin +@STALE@ -exec rm -rf {} + 2>/dev/null
idx=$(mktemp -d "$st/patch-index.XXXXXX") || { idx=; fail stage_failed; }
real=$(git rev-parse --path-format=absolute --git-path index) || fail patch_failed
if [ -f "$real" ]; then
	cp "$real" "$idx/index" || fail patch_failed
fi
export GIT_INDEX_FILE="$idx/index"
git ls-files -z -o --exclude-standard |
	xargs -0 sh -c 'for f do case $f in */) ;; *) printf "%s\000" "$f" ;; esac; done' sh |
	git -c core.fsmonitor=false -c advice.addEmptyPathspec=false --literal-pathspecs add -N --pathspec-from-file=- --pathspec-file-nul || fail patch_failed
{
	git -c core.fsmonitor=false -c core.quotePath=true diff --binary --no-color --no-ext-diff --no-textconv \
		--src-prefix=a/ --dst-prefix=b/ --submodule=short --find-renames --relative "$base" --
	echo "$?" > "$idx/status"
} | head -c "$((max + 1))" > "$out" || fail patch_failed
size=$(($(wc -c < "$out")))
echo "size $size"
[ "$size" -le "$max" ] || fail patch_too_large
[ "$(cat "$idx/status")" = 0 ] || fail patch_failed
echo "sha256 $(sha < "$out")"
keep=1
echo ok`,
		"@ROOT@", b64([]byte(root)),
		"@BASE@", shellQuote(base),
		"@MAX@", strconv.FormatInt(cfg.PatchMax, 10),
		"@STAGE@", shellQuote(cfg.Staging),
		"@OUT@", shellQuote(out),
		"@STALE@", strconv.Itoa(staleStagingMinutes))
}

// patchChunkScript prints n bytes of the patch file from offset.
func patchChunkScript(cfg guestConfig, file string, offset, n int64) string {
	return script(cfg, `f=@FILE@
[ -f "$f" ] || fail patch_gone
echo "data chunk"
tail -c +@FROM@ "$f" | head -c @N@ | enc
echo "end"
echo ok`,
		"@FILE@", shellQuote(file),
		"@FROM@", strconv.FormatInt(offset+1, 10),
		"@N@", strconv.FormatInt(n, 10))
}
