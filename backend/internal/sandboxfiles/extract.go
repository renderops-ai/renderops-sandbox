package sandboxfiles

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Extract downloads a .tar.gz (an upload, through a short-lived read URL)
// into the sandbox and unpacks it at a new directory in the workspace. One
// guest script does it all:
//
//  1. checks dest (it must not exist, or be an empty directory) before
//     downloading anything;
//  2. downloads with curl (busybox wget as a fallback) to the staging
//     directory and checks the size and sha256 the uploader declared;
//  3. unpacks with Python's tarfile (extractPy) into a fresh directory next to
//     dest, after a full validation pass over the archive: no absolute paths
//     or "..", no entry below a symlink, every symlink resolving inside dest
//     when followed through the archive's other symlinks, hard links only to
//     regular files earlier in the archive, no devices, fifos or sparse
//     files, no duplicate names, and entry, size and count caps. tarfile's
//     own "data" filter runs while unpacking, and every symlink is checked
//     with realpath afterwards, as second and third layers;
//  4. renames that directory to dest (rename(2): atomic, and it refuses a
//     dest that is no longer empty), so dest is either the whole archive or
//     untouched. The download and any partial unpack are removed however the
//     script ends.
//
// The URL, dest and the Python program reach the guest as base64 literals,
// never as shell text. Python (3.12+, for tarfile's data filter) is part of
// every sandbox image; busybox tar has no way to refuse unsafe entries.
const (
	// MaxExtractEntries, MaxExtractBytes and MaxExtractEntryBytes cap an
	// archive: entries, total unpacked bytes and one file's bytes.
	MaxExtractEntries    = 200_000
	MaxExtractBytes      = 4 << 30
	MaxExtractEntryBytes = 2 << 30
	// MaxExtractURLBytes bounds the download URL (a blob URL with a SAS).
	MaxExtractURLBytes = 8 << 10

	// maxEntryNameBytes cuts the refused entry's name in errors (extractPy
	// cuts it to the same length).
	maxEntryNameBytes = 512
	// extractMemoryKB is the unpacker's address-space limit (ulimit -v): a
	// hostile archive cannot make it buffer a huge header.
	extractMemoryKB = 512 << 10

	downloadTimeoutS = 600
	extractTimeoutS  = 900
)

// Extract error codes (see errorMessages).
const (
	CodeInvalidDest        = "invalid_dest"
	CodeDestExists         = "dest_exists"
	CodeDestNotEmpty       = "dest_not_empty"
	CodeDownloadFailed     = "download_failed"
	CodeSizeMismatch       = "size_mismatch"
	CodeChecksumMismatch   = "checksum_mismatch"
	CodeInvalidArchive     = "invalid_archive"
	CodeUnsafePath         = "unsafe_path"
	CodeUnsafeLink         = "unsafe_link"
	CodeUnsupportedEntry   = "unsupported_entry"
	CodeDuplicateEntry     = "duplicate_entry"
	CodeEntryTooLarge      = "entry_too_large"
	CodeArchiveTooLarge    = "archive_too_large"
	CodeTooManyEntries     = "too_many_entries"
	CodeNoSpace            = "no_space"
	CodeExtractUnsupported = "extract_unsupported"
)

// ArchiveRejected reports whether code means the archive itself is unusable
// (as opposed to dest, the download or the guest): extracting it again would
// fail the same way.
func ArchiveRejected(code string) bool {
	switch code {
	case CodeSizeMismatch, CodeChecksumMismatch, CodeInvalidArchive, CodeUnsafePath, CodeUnsafeLink,
		CodeUnsupportedEntry, CodeDuplicateEntry, CodeEntryTooLarge, CodeArchiveTooLarge, CodeTooManyEntries:
		return true
	}
	return false
}

// ExtractRequest is one archive to unpack.
type ExtractRequest struct {
	// URL is where the guest downloads the .tar.gz (https).
	URL string
	// SHA256 and Size are what the uploader declared.
	SHA256 string
	Size   int64
	// Dest is the directory to create, below the workspace.
	Dest string
}

// Extracted is the result of an extract.
type Extracted struct {
	Dest    string `json:"dest"`
	Entries int64  `json:"entries"`
	Bytes   int64  `json:"bytes"`
}

// CheckDest validates an extract's dest like a write path: absolute, clean,
// below the workspace (not the workspace itself) and outside any .git
// directory. Extract checks it too; callers check first to refuse a bad dest
// before any other work.
func (c *Client) CheckDest(dest string) error {
	if err := c.checkWritePath(dest); err != nil {
		return newError(CodeInvalidDest)
	}
	return nil
}

func (c *Client) checkURL(raw string) error {
	parsed, err := url.Parse(raw)
	scheme := "https"
	if err == nil && c.cfg.AllowHTTP && parsed.Scheme == "http" {
		scheme = "http"
	}
	if err != nil || len(raw) > MaxExtractURLBytes || parsed.Scheme != scheme || parsed.Host == "" ||
		parsed.User != nil || strings.ContainsAny(raw, "\x00\r\n") {
		return &Error{Code: CodeGuestFailed, Message: "the upload's download URL is invalid"}
	}
	return nil
}

// Extract downloads and unpacks req (see the package comment above). It
// counts as sandbox activity.
func (c *Client) Extract(ctx context.Context, req ExtractRequest) (Extracted, error) {
	if err := c.CheckDest(req.Dest); err != nil {
		return Extracted{}, err
	}
	if err := c.checkURL(req.URL); err != nil {
		return Extracted{}, err
	}
	sum := strings.ToLower(req.SHA256)
	if !ValidSHA256(sum) {
		return Extracted{}, newError(CodeInvalidSHA256)
	}
	if req.Size <= 0 {
		return Extracted{}, newError(CodeSizeMismatch)
	}
	download := c.cfg.Staging + "/upload-" + newStagingID() + ".tar.gz"
	out, err := c.run(ctx, "extract", extractScript(c.cfg, req.URL, req.Dest, sum, req.Size, download), extractTimeoutS, false)
	if err != nil {
		return Extracted{}, err
	}
	entries, _ := strconv.ParseInt(out.fields["entries"], 10, 64)
	bytes, _ := strconv.ParseInt(out.fields["bytes"], 10, 64)
	return Extracted{Dest: req.Dest, Entries: entries, Bytes: bytes}, nil
}

// extractScript: see Extract. Python prints its own "error <code>" line and
// exits 3; any other failure of it is extract_failed (reported as
// files_failed, with its stderr logged).
func extractScript(cfg guestConfig, rawURL, dest, sum string, size int64, download string) string {
	proto := "=https"
	if cfg.AllowHTTP {
		proto = "=http,https"
	}
	return script(cfg, `umask 022
u=$(dec @URL@) || fail extract_failed
u=${u%x}
d=$(dec @DEST@) || fail invalid_dest
d=${d%x}
py=$(dec @PY@) || fail extract_failed
py=${py%x}
want=@SHA@
size=@SIZE@
st=@STAGE@
f=@FILE@
q=
cleanup() {
	rm -f "$f"
	[ -z "$q" ] || rm -rf "$q"
}
trap cleanup EXIT
resolve "$d" || fail invalid_dest
inside "$R" || fail invalid_dest
[ "$R" != "$ws" ] || fail invalid_dest
case "$R/" in */.git/*) fail invalid_dest ;; esac
dest=$R
if [ -e "$dest" ] || [ -L "$dest" ]; then
	[ -d "$dest" ] || fail dest_exists
	[ -z "$(ls -A "$dest")" ] || fail dest_not_empty
fi
parent=${dest%/*}
[ -n "$parent" ] || parent=/
mkdir -p "$parent" || fail extract_failed
python3 -c 'import tarfile; tarfile.data_filter' >/dev/null 2>&1 || fail extract_unsupported
mkdir -p "$st" || fail stage_failed
find "$st" -type f -mmin +@STALE@ -exec rm -f {} + 2>/dev/null
find "$parent" -maxdepth 1 -type d -name '.spawn-extract.*' -mmin +@STALE@ -exec rm -rf {} + 2>/dev/null
if command -v curl >/dev/null 2>&1; then
	curl -fsS --proto @PROTO@ --max-filesize "$size" --connect-timeout 20 --max-time @DLTIME@ --retry 2 -o "$f" "$u" || fail download_failed
elif command -v wget >/dev/null 2>&1; then
	wget -q -T 20 -O "$f" "$u" || fail download_failed
else
	fail download_failed
fi
[ "$(($(wc -c < "$f")))" = "$size" ] || fail size_mismatch
[ "$(sha < "$f")" = "$want" ] || fail checksum_mismatch
q=$(mktemp -d "$parent/.spawn-extract.XXXXXX") || { q=; fail extract_failed; }
(ulimit -v @MEM@ 2>/dev/null; exec python3 -I -S -c "$py" "$f" "$q" "$dest" @ENTRIES@ @BYTES@ @ENTRY@)
rc=$?
[ "$rc" = 0 ] || { [ "$rc" = 3 ] && exit 3; fail extract_failed; }
q=
echo ok`,
		"@URL@", b64([]byte(rawURL)),
		"@DEST@", b64([]byte(dest)),
		"@PY@", b64([]byte(extractPy)),
		"@SHA@", shellQuote(sum),
		"@SIZE@", strconv.FormatInt(size, 10),
		"@STAGE@", shellQuote(cfg.Staging),
		"@FILE@", shellQuote(download),
		"@STALE@", strconv.Itoa(staleStagingMinutes),
		"@PROTO@", shellQuote(proto),
		"@DLTIME@", strconv.Itoa(downloadTimeoutS),
		"@MEM@", strconv.Itoa(extractMemoryKB),
		"@ENTRIES@", strconv.FormatInt(cfg.ExtractEntries, 10),
		"@BYTES@", strconv.FormatInt(cfg.ExtractBytes, 10),
		"@ENTRY@", strconv.FormatInt(cfg.ExtractEntryBytes, 10))
}

// newStagingID names one staging file (server-chosen, so safe in scripts).
func newStagingID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

// extractPy validates and unpacks an archive: argv is the archive, the empty
// directory to unpack into, the final dest, and the entry, total-bytes and
// per-file caps. It reads the archive twice:
//
//  1. scan validates every entry and the archive as a whole (which entries
//     sit below a symlink, where each symlink leads through the others)
//     before anything is written;
//  2. unpack extracts it with tarfile's "data" filter as a second layer,
//     then every symlink is followed with realpath as a third.
//
// It prints "entries N" and "bytes N" and renames the directory to dest; on
// a refusal it prints "entry <base64 name>" (when an entry is to blame) and
// "error <code>", and exits 3.
const extractPy = `import base64, errno, os, sys, tarfile, zlib

MAX_LINK_HOPS = 40


def say(*words):
    sys.stdout.write(" ".join(words) + "\n")
    sys.stdout.flush()


def fail(code, name=None):
    if name is not None:
        raw = name.encode("utf-8", "surrogateescape")[:512]
        say("entry", base64.b64encode(raw).decode("ascii"))
    say("error", code)
    sys.exit(3)


def components(name):
    """The clean components of a relative name, or None when it is unsafe."""
    if not name or name.startswith("/") or "\x00" in name:
        return None
    parts = [p for p in name.split("/") if p not in ("", ".")]
    if ".." in parts:
        return None
    if len(name.encode("utf-8", "surrogateescape")) > 4096:
        return None
    if any(len(p.encode("utf-8", "surrogateescape")) > 255 for p in parts):
        return None
    return parts


def stays_inside(rel, links):
    """Whether the symlink rel, followed through the archive's other
    symlinks, resolves inside the root. Works on names only: the root is
    empty before unpacking, so the archive's symlinks are the only ones."""
    resolved = rel.split("/")[:-1]
    pending = list(reversed(links[rel].split("/")))
    hops = 0
    while pending:
        part = pending.pop()
        if part in ("", "."):
            continue
        if part == "..":
            if not resolved:
                return False
            resolved.pop()
            continue
        resolved.append(part)
        target = links.get("/".join(resolved))
        if target is not None:
            hops += 1
            if hops > MAX_LINK_HOPS:
                return False
            resolved.pop()
            pending.extend(reversed(target.split("/")))
    return True


def scan(src, max_entries, max_bytes, max_entry):
    """Pass 1: every entry's name, type and size, hard links, then the
    entries below symlinks and where each symlink leads."""
    kinds = {}
    links = {}
    entries = 0
    total = 0
    with tarfile.open(src, mode="r|gz") as tar:
        for member in tar:
            entries += 1
            if entries > max_entries:
                fail("too_many_entries")
            name = member.name
            parts = components(name)
            if parts is None:
                fail("unsafe_path", name)
            if not parts:
                # The archive root ("./") itself.
                if member.isdir():
                    continue
                fail("unsafe_path", name)
            rel = "/".join(parts)
            if rel in kinds:
                fail("duplicate_entry", name)
            if member.issparse():
                fail("unsupported_entry", name)
            if member.isreg():
                if member.size > max_entry:
                    fail("entry_too_large", name)
                total += member.size
                if total > max_bytes:
                    fail("archive_too_large")
                kind = "file"
            elif member.isdir():
                kind = "dir"
            elif member.issym():
                target = member.linkname
                if not target or target.startswith("/") or "\x00" in target or len(target) > 4096:
                    fail("unsafe_link", name)
                links[rel] = target
                kind = "symlink"
            elif member.islnk():
                target = components(member.linkname)
                # Only to a regular file earlier in the archive (tar's order).
                if not target or kinds.get("/".join(target)) != "file":
                    fail("unsafe_link", name)
                kind = "hardlink"
            else:
                fail("unsupported_entry", name)
            kinds[rel] = kind
    for rel in kinds:
        parts = rel.split("/")
        for i in range(1, len(parts)):
            if "/".join(parts[:i]) in links:
                # Unpacking it would write through the symlink.
                fail("unsafe_link", rel)
    for rel in links:
        if not stays_inside(rel, links):
            fail("unsafe_link", rel)
    return kinds, total


def unpack(src, root, kinds):
    """Pass 2: extract the entries scan accepted."""
    with tarfile.open(src, mode="r|gz") as tar:
        for member in tar:
            parts = components(member.name)
            if not parts:
                continue
            rel = "/".join(parts)
            if rel not in kinds:
                fail("invalid_archive")
            member.name = rel
            if member.islnk():
                member.linkname = "/".join(components(member.linkname))
            tar.extract(member, root, set_attrs=True, filter="data")


def check_links(root):
    """Pass 3: every symlink, followed through any chain, stays inside."""
    real_root = os.path.realpath(root)
    for directory, dirs, files in os.walk(root):
        for name in dirs + files:
            path = os.path.join(directory, name)
            if os.path.islink(path):
                real = os.path.realpath(path)
                if real != real_root and not real.startswith(real_root + os.sep):
                    fail("unsafe_link", os.path.relpath(path, root))


def main():
    src, root, dest = sys.argv[1], sys.argv[2], sys.argv[3]
    max_entries, max_bytes, max_entry = (int(v) for v in sys.argv[4:7])
    if not hasattr(tarfile, "data_filter"):
        fail("extract_unsupported")
    try:
        kinds, total = scan(src, max_entries, max_bytes, max_entry)
        unpack(src, root, kinds)
        check_links(root)
        os.chmod(root, 0o755)
    except tarfile.FilterError as e:
        name = e.tarinfo.name if e.tarinfo is not None else None
        if isinstance(e, (tarfile.AbsoluteLinkError, tarfile.LinkOutsideDestinationError)):
            fail("unsafe_link", name)
        if isinstance(e, tarfile.SpecialFileError):
            fail("unsupported_entry", name)
        fail("unsafe_path", name)
    except (tarfile.TarError, EOFError, zlib.error, MemoryError, UnicodeError):
        fail("invalid_archive")
    except OSError as e:
        if e.errno in (errno.ENOSPC, errno.EDQUOT):
            fail("no_space")
        if e.errno in (errno.ENOTDIR, errno.EEXIST, errno.EISDIR, errno.ENOTEMPTY, errno.ENAMETOOLONG, errno.ELOOP):
            fail("invalid_archive")
        raise
    try:
        os.rename(root, dest)
    except OSError as e:
        if e.errno in (errno.ENOTEMPTY, errno.EEXIST):
            fail("dest_not_empty")
        if e.errno == errno.ENOTDIR:
            fail("dest_exists")
        raise
    say("entries", str(len(kinds)))
    say("bytes", str(total))


main()
`
