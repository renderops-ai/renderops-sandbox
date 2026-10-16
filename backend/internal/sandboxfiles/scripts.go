package sandboxfiles

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// Every guest command is `printf %s <base64 script> | base64 -d | sh`, so
// quoting and `exit` never reach the guest's persistent shell, and each script
// body runs as a function with stdin from /dev/null. User-supplied paths and
// content never become shell text: they reach the script as base64 literals
// and are decoded into variables inside it. Values placed in scripts as text
// are server-chosen (directories, a staging ID) or validated hex (shas, revs),
// and are single-quoted anyway.
//
// Scripts answer in lines: "spawn-files 1" first, then "key value" lines,
// base64 data blocks ("data <name>", 76-column lines, "end"; the guest shell
// reads output line by line with a 64 KB limit), and finally "ok" or
// "error <code>".

// shellCommand wraps a script for the guest's persistent shell.
func shellCommand(script string) string {
	return "printf %s " + base64.StdEncoding.EncodeToString([]byte(script)) + " | base64 -d | sh"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func b64(value []byte) string {
	return shellQuote(base64.StdEncoding.EncodeToString(value))
}

// script puts the common prelude before body, fills the placeholders (one
// pass: a value is never scanned for placeholders) and wraps the result as a
// function run with stdin from /dev/null.
func script(cfg guestConfig, body string, values ...string) string {
	values = append(values, "@WS@", shellQuote(cfg.Workspace))
	return "main() {\n" + strings.NewReplacer(values...).Replace(prelude+body) + "\n}\nmain </dev/null\n"
}

// prelude is shared by every script:
//   - git variables the persistent shell may have exported are dropped, and
//     optional locks are off so reads never touch the agent's index;
//   - fail prints the error code and stops;
//   - dec prints a base64 value followed by "x", so command substitution keeps
//     trailing newlines (the caller strips the x);
//   - enc prints stdin as base64 in 76-column lines;
//   - sha prints the sha256 of stdin;
//   - resolve sets R to $1 with symlinks resolved, like `realpath -m`: the
//     longest existing prefix goes through realpath and the missing rest is
//     appended (paths are already clean, so the rest has no "..");
//   - inside tells whether $1 is the workspace or below it;
//   - cap keeps the first $1 NUL-terminated records (swapping NUL and newline
//     lets head count records whatever the file names contain).
const prelude = `unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_COMMON_DIR GIT_NAMESPACE GIT_CEILING_DIRECTORIES GIT_CONFIG GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT GIT_EXTERNAL_DIFF GIT_DIFF_OPTS
export GIT_OPTIONAL_LOCKS=0 LC_ALL=C
ws=@WS@
echo "spawn-files 1"
fail() { echo "error $1"; exit 3; }
dec() { printf %s "$1" | base64 -d && printf x; }
enc() { base64 | tr -d '\n' | fold -w 76; echo; }
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi | cut -d ' ' -f 1; }
resolve() {
	pre=$1
	rest=
	while [ ! -e "$pre" ] && [ ! -L "$pre" ]; do
		rest=/${pre##*/}$rest
		pre=${pre%/*}
		[ -n "$pre" ] || pre=/
	done
	R=$(realpath "$pre" 2>/dev/null && printf x) || return 1
	R=${R%x}
	R=${R%?}
	[ "$R" != / ] || R=
	R=$R$rest
	[ -n "$R" ] || R=/
}
inside() { case $1 in "$ws" | "$ws"/*) return 0 ;; esac; return 1; }
cap() { tr '\n\000' '\000\n' | head -n "$1" | tr '\n\000' '\000\n'; }
`

// treeScript lists the files under root: tracked, then untracked-not-ignored
// in a git work tree (so a cut keeps tracked files), otherwise everything but
// well-known heavy directories. git ls-files -c also lists tracked files
// deleted from the work tree; their list comes separately so the caller can
// drop them, and the main list is capped high enough that it still holds
// max+1 files after dropping them.
func treeScript(cfg guestConfig, root string) string {
	return script(cfg, `r=$(dec @ROOT@) || fail invalid_path
r=${r%x}
max=@MAX@
resolve "$r" || fail invalid_path
inside "$R" || fail invalid_path
[ -e "$R" ] || fail not_found
[ -d "$R" ] || fail not_a_directory
cd "$R" || fail not_found
if [ "$(git rev-parse --is-inside-work-tree 2>/dev/null)" = true ]; then
	echo "git 1"
	deleted=$(git ls-files -d -z 2>/dev/null | cap "$max" | tr -cd '\000' | wc -c)
	echo "data deleted"
	git ls-files -d -z 2>/dev/null | cap "$max" | enc
	echo "end"
	echo "data files"
	{ git ls-files -c -z 2>/dev/null; git ls-files -o --exclude-standard -z 2>/dev/null; } | cap "$((max + 1 + deleted))" | enc
	echo "end"
else
	echo "git 0"
	echo "data files"
	find . \( -type d \( -name .git -o -name node_modules -o -name .venv -o -name __pycache__ -o -name build -o -name dist \) -prune \) -o \( ! -type d -print0 \) 2>/dev/null | cap "$((max + 1))" | enc
	echo "end"
fi
echo ok`,
		"@ROOT@", b64([]byte(root)),
		"@MAX@", strconv.Itoa(cfg.TreeMax))
}

// readScript prints a file's mode and content (at most MaxFileBytes+1 bytes),
// from the work tree or, with rev, from that commit of the repository that
// contains the path. A file over the limit reports its size and fails.
func readScript(cfg guestConfig, path, rev string) string {
	return script(cfg, `p=$(dec @PATH@) || fail invalid_path
p=${p%x}
rev=@REV@
max=@MAX@
resolve "$p" || fail invalid_path
inside "$R" || fail invalid_path
[ "$R" != "$ws" ] || fail not_a_file
if [ -z "$rev" ]; then
	[ -e "$R" ] || fail file_not_found
	[ -f "$R" ] || fail not_a_file
	size=$(wc -c < "$R") || fail read_failed
	echo "size $((size))"
	if [ -x "$R" ]; then echo "mode 100755"; else echo "mode 100644"; fi
	[ "$((size))" -le "$max" ] || fail file_too_large
	echo "data content"
	head -c "$((max + 1))" < "$R" | enc
	echo "end"
	echo ok
	exit 0
fi
d=$R
while [ ! -d "$d" ]; do
	d=${d%/*}
	[ -n "$d" ] || d=/
done
cd "$d" || fail read_failed
top=$(git rev-parse --show-toplevel 2>/dev/null && printf x) || fail not_a_git_repo
top=${top%x}
top=${top%?}
inside "$top" || fail not_a_git_repo
case $R in "$top"/*) rel=${R#"$top"/} ;; *) fail not_a_file ;; esac
cd "$top" || fail read_failed
git cat-file -e "$rev^{commit}" 2>/dev/null || fail rev_not_found
t=$(git cat-file -t "$rev:$rel" 2>/dev/null) || fail not_in_revision
[ "$t" = blob ] || fail not_a_file
size=$(git cat-file -s "$rev:$rel") || fail read_failed
echo "size $size"
echo "mode $(git --literal-pathspecs ls-tree -z "$rev" -- "$rel" 2>/dev/null | tr '\000' '\n' | head -n 1 | cut -d ' ' -f 1)"
[ "$size" -le "$max" ] || fail file_too_large
echo "data content"
git cat-file blob "$rev:$rel" | head -c "$((max + 1))" | enc
echo "end"
echo ok`,
		"@PATH@", b64([]byte(path)),
		"@REV@", shellQuote(rev),
		"@MAX@", strconv.Itoa(MaxFileBytes))
}

// Write modes: check only runs the checks before a staged upload; inline
// carries the content in the script; staged takes it from the staging file.
const (
	writeCheck  = "check"
	writeInline = "inline"
	writeStaged = "staged"
)

// writeScript replaces (or creates) a file atomically: the content goes to a
// temporary file in the same directory, is verified against want (its sha256),
// gets the existing file's mode (and owner, best effort), and is renamed over
// the file. expected is the sha256 the editor loaded ("" = the file must not
// exist yet). It is compared once before writing and again right before the
// rename; the window between that check and the rename is a few
// microseconds, and nothing in the guest locks files against the agent. A new
// file is linked into place (ln fails if the name exists), so a create never
// replaces a file that appeared meanwhile. Staged uploads remove the staging
// file however the write ends.
func writeScript(cfg guestConfig, path, expected, want, mode, inline, staging string) string {
	return script(cfg, `p=$(dec @PATH@) || fail invalid_path
p=${p%x}
exp=@EXPECTED@
want=@WANT@
mode=@MODE@
stage=@STAGE@
tmp=
cleanup() {
	[ -z "$tmp" ] || rm -f "$tmp"
	[ -z "$stage" ] || rm -f "$stage"
}
trap cleanup EXIT
resolve "$p" || fail invalid_path
inside "$R" || fail invalid_path
[ "$R" != "$ws" ] || fail not_a_file
case "$R/" in */.git/*) fail invalid_path ;; esac
dir=${R%/*}
[ -n "$dir" ] || dir=/
[ -d "$dir" ] || fail parent_not_found
current() {
	cur=
	if [ -e "$R" ] || [ -L "$R" ]; then
		[ -f "$R" ] || fail not_a_file
		cur=$(sha < "$R")
		[ -n "$cur" ] || fail read_failed
	fi
}
compare() {
	if [ -z "$exp" ]; then
		[ -z "$cur" ] || fail exists
	elif [ "$cur" != "$exp" ]; then
		echo "sha256 $cur"
		fail conflict
	fi
}
current
compare
if [ "$mode" = check ]; then
	echo ok
	exit 0
fi
tmp=$(mktemp "$dir/.spawn-write.XXXXXX") || { tmp=; fail write_failed; }
if [ "$mode" = inline ]; then
	printf %s @DATA@ | base64 -d > "$tmp" || fail write_failed
else
	[ -f "$stage" ] || fail write_failed
	base64 -d < "$stage" > "$tmp" || fail write_failed
fi
[ "$(sha < "$tmp")" = "$want" ] || fail write_corrupt
if [ -n "$cur" ]; then
	m=$(stat -c %a "$R" 2>/dev/null || stat -f %Lp "$R" 2>/dev/null)
	[ -n "$m" ] || fail write_failed
	chmod "$m" "$tmp" || fail write_failed
	# Keeping the owner is best effort: everything in the guest runs as root.
	o=$(stat -c %u:%g "$R" 2>/dev/null || stat -f %u:%g "$R" 2>/dev/null) && chown "$o" "$tmp" 2>/dev/null
else
	chmod 644 "$tmp" || fail write_failed
fi
current
compare
if [ -z "$exp" ]; then
	if ! ln "$tmp" "$R" 2>/dev/null; then
		if [ -e "$R" ] || [ -L "$R" ]; then fail exists; fi
		fail write_failed
	fi
else
	mv -f "$tmp" "$R" || fail write_failed
	tmp=
fi
echo ok`,
		"@PATH@", b64([]byte(path)),
		"@EXPECTED@", shellQuote(expected),
		"@WANT@", shellQuote(want),
		"@MODE@", shellQuote(mode),
		"@STAGE@", shellQuote(staging),
		"@DATA@", shellQuote(inline))
}

// chunkScript appends one chunk of a staged upload's base64 text to its
// staging file. The first chunk creates the file and removes staging files
// that failed writes left behind.
func chunkScript(cfg guestConfig, staging, chunk string, first bool) string {
	firstFlag := "0"
	if first {
		firstFlag = "1"
	}
	return script(cfg, `umask 077
d=@DIR@
f=@FILE@
mkdir -p "$d" || fail stage_failed
if [ @FIRST@ = 1 ]; then
	find "$d" -type f -mmin +@STALE@ -exec rm -f {} + 2>/dev/null
	: > "$f" || fail stage_failed
else
	[ -f "$f" ] || fail stage_failed
fi
printf %s @DATA@ >> "$f" || fail stage_failed
echo ok`,
		"@DIR@", shellQuote(cfg.Staging),
		"@FILE@", shellQuote(staging),
		"@FIRST@", firstFlag,
		"@STALE@", strconv.Itoa(staleStagingMinutes),
		"@DATA@", shellQuote(chunk))
}

// removeScript deletes a staging file (cleanup after a failed upload).
func removeScript(cfg guestConfig, staging string) string {
	return script(cfg, `rm -f @FILE@
echo ok`, "@FILE@", shellQuote(staging))
}

// changesScript lists what changed under root against base: the work tree
// (uncommitted edits included) diffed with base, as name-status and numstat,
// plus untracked files. Paths are relative to root.
func changesScript(cfg guestConfig, root, base string) string {
	return script(cfg, `r=$(dec @ROOT@) || fail invalid_path
r=${r%x}
base=@BASE@
lim=@LIMIT@
max=@MAX@
resolve "$r" || fail invalid_path
inside "$R" || fail invalid_path
[ -e "$R" ] || fail not_found
[ -d "$R" ] || fail not_a_directory
cd "$R" || fail not_found
[ "$(git rev-parse --is-inside-work-tree 2>/dev/null)" = true ] || fail not_a_git_repo
git cat-file -e "$base^{commit}" 2>/dev/null || fail rev_not_found
echo "head $(git rev-parse -q --verify HEAD 2>/dev/null)"
echo "data status"
git diff --name-status -z --find-renames --relative --no-ext-diff "$base" -- 2>/dev/null | head -c "$((lim + 1))" | enc
echo "end"
echo "data numstat"
git diff --numstat -z --find-renames --relative --no-ext-diff --no-textconv "$base" -- 2>/dev/null | head -c "$((lim + 1))" | enc
echo "end"
echo "data untracked"
git ls-files -o --exclude-standard -z 2>/dev/null | cap "$((max + 1))" | enc
echo "end"
echo ok`,
		"@ROOT@", b64([]byte(root)),
		"@BASE@", shellQuote(base),
		"@LIMIT@", strconv.Itoa(maxListBytes),
		"@MAX@", strconv.Itoa(cfg.ChangesMax))
}
