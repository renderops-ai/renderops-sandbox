package sandboxfiles

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The spawn CLI's guest scripts: Extract (uploads) and the patch, run end to
// end through Client in the same two places as the files API's scripts (see
// guest_test.go).

// serveLocal serves files from an httptest server the local guest reaches.
func serveLocal(t *testing.T, files map[string][]byte) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := files[strings.TrimPrefix(r.URL.Path, "/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if _, err := w.Write(body); err != nil {
			t.Logf("serve %s: %v", r.URL.Path, err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// serveInContainer copies files into the container and serves them there
// with python's http.server (the container cannot reach the host's loopback).
func serveInContainer(container string) func(t *testing.T, files map[string][]byte) string {
	var once sync.Once
	var startErr error
	n := 0
	return func(t *testing.T, files map[string][]byte) string {
		t.Helper()
		n++
		dir := "/srv/" + strconv.Itoa(n)
		for name, body := range files {
			cmd := exec.Command("docker", "exec", "-i", container, "sh", "-c", `mkdir -p "$1" && cat > "$1/$2"`, "sh", dir, name)
			cmd.Stdin = bytes.NewReader(body)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("copy %s into the container: %v\n%s", name, err, out)
			}
		}
		once.Do(func() {
			if out, err := exec.Command("docker", "exec", "-d", container, "python3", "-m", "http.server", "8765",
				"--bind", "127.0.0.1", "--directory", "/srv").CombinedOutput(); err != nil {
				startErr = fmt.Errorf("start http.server: %v\n%s", err, out)
				return
			}
			for range 50 {
				if exec.Command("docker", "exec", container, "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8765/").Run() == nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			startErr = fmt.Errorf("http.server did not start")
		})
		if startErr != nil {
			t.Fatal(startErr)
		}
		return "http://127.0.0.1:8765/" + strconv.Itoa(n)
	}
}

// tarEntry is one entry of a test archive. size overrides the header's size
// (a huge entry is written as a header alone, ending the archive there).
type tarEntry struct {
	name  string
	typ   byte
	body  string
	link  string
	mode  int64
	size  int64
	major int64
}

func tarGz(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	complete := true
	for _, e := range entries {
		header := &tar.Header{
			Name: e.name, Typeflag: e.typ, Linkname: e.link, Mode: e.mode,
			ModTime: time.Unix(1_700_000_000, 0), Devmajor: e.major, Devminor: 3,
		}
		if header.Mode == 0 {
			header.Mode = 0o644
			if e.typ == tar.TypeDir {
				header.Mode = 0o755
			}
		}
		if e.typ == tar.TypeReg {
			header.Size = int64(len(e.body))
		}
		if e.size > 0 {
			header.Size = e.size
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("tar header %q: %v", e.name, err)
		}
		if e.size > 0 {
			complete = false
			break
		}
		if e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("tar body %q: %v", e.name, err)
			}
		}
	}
	if complete {
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return gzipBytes(t, raw.Bytes())
}

func gzipBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	zw := gzip.NewWriter(&out)
	if _, err := zw.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// needsExtractTools skips when the host lacks what Extract needs (the
// sandbox image always has python3 3.12+ and curl).
func needsExtractTools(t *testing.T, h *harness) {
	t.Helper()
	if _, err := h.fixture(`python3 -c 'import tarfile; tarfile.data_filter' && command -v curl >/dev/null`); err != nil {
		t.Skipf("python3 3.12+ (tarfile.data_filter) and curl are required: %v", err)
	}
}

func extractScenario(t *testing.T, h *harness) {
	needsExtractTools(t, h)
	good := tarGz(t,
		tarEntry{name: "./", typ: tar.TypeDir},
		tarEntry{name: "README.md", typ: tar.TypeReg, body: "hello\n"},
		tarEntry{name: "bin/run.sh", typ: tar.TypeReg, body: "#!/bin/sh\necho hi\n", mode: 0o755},
		tarEntry{name: "src/", typ: tar.TypeDir},
		tarEntry{name: "src/main.go", typ: tar.TypeReg, body: "package main\n"},
		tarEntry{name: "src/same.go", typ: tar.TypeLink, link: "src/main.go"},
		tarEntry{name: "docs/readme", typ: tar.TypeSymlink, link: "../README.md"},
		tarEntry{name: "self", typ: tar.TypeSymlink, link: "."},
		tarEntry{name: ".git/HEAD", typ: tar.TypeReg, body: "ref: refs/heads/main\n"},
		tarEntry{name: "setuid", typ: tar.TypeReg, body: "s", mode: 0o4777},
		tarEntry{name: "quote ' \" $(id) *.txt", typ: tar.TypeReg, body: "q"},
		tarEntry{name: "new\nline", typ: tar.TypeReg, body: "n"},
		tarEntry{name: "uni-\u00fc.txt", typ: tar.TypeReg, body: "u"},
	)
	base := h.serve(t, map[string][]byte{"good.tar.gz": good, "other.tar.gz": []byte("other")})
	goodURL := base + "/good.tar.gz"
	c, rec := h.client(t)
	ctx := context.Background()
	request := func(dest string) ExtractRequest {
		return ExtractRequest{URL: goodURL, SHA256: sum(good), Size: int64(len(good)), Dest: dest}
	}
	staging := shellQuote(h.staging)

	dest := h.ws + "/extract/good/repo"
	got, err := c.Extract(ctx, request(dest))
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := int64(len("hello\n#!/bin/sh\necho hi\npackage main\nref: refs/heads/main\nsqnu"))
	if got.Dest != dest || got.Entries != 12 || got.Bytes != wantBytes {
		t.Fatalf("extracted = %+v, want 12 entries and %d bytes", got, wantBytes)
	}
	for _, call := range rec.reset() {
		if call.passive {
			t.Fatal("extract counts as sandbox activity")
		}
	}
	out := h.mustFixture(t, `cd "$WS/extract/good/repo"
mode() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }
inode() { stat -c %i "$1" 2>/dev/null || stat -f %i "$1"; }
cat README.md bin/run.sh src/main.go .git/HEAD
echo "run $(mode bin/run.sh) setuid $(mode setuid) readme $(mode README.md)"
echo "links $(readlink docs/readme) $(readlink self) $(cat docs/readme)"
[ "$(inode src/main.go)" = "$(inode src/same.go)" ] && echo hardlinked
cat "quote ' \" \$(id) *.txt" "$(printf 'new\nline')" uni-*.txt
echo
ls -A "$WS/extract/good"
ls -A `+staging+` | grep -c '^upload-' || true`)
	want := "hello\n#!/bin/sh\necho hi\npackage main\nref: refs/heads/main\n" +
		"run 755 setuid 755 readme 644\n" +
		"links ../README.md . hello\n" +
		"hardlinked\n" +
		"qnu\n" +
		"repo\n" +
		"0\n"
	if out != want {
		t.Fatalf("extracted tree:\n%s\nwant:\n%s", out, want)
	}

	// dest is in use: not empty, a file; an empty directory is fine.
	_, err = c.Extract(ctx, request(dest))
	wantCode(t, err, CodeDestNotEmpty)
	h.mustFixture(t, `mkdir -p "$WS/extract/empty"
printf x > "$WS/extract/file"
ln -s "$(dirname "$WS")/outside" "$WS/extract/escape"`)
	_, err = c.Extract(ctx, request(h.ws+"/extract/file"))
	wantCode(t, err, CodeDestExists)
	if got, err := c.Extract(ctx, request(h.ws+"/extract/empty")); err != nil || got.Entries != 12 {
		t.Fatalf("extract into an empty directory = %+v, %v", got, err)
	}

	// dest must stay in the workspace, outside .git.
	rec.reset()
	for _, bad := range []string{h.ws, h.ws + "/a/../b", "relative", h.ws + "/repo/.git/x", "/etc/x", h.ws + "/"} {
		_, err = c.Extract(ctx, request(bad))
		wantCode(t, err, CodeInvalidDest)
	}
	if calls := rec.reset(); len(calls) != 0 {
		t.Fatalf("%d execs for invalid dests", len(calls))
	}
	_, err = c.Extract(ctx, request(h.ws+"/extract/escape/x"))
	wantCode(t, err, CodeInvalidDest)

	// The download must be what the uploader declared.
	bad := request(h.ws + "/extract/bad-sum")
	bad.SHA256 = sum([]byte("something else"))
	_, err = c.Extract(ctx, bad)
	wantCode(t, err, CodeChecksumMismatch)
	bad = request(h.ws + "/extract/bad-size")
	bad.Size++
	_, err = c.Extract(ctx, bad)
	wantCode(t, err, CodeSizeMismatch)
	bad = request(h.ws + "/extract/missing")
	bad.URL = base + "/missing.tar.gz"
	_, err = c.Extract(ctx, bad)
	wantCode(t, err, CodeDownloadFailed)
	if out := h.mustFixture(t, `ls -A "$WS/extract"; ls -A `+staging+` | grep -c '^upload-' || true`); out != "empty\nescape\nfile\ngood\n0\n" {
		t.Fatalf("failed extracts left files behind: %q", out)
	}

	// Plain http is refused outside tests, before any exec.
	rec.reset()
	c.cfg.AllowHTTP = false
	_, err = c.Extract(ctx, request(h.ws+"/extract/http"))
	wantCode(t, err, CodeGuestFailed)
	c.cfg.AllowHTTP = true
	if calls := rec.reset(); len(calls) != 0 {
		t.Fatal("an http URL reached the guest")
	}
}

func maliciousExtractScenario(t *testing.T, h *harness) {
	needsExtractTools(t, h)
	valid := tarGz(t, tarEntry{name: "a.txt", typ: tar.TypeReg, body: strings.Repeat("a", 4096)})
	cases := []struct {
		name    string
		archive []byte
		code    string
		entry   string
		limits  func(*guestConfig)
	}{
		{name: "dot-dot", archive: tarGz(t, tarEntry{name: "../spawn-evil-1", typ: tar.TypeReg, body: "x"}),
			code: CodeUnsafePath, entry: "../spawn-evil-1"},
		{name: "absolute", archive: tarGz(t, tarEntry{name: "/tmp/spawn-evil-abs", typ: tar.TypeReg, body: "x"}),
			code: CodeUnsafePath, entry: "/tmp/spawn-evil-abs"},
		{name: "inner dot-dot", archive: tarGz(t, tarEntry{name: "ok.txt", typ: tar.TypeReg, body: "x"},
			tarEntry{name: "a/../../spawn-evil-3", typ: tar.TypeReg, body: "x"}), code: CodeUnsafePath},
		{name: "absolute symlink", archive: tarGz(t, tarEntry{name: "l", typ: tar.TypeSymlink, link: "/etc"}),
			code: CodeUnsafeLink, entry: "l"},
		{name: "escaping symlink", archive: tarGz(t, tarEntry{name: "d/l", typ: tar.TypeSymlink, link: "../../outside"}),
			code: CodeUnsafeLink},
		{name: "write through a symlink", archive: tarGz(t,
			tarEntry{name: "sub/", typ: tar.TypeDir},
			tarEntry{name: "l", typ: tar.TypeSymlink, link: "sub"},
			tarEntry{name: "l/spawn-evil-7", typ: tar.TypeReg, body: "x"}), code: CodeUnsafeLink, entry: "l/spawn-evil-7"},
		{name: "symlink before its child", archive: tarGz(t,
			tarEntry{name: "l/spawn-evil-7b", typ: tar.TypeReg, body: "x"},
			tarEntry{name: "l", typ: tar.TypeSymlink, link: "sub"}), code: CodeUnsafeLink, entry: "l/spawn-evil-7b"},
		// Each link alone stays inside; d/up/.. through the d/up link does not.
		{name: "symlink chain", archive: tarGz(t,
			tarEntry{name: "d/", typ: tar.TypeDir},
			tarEntry{name: "d/up", typ: tar.TypeSymlink, link: ".."},
			tarEntry{name: "a", typ: tar.TypeSymlink, link: "d/up/.."}), code: CodeUnsafeLink, entry: "a"},
		{name: "symlink chain, outer link first", archive: tarGz(t,
			tarEntry{name: "a", typ: tar.TypeSymlink, link: "d/up/../x"},
			tarEntry{name: "d/up", typ: tar.TypeSymlink, link: ".."}), code: CodeUnsafeLink, entry: "a"},
		{name: "symlink loop", archive: tarGz(t,
			tarEntry{name: "a", typ: tar.TypeSymlink, link: "b"},
			tarEntry{name: "b", typ: tar.TypeSymlink, link: "a"}), code: CodeUnsafeLink},
		{name: "absolute hard link", archive: tarGz(t, tarEntry{name: "h", typ: tar.TypeLink, link: "/etc/passwd"}),
			code: CodeUnsafeLink, entry: "h"},
		{name: "escaping hard link", archive: tarGz(t, tarEntry{name: "h", typ: tar.TypeLink, link: "../../outside/secret.txt"}),
			code: CodeUnsafeLink},
		{name: "forward hard link", archive: tarGz(t,
			tarEntry{name: "h", typ: tar.TypeLink, link: "later.txt"},
			tarEntry{name: "later.txt", typ: tar.TypeReg, body: "x"}), code: CodeUnsafeLink},
		{name: "hard link to a directory", archive: tarGz(t,
			tarEntry{name: "d/", typ: tar.TypeDir},
			tarEntry{name: "h", typ: tar.TypeLink, link: "d"}), code: CodeUnsafeLink},
		{name: "device", archive: tarGz(t, tarEntry{name: "null", typ: tar.TypeChar, major: 1}),
			code: CodeUnsupportedEntry, entry: "null"},
		{name: "fifo", archive: tarGz(t, tarEntry{name: "pipe", typ: tar.TypeFifo}), code: CodeUnsupportedEntry},
		{name: "duplicate", archive: tarGz(t,
			tarEntry{name: "f.txt", typ: tar.TypeReg, body: "1"},
			tarEntry{name: "./f.txt", typ: tar.TypeReg, body: "2"}), code: CodeDuplicateEntry},
		{name: "file over a symlink", archive: tarGz(t,
			tarEntry{name: "t.txt", typ: tar.TypeReg, body: "t"},
			tarEntry{name: "s", typ: tar.TypeSymlink, link: "t.txt"},
			tarEntry{name: "s", typ: tar.TypeReg, body: "x"}), code: CodeDuplicateEntry},
		{name: "huge entry header", archive: tarGz(t,
			tarEntry{name: "ok.txt", typ: tar.TypeReg, body: "x"},
			tarEntry{name: "huge.bin", typ: tar.TypeReg, size: 8 << 30}), code: CodeEntryTooLarge, entry: "huge.bin"},
		{name: "entry over the cap", archive: valid, code: CodeEntryTooLarge,
			limits: func(cfg *guestConfig) { cfg.ExtractEntryBytes = 4095 }},
		{name: "archive over the cap", archive: tarGz(t,
			tarEntry{name: "a", typ: tar.TypeReg, body: strings.Repeat("a", 2000)},
			tarEntry{name: "b", typ: tar.TypeReg, body: strings.Repeat("b", 2000)}), code: CodeArchiveTooLarge,
			limits: func(cfg *guestConfig) { cfg.ExtractBytes = 3000 }},
		{name: "too many entries", archive: tarGz(t,
			tarEntry{name: "a", typ: tar.TypeReg}, tarEntry{name: "b", typ: tar.TypeReg},
			tarEntry{name: "c", typ: tar.TypeReg}, tarEntry{name: "d", typ: tar.TypeReg}), code: CodeTooManyEntries,
			limits: func(cfg *guestConfig) { cfg.ExtractEntries = 3 }},
		{name: "not gzip", archive: []byte("just text, not an archive"), code: CodeInvalidArchive},
		{name: "truncated", archive: valid[:len(valid)/2], code: CodeInvalidArchive},
		{name: "garbage tar", archive: gzipBytes(t, bytes.Repeat([]byte("garbage!"), 200)), code: CodeInvalidArchive},
		{name: "file then child", archive: tarGz(t,
			tarEntry{name: "f", typ: tar.TypeReg, body: "x"},
			tarEntry{name: "f/child", typ: tar.TypeReg, body: "x"}), code: CodeInvalidArchive},
	}
	files := map[string][]byte{}
	for i, tc := range cases {
		files[fmt.Sprintf("case-%d.tar.gz", i)] = tc.archive
	}
	base := h.serve(t, files)
	before := h.mustFixture(t, `cd "$(dirname "$WS")/outside" && find . | sort`)
	ctx := context.Background()
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := h.client(t)
			if tc.limits != nil {
				tc.limits(&c.cfg)
			}
			dest := fmt.Sprintf("%s/evil/%d", h.ws, i)
			_, err := c.Extract(ctx, ExtractRequest{
				URL: fmt.Sprintf("%s/case-%d.tar.gz", base, i), SHA256: sum(tc.archive),
				Size: int64(len(tc.archive)), Dest: dest,
			})
			fe := wantCode(t, err, tc.code)
			if tc.entry != "" && fe.Entry != tc.entry {
				t.Fatalf("entry = %q, want %q", fe.Entry, tc.entry)
			}
		})
	}
	out := h.mustFixture(t, `find "$WS/evil" -mindepth 1
find "$(dirname "$WS")" /tmp -maxdepth 3 -name 'spawn-evil*' 2>/dev/null
ls -A `+shellQuote(h.staging)+` | grep '^upload-' || true
cd "$(dirname "$WS")/outside" && find . | sort`)
	if out != before {
		t.Fatalf("a refused archive left files behind or changed the outside tree:\n%s\nwant only:\n%s", out, before)
	}
}

func patchScenario(t *testing.T, h *harness) {
	out := h.mustFixture(t, `d=$WS/patch
mkdir -p "$d/sub"
cd "$d"
g init -q
printf 'a\n' > a.txt
printf 'mod\n' > mod.txt
printf 'del\n' > del.txt
printf '\000\001\002' > bin.dat
printf 's\n' > sub/s.txt
printf '1\n2\n3\n4\n5\n6\n7\n8\n9\n' > rename-me.txt
printf 'ignored.log\n' > .gitignore
g add .
g commit -qm base
git rev-parse HEAD
printf 'a2\n' >> a.txt
g commit -qam second
printf 'MOD\n' > mod.txt
rm del.txt
g mv rename-me.txt renamed.txt
printf '\000\003\004\377' > bin.dat
printf 'staged\n' > staged.txt
g add staged.txt
printf 'new\n' > new.txt
printf '\000\377\376' > new.bin
printf 'u\n' > 'sub/sp ace*.txt'
printf 'S\n' >> sub/s.txt
printf 'x\n' > ignored.log
mkdir nested
g -C nested init -q
printf n > nested/n.txt
git rev-parse HEAD
mkdir -p "$WS/patch-clean"
cd "$WS/patch-clean"
g init -q
printf c > c.txt
g add .
g commit -qm clean
git rev-parse HEAD`)
	revs := strings.Fields(out)
	if len(revs) != 3 {
		t.Fatalf("fixture = %q", out)
	}
	base, head, clean := revs[0], revs[1], revs[2]
	// The index's hash first: status must not refresh (write) it either.
	state := `cd "$WS/patch" && (sha256sum .git/index 2>/dev/null || shasum -a 256 .git/index) && GIT_OPTIONAL_LOCKS=0 git status --porcelain`
	before := h.mustFixture(t, state)
	c, rec := h.client(t)
	ctx := context.Background()

	read := func(c *Client, root, base string) (*Patch, []byte) {
		t.Helper()
		p, err := c.PreparePatch(ctx, root, base)
		if err != nil {
			t.Fatalf("prepare patch of %s: %v", root, err)
		}
		var body bytes.Buffer
		if err := c.WritePatch(ctx, p, &body); err != nil {
			t.Fatalf("read patch: %v", err)
		}
		c.RemovePatch(ctx, p)
		if int64(body.Len()) != p.Size || sum(body.Bytes()) != p.SHA256 {
			t.Fatalf("patch body %d bytes, sha %s; prepared %+v", body.Len(), sum(body.Bytes()), p)
		}
		return p, body.Bytes()
	}

	patch, body := read(c, h.ws+"/patch", base)
	if patch.Base != base || patch.Head != head {
		t.Fatalf("patch = %+v", patch)
	}
	for _, call := range rec.reset() {
		if !call.passive {
			t.Fatal("patches must not count as sandbox activity")
		}
	}
	text := string(body)
	for _, want := range []string{"diff --git a/new.txt b/new.txt\nnew file mode", "GIT binary patch", "rename from rename-me.txt",
		"diff --git a/staged.txt", "b/sub/sp ace*.txt", "deleted file mode"} {
		if !strings.Contains(text, want) {
			t.Errorf("patch lacks %q:\n%s", want, text)
		}
	}
	for _, unwanted := range []string{"ignored.log", "nested"} {
		if strings.Contains(text, unwanted) {
			t.Errorf("patch includes %s", unwanted)
		}
	}
	if after := h.mustFixture(t, state); after != before {
		t.Fatalf("the repository's index changed:\n%s\nwant:\n%s", after, before)
	}

	// Applied to a clone at base, the patch reproduces the work tree.
	listing := `list() { find . -path ./.git -prune -o -path ./nested -prune -o -name ignored.log -prune -o -type f -print | sort | while IFS= read -r f; do
	printf '%s %s\n' "$f" "$(if command -v sha256sum >/dev/null; then sha256sum < "$f"; else shasum -a 256 < "$f"; fi | cut -d ' ' -f 1)"
done; }
`
	compare := h.mustFixture(t, listing+`cd "$WS"
rm -rf patch-copy
git clone -q patch patch-copy
cd patch-copy
git -c advice.detachedHead=false checkout -q `+base+`
printf %s '`+base64.StdEncoding.EncodeToString(body)+`' | base64 -d > ../patch.diff
git apply --binary ../patch.diff
list > ../copy.list
cd ../patch
list > ../work.list
cmp ../copy.list ../work.list && echo same
cat ../work.list`)
	if !strings.HasPrefix(compare, "same\n") || !strings.Contains(compare, "./new.bin ") || strings.Contains(compare, "./del.txt ") {
		t.Fatalf("the applied patch does not reproduce the work tree:\n%s", compare)
	}

	// Chunked reads give the same bytes.
	small, _ := h.client(t)
	small.cfg.PatchChunk = 100
	if _, chunked := read(small, h.ws+"/patch", base); !bytes.Equal(chunked, body) {
		t.Fatal("a chunked read differs")
	}

	// A subdirectory root: its changes only, relative to it.
	_, sub := read(c, h.ws+"/patch/sub", base)
	if s := string(sub); !strings.Contains(s, "diff --git a/s.txt b/s.txt") || !strings.Contains(s, "b/sp ace*.txt") || strings.Contains(s, "mod.txt") {
		t.Fatalf("subdirectory patch:\n%s", s)
	}

	// No changes: an empty patch, read with no further exec.
	rec.reset()
	empty, emptyBody := read(c, h.ws+"/patch-clean", clean)
	if empty.Size != 0 || len(emptyBody) != 0 || empty.SHA256 != sum(nil) {
		t.Fatalf("empty patch = %+v", empty)
	}
	if calls := rec.reset(); len(calls) != 2 { // prepare and remove
		t.Fatalf("%d execs for an empty patch", len(calls))
	}

	// Over the cap: refused with its size, nothing left in staging.
	capped, _ := h.client(t)
	capped.cfg.PatchMax = 64
	_, err := capped.PreparePatch(ctx, h.ws+"/patch", base)
	if fe := wantCode(t, err, CodePatchTooLarge); fe.Size <= 64 {
		t.Fatalf("too large size = %d", fe.Size)
	}
	if out := h.mustFixture(t, `ls -A `+shellQuote(h.staging)+` | grep '^patch-' || true`); out != "" {
		t.Fatalf("patch staging files left behind: %q", out)
	}

	_, err = c.PreparePatch(ctx, h.ws+"/patch", strings.Repeat("0", 40))
	wantCode(t, err, CodeRevNotFound)
	_, err = c.PreparePatch(ctx, h.ws+"/read", base)
	wantCode(t, err, CodeNotAGitRepo)
	_, err = c.PreparePatch(ctx, h.ws+"/patch", "HEAD")
	wantCode(t, err, CodeInvalidRev)
	_, err = c.PreparePatch(ctx, "/etc", base)
	wantCode(t, err, CodeInvalidPath)
}
