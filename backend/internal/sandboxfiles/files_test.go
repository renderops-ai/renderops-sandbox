package sandboxfiles

import (
	"context"
	"encoding/base64"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"backend/internal/plane"
)

// noExecClient returns a client that fails the test if it reaches the guest.
func noExecClient(t *testing.T) *Client {
	t.Helper()
	exec := func(context.Context, string, int, plane.ExecOptions) (plane.ExecResult, error) {
		t.Fatal("the request must be refused before it reaches the sandbox")
		return plane.ExecResult{}, nil
	}
	return &Client{exec: exec, cfg: defaultConfig(), log: slog.New(slog.DiscardHandler)}
}

func TestPathsAreValidatedBeforeTheGuest(t *testing.T) {
	c := noExecClient(t)
	ctx := context.Background()
	rev := strings.Repeat("a", 40)
	invalid := []string{
		"",
		"workspace/a.txt",
		"/workspace/../etc/passwd",
		"/workspace/./a.txt",
		"/workspace//a.txt",
		"/workspace/a/",
		"/workspace/a/..",
		"/etc/passwd",
		"/workspacefoo/a.txt",
		"/workspace/a\x00.txt",
		"/workspace/" + strings.Repeat("a", MaxPathBytes),
	}
	for _, p := range invalid {
		if _, err := c.Read(ctx, p, ""); err == nil || err.(*Error).Code != CodeInvalidPath {
			t.Errorf("read %q: %v", p, err)
		}
		if _, err := c.Write(ctx, p, []byte("x"), nil); err == nil || err.(*Error).Code != CodeInvalidPath {
			t.Errorf("write %q: %v", p, err)
		}
		if _, err := c.Tree(ctx, p); err == nil || err.(*Error).Code != CodeInvalidPath {
			t.Errorf("tree %q: %v", p, err)
		}
		if _, err := c.Changes(ctx, p, rev); err == nil || err.(*Error).Code != CodeInvalidPath {
			t.Errorf("changes %q: %v", p, err)
		}
	}
	for _, p := range []string{"/workspace/repo/.git/config", "/workspace/repo/.git", "/workspace/.git/x", "/workspace/a/.git/b/c"} {
		if _, err := c.Write(ctx, p, []byte("x"), nil); err == nil || err.(*Error).Code != CodeInvalidPath {
			t.Errorf("write %q: %v", p, err)
		}
	}
	if _, err := c.Read(ctx, "/workspace", ""); err == nil || err.(*Error).Code != CodeNotAFile {
		t.Errorf("read of the workspace: %v", err)
	}
	if err := c.checkPath(strings.Repeat("/a", MaxPathBytes/2)[:MaxPathBytes], true); err == nil {
		t.Error("a path outside the workspace passed")
	}
	if err := c.checkPath("/workspace/"+strings.Repeat("a", MaxPathBytes-len("/workspace/")), false); err != nil {
		t.Errorf("a path of exactly %d bytes: %v", MaxPathBytes, err)
	}
	for _, p := range []string{"/workspace/.gitignore", "/workspace/a.git/x", "/workspace/x/.github/ci.yml"} {
		if err := c.checkWritePath(p); err != nil {
			t.Errorf("write %q: %v", p, err)
		}
	}
}

func TestRevsAndShasAreValidatedBeforeTheGuest(t *testing.T) {
	c := noExecClient(t)
	ctx := context.Background()
	for _, rev := range []string{"HEAD", "main", strings.Repeat("A", 40), strings.Repeat("a", 39), strings.Repeat("a", 64), "--output=/tmp/x" + strings.Repeat("a", 25)} {
		if _, err := c.Read(ctx, "/workspace/a.txt", rev); err == nil || err.(*Error).Code != CodeInvalidRev {
			t.Errorf("read rev %q: %v", rev, err)
		}
		if _, err := c.Changes(ctx, "/workspace", rev); err == nil || err.(*Error).Code != CodeInvalidRev {
			t.Errorf("changes base %q: %v", rev, err)
		}
	}
	for _, sha := range []string{"", "zz", strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("g", 64)} {
		if _, err := c.Write(ctx, "/workspace/a.txt", []byte("x"), &sha); err == nil || err.(*Error).Code != CodeInvalidSHA256 {
			t.Errorf("expected sha %q: %v", sha, err)
		}
	}
	big := make([]byte, MaxFileBytes+1)
	_, err := c.Write(ctx, "/workspace/a.txt", big, nil)
	if fe, ok := err.(*Error); !ok || fe.Code != CodeFileTooLarge || fe.Size != MaxFileBytes+1 {
		t.Errorf("oversize write: %v", err)
	}
}

// User paths and content reach the guest only as base64: the command is a
// fixed prefix and base64, and the script holds them only as base64 literals.
func TestUserDataNeverBecomesShellText(t *testing.T) {
	cfg := defaultConfig()
	hostile := "/workspace/a'; rm -rf / #$(reboot)`id`\n\"x"
	content := "'; rm -rf / #$(reboot)`id`\n"
	commandShape := regexp.MustCompile(`^printf %s [A-Za-z0-9+/=]+ \| base64 -d \| sh$`)
	scripts := map[string]string{
		"tree":    treeScript(cfg, hostile),
		"read":    readScript(cfg, hostile, ""),
		"write":   writeScript(cfg, hostile, "", strings.Repeat("a", 64), writeInline, base64.StdEncoding.EncodeToString([]byte(content)), ""),
		"changes": changesScript(cfg, hostile, strings.Repeat("a", 40)),
	}
	for name, script := range scripts {
		command := shellCommand(script)
		if !commandShape.MatchString(command) {
			t.Errorf("%s: command is not printf|base64|sh", name)
		}
		if strings.Contains(script, "rm -rf") || strings.Contains(script, "reboot") {
			t.Errorf("%s: user data appears in the script text", name)
		}
		if !strings.Contains(script, base64.StdEncoding.EncodeToString([]byte(hostile))) {
			t.Errorf("%s: the path is not passed as base64", name)
		}
	}
}

func TestChunksCoverTheContentInOrder(t *testing.T) {
	for _, n := range []int{0, 1, WriteChunkSize - 1, WriteChunkSize, WriteChunkSize + 1, 3*WriteChunkSize + 7} {
		s := strings.Repeat("x", n)
		parts := chunks(s, WriteChunkSize)
		if strings.Join(parts, "") != s {
			t.Fatalf("n=%d: chunks do not join back", n)
		}
		for i, part := range parts {
			if len(part) > WriteChunkSize || (i < len(parts)-1 && len(part) != WriteChunkSize) {
				t.Fatalf("n=%d: chunk %d has %d bytes", n, i, len(part))
			}
		}
	}
	// A full chunk's command stays under 400 KB (a whole 2 MiB file in one
	// command would be 3.7 MB).
	command := shellCommand(chunkScript(defaultConfig(), StagingDir+"/x.b64", strings.Repeat("A", WriteChunkSize), true))
	if len(command) > 400<<10 {
		t.Fatalf("a chunk command is %d bytes", len(command))
	}
}

func TestParseOutput(t *testing.T) {
	data := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("z", 200)))
	var folded strings.Builder
	for len(data) > 76 {
		folded.WriteString(data[:76] + "\n")
		data = data[76:]
	}
	folded.WriteString(data + "\n")
	out, err := parseOutput("noise\nspawn-files 1\nsize 200\nmode 100644\ndata content\n" + folded.String() + "end\nok\n")
	if err != nil || !out.ok || string(out.data["content"]) != strings.Repeat("z", 200) || out.size() != 200 || out.fields["mode"] != "100644" {
		t.Fatalf("out = %+v, %v", out, err)
	}
	if _, err := parseOutput("size 1\nok\n"); err == nil {
		t.Fatal("output without the header was accepted")
	}
	if _, err := parseOutput("spawn-files 1\ndata content\nYWJj\n"); err == nil {
		t.Fatal("a cut-off data block was accepted")
	}
	if _, err := parseOutput("spawn-files 1\ndata content\n!!!!\nend\nok\n"); err == nil {
		t.Fatal("invalid base64 was accepted")
	}
	out, err = parseOutput("spawn-files 1\nsha256 " + strings.Repeat("b", 64) + "\nerror conflict\n")
	if err != nil {
		t.Fatal(err)
	}
	if fe := out.err(); fe.Code != CodeConflict || fe.SHA256 == nil || *fe.SHA256 != strings.Repeat("b", 64) {
		t.Fatalf("conflict = %+v", fe)
	}
	out, _ = parseOutput("spawn-files 1\nsha256 \nerror conflict\n")
	if fe := out.err(); fe.SHA256 != nil {
		t.Fatalf("conflict on a missing file has sha %q", *fe.SHA256)
	}
	out, _ = parseOutput("spawn-files 1\nsize 3000000\nerror file_too_large\n")
	if fe := out.err(); fe.Code != CodeFileTooLarge || fe.Size != 3000000 {
		t.Fatalf("too large = %+v", fe)
	}
	out, _ = parseOutput("spawn-files 1\nerror write_corrupt\n")
	if fe := out.err(); fe.Code != CodeGuestFailed {
		t.Fatalf("an internal guest code leaked: %+v", fe)
	}
}

func TestClientReportsBrokenGuestOutput(t *testing.T) {
	for _, stdout := range []string{"", "spawn-files 1\nsize 1\n", "spawn-files 1\ndata content\nabc"} {
		c := &Client{
			exec: func(context.Context, string, int, plane.ExecOptions) (plane.ExecResult, error) {
				return plane.ExecResult{Stdout: stdout, ExitCode: 2}, nil
			},
			cfg: defaultConfig(),
			log: slog.New(slog.DiscardHandler),
		}
		if _, err := c.Read(context.Background(), "/workspace/a.txt", ""); err == nil || err.(*Error).Code != CodeGuestFailed {
			t.Errorf("stdout %q: %v", stdout, err)
		}
	}
}

func TestParseChanges(t *testing.T) {
	status := "M\x00a.txt\x00R087\x00old.txt\x00new.txt\x00D\x00gone.txt\x00T\x00link\x00A\x00add.txt\x00"
	numstat := "3\t1\ta.txt\x001\t2\t\x00old.txt\x00new.txt\x000\t4\tgone.txt\x00-\t-\tlink\x005\t0\tadd.txt\x00"
	untracked := "u.txt\x00nested/\x00"
	files, truncated := parseChanges([]byte(status), []byte(numstat), []byte(untracked), 10)
	want := []Change{
		{Path: "a.txt", Status: "M", Additions: 3, Deletions: 1},
		{Path: "new.txt", From: "old.txt", Status: "R", Additions: 1, Deletions: 2},
		{Path: "gone.txt", Status: "D", Deletions: 4},
		{Path: "link", Status: "M"},
		{Path: "add.txt", Status: "A", Additions: 5},
		{Path: "u.txt", Status: "?"},
	}
	if truncated || len(files) != len(want) {
		t.Fatalf("files = %+v truncated=%v", files, truncated)
	}
	for i := range want {
		if files[i] != want[i] {
			t.Errorf("file %d = %+v, want %+v", i, files[i], want[i])
		}
	}
	if files, truncated := parseChanges([]byte(status), []byte(numstat), []byte(untracked), 2); !truncated || len(files) != 2 {
		t.Fatalf("capped = %+v truncated=%v", files, truncated)
	}
	// A listing cut mid-record by the byte cap drops the partial record.
	if files, _ := parseChanges([]byte("M\x00a.txt\x00R100\x00old"), nil, nil, 10); len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("cut listing = %+v", files)
	}
	// More untracked files than the cap: the script stopped listing them.
	if _, truncated := parseChanges(nil, nil, []byte("a\x00b\x00c\x00"), 2); !truncated {
		t.Fatal("a full untracked listing must report truncation")
	}
}
