package handler_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/sandboxfiles"
)

const filesSandboxID = "3ecb0221-faf1-4976-a8ae-6fd6262de59a"

// fakeFileExecer answers each guest command with the next canned stdout.
type fakeFileExecer struct {
	answers []string
	err     error
	calls   []fileExecCall
}

type fileExecCall struct {
	sandboxID string
	passive   bool
	timeoutS  int
}

func (f *fakeFileExecer) ExecIfActive(_ context.Context, sandboxID, _ string, timeoutS int, passive bool) (plane.ExecResult, error) {
	f.calls = append(f.calls, fileExecCall{sandboxID: sandboxID, passive: passive, timeoutS: timeoutS})
	if f.err != nil {
		return plane.ExecResult{}, f.err
	}
	if len(f.answers) == 0 {
		return plane.ExecResult{}, fmt.Errorf("unexpected guest command %d", len(f.calls))
	}
	answer := f.answers[0]
	f.answers = f.answers[1:]
	return plane.ExecResult{Stdout: answer}, nil
}

// guestAnswer builds a script's stdout: key/value lines, data blocks, then
// "ok" (or the error line given as the "error" key).
func guestAnswer(fields map[string]string, data map[string][]byte) string {
	var b strings.Builder
	b.WriteString("spawn-files 1\n")
	for key, value := range fields {
		if key != "error" {
			b.WriteString(key + " " + value + "\n")
		}
	}
	for name, raw := range data {
		b.WriteString("data " + name + "\n")
		encoded := base64.StdEncoding.EncodeToString(raw)
		for len(encoded) > 76 {
			b.WriteString(encoded[:76] + "\n")
			encoded = encoded[76:]
		}
		b.WriteString(encoded + "\nend\n")
	}
	if code, ok := fields["error"]; ok {
		b.WriteString("error " + code + "\n")
	} else {
		b.WriteString("ok\n")
	}
	return b.String()
}

func newFilesTestServer(t *testing.T, state plane.SessionState, owner string, execer handler.FileExecer) http.Handler {
	t.Helper()
	sessions := newFakeSessionService()
	sessions.sessions[filesSandboxID] = &plane.SessionInfo{ID: filesSandboxID, UserID: owner, State: state}
	resolver := &fakePlatformService{record: platform.KeyRecord{
		ID: "key-1", UserID: "tenant-1", IsActive: true, BalanceUSD: 10,
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{sandboxID}/files/tree", handler.FileTreeHandler(sessions, execer))
	mux.HandleFunc("GET /v1/sandboxes/{sandboxID}/files/content", handler.ReadFileHandler(sessions, execer))
	mux.HandleFunc("PUT /v1/sandboxes/{sandboxID}/files/content", handler.WriteFileHandler(sessions, execer))
	mux.HandleFunc("GET /v1/sandboxes/{sandboxID}/files/changes", handler.FileChangesHandler(sessions, execer))
	return middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux)
}

func filesRequest(t *testing.T, server http.Handler, method, path string, query url.Values, body []byte) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	target := "/v1/sandboxes/" + filesSandboxID + "/files/" + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer ro_live_test-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	var decoded map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("%s %s: body is not JSON: %q", method, path, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("%s %s: missing Cache-Control: no-store", method, path)
	}
	return response, decoded
}

func hexSHA(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func writeBody(t *testing.T, fields map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFilesHideForeignSandboxes(t *testing.T) {
	execer := &fakeFileExecer{}
	server := newFilesTestServer(t, plane.StateActive, "tenant-2", execer)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "tree"}, {http.MethodGet, "content"}, {http.MethodPut, "content"}, {http.MethodGet, "changes"},
	} {
		response, body := filesRequest(t, server, c.method, c.path, url.Values{"path": {"/workspace/a"}, "root": {"/workspace"}}, []byte(`{"path":"/workspace/a","content":"x"}`))
		if response.Code != http.StatusNotFound || body["code"] != "sandbox_not_found" {
			t.Fatalf("%s %s: %d %v", c.method, c.path, response.Code, body)
		}
	}
	if len(execer.calls) != 0 {
		t.Fatal("a foreign sandbox reached the worker")
	}
}

func TestFilesRefusePausedSandboxWithoutResuming(t *testing.T) {
	execer := &fakeFileExecer{}
	server := newFilesTestServer(t, plane.StatePaused, "tenant-1", execer)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "tree"}, {http.MethodGet, "content"}, {http.MethodPut, "content"}, {http.MethodGet, "changes"},
	} {
		response, body := filesRequest(t, server, c.method, c.path, url.Values{"path": {"/workspace/a"}, "root": {"/workspace"}}, []byte(`{"path":"/workspace/a","content":"x"}`))
		if response.Code != http.StatusConflict || body["code"] != "sandbox_not_active" {
			t.Fatalf("%s %s: %d %v", c.method, c.path, response.Code, body)
		}
	}
	if len(execer.calls) != 0 {
		t.Fatal("a paused sandbox reached the worker")
	}

	// The sandbox pauses after the check: the worker refuses (ActiveOnly).
	racing := &fakeFileExecer{err: fmt.Errorf("exec on worker w1: %w: session is paused", plane.ErrSandboxNotActive)}
	server = newFilesTestServer(t, plane.StateActive, "tenant-1", racing)
	response, body := filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/a"}}, nil)
	if response.Code != http.StatusConflict || body["code"] != "sandbox_not_active" {
		t.Fatalf("paused during the request: %d %v", response.Code, body)
	}
}

func TestReadFileReturnsTextAndDetectsBinary(t *testing.T) {
	text := []byte("package main\n")
	binary := []byte("PNG\x00\x01")
	latin1 := []byte("caf\xe9")
	execer := &fakeFileExecer{answers: []string{
		guestAnswer(map[string]string{"size": "13", "mode": "100755"}, map[string][]byte{"content": text}),
		guestAnswer(map[string]string{"size": "5", "mode": "100644"}, map[string][]byte{"content": binary}),
		guestAnswer(map[string]string{"size": "4", "mode": "100644"}, map[string][]byte{"content": latin1}),
		guestAnswer(map[string]string{"size": "0", "mode": "100644"}, map[string][]byte{"content": nil}),
	}}
	server := newFilesTestServer(t, plane.StateActive, "tenant-1", execer)

	response, body := filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/repo/main.go"}}, nil)
	if response.Code != http.StatusOK || body["content"] != string(text) || body["binary"] != false ||
		body["sha256"] != hexSHA(text) || body["size"] != float64(13) || body["mode"] != "100755" || body["path"] != "/workspace/repo/main.go" {
		t.Fatalf("text: %d %v", response.Code, body)
	}
	for _, content := range [][]byte{binary, latin1} {
		response, body = filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/repo/x"}}, nil)
		if _, hasContent := body["content"]; response.Code != http.StatusOK || body["binary"] != true || hasContent || body["sha256"] != hexSHA(content) {
			t.Fatalf("binary: %d %v", response.Code, body)
		}
	}
	response, body = filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/repo/empty"}}, nil)
	if content, hasContent := body["content"]; response.Code != http.StatusOK || !hasContent || content != "" {
		t.Fatalf("empty file: %d %v", response.Code, body)
	}
	for _, call := range execer.calls {
		if !call.passive || call.sandboxID != filesSandboxID || call.timeoutS <= 0 {
			t.Fatalf("read exec = %+v, want passive with a timeout", call)
		}
	}
}

func TestReadFileErrors(t *testing.T) {
	execer := &fakeFileExecer{answers: []string{
		guestAnswer(map[string]string{"size": "3145728", "error": "file_too_large"}, nil),
		guestAnswer(map[string]string{"error": "not_in_revision"}, nil),
		guestAnswer(map[string]string{"error": "file_not_found"}, nil),
		"bash: printf: write error\n",
	}}
	server := newFilesTestServer(t, plane.StateActive, "tenant-1", execer)
	rev := strings.Repeat("a", 40)

	response, body := filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/big.bin"}}, nil)
	if response.Code != http.StatusRequestEntityTooLarge || body["code"] != "file_too_large" || body["size"] != float64(3<<20) {
		t.Fatalf("too large: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/new.go"}, "rev": {rev}}, nil)
	if response.Code != http.StatusNotFound || body["code"] != "not_in_revision" {
		t.Fatalf("not in revision: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/gone.go"}}, nil)
	if response.Code != http.StatusNotFound || body["code"] != "file_not_found" {
		t.Fatalf("missing: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/x"}}, nil)
	if response.Code != http.StatusBadGateway || body["code"] != "files_failed" {
		t.Fatalf("broken guest output: %d %v", response.Code, body)
	}

	calls := len(execer.calls)
	for _, query := range []url.Values{
		{"path": {"/etc/passwd"}},
		{"path": {"/workspace/../etc/passwd"}},
		{"path": {"/workspace/a\x00b"}},
		{},
	} {
		response, body = filesRequest(t, server, http.MethodGet, "content", query, nil)
		if response.Code != http.StatusBadRequest || body["code"] != "invalid_path" {
			t.Fatalf("%v: %d %v", query, response.Code, body)
		}
	}
	response, body = filesRequest(t, server, http.MethodGet, "content", url.Values{"path": {"/workspace/a"}, "rev": {"HEAD"}}, nil)
	if response.Code != http.StatusBadRequest || body["code"] != "invalid_rev" {
		t.Fatalf("bad rev: %d %v", response.Code, body)
	}
	if len(execer.calls) != calls {
		t.Fatal("invalid requests reached the worker")
	}
}

func TestWriteFile(t *testing.T) {
	current := strings.Repeat("c", 64)
	execer := &fakeFileExecer{answers: []string{
		guestAnswer(nil, nil),
		guestAnswer(map[string]string{"sha256": current, "error": "conflict"}, nil),
		guestAnswer(map[string]string{"sha256": "", "error": "conflict"}, nil),
		guestAnswer(map[string]string{"error": "exists"}, nil),
		guestAnswer(map[string]string{"error": "parent_not_found"}, nil),
	}}
	server := newFilesTestServer(t, plane.StateActive, "tenant-1", execer)
	loaded := strings.Repeat("a", 64)

	response, body := filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/repo/a.go", "content": "x := 1\n", "expected_sha256": loaded}))
	if response.Code != http.StatusOK || body["path"] != "/workspace/repo/a.go" || body["size"] != float64(7) || body["sha256"] != hexSHA([]byte("x := 1\n")) {
		t.Fatalf("write: %d %v", response.Code, body)
	}
	if len(body) != 3 {
		t.Fatalf("write response has extra fields: %v", body)
	}
	if call := execer.calls[0]; call.passive {
		t.Fatal("a save counts as sandbox activity")
	}

	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/repo/a.go", "content": "y", "expected_sha256": loaded}))
	if response.Code != http.StatusConflict || body["code"] != "conflict" || body["sha256"] != current {
		t.Fatalf("conflict: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/repo/a.go", "content": "y", "expected_sha256": loaded}))
	if sum, present := body["sha256"]; response.Code != http.StatusConflict || !present || sum != nil {
		t.Fatalf("conflict on a deleted file: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/repo/b.go", "content": "y", "expected_sha256": nil}))
	if response.Code != http.StatusConflict || body["code"] != "exists" {
		t.Fatalf("exists: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/nodir/b.go", "content": "y"}))
	if response.Code != http.StatusNotFound || body["code"] != "parent_not_found" {
		t.Fatalf("parent missing: %d %v", response.Code, body)
	}

	calls := len(execer.calls)
	oversize := strings.Repeat("z", sandboxfiles.MaxFileBytes+1)
	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/repo/big.txt", "content": oversize}))
	if response.Code != http.StatusRequestEntityTooLarge || body["code"] != "file_too_large" || body["size"] != float64(sandboxfiles.MaxFileBytes+1) {
		t.Fatalf("oversize: %d %v", response.Code, body["code"])
	}
	huge := strings.Repeat("\x01", sandboxfiles.MaxFileBytes+1) // 6 bytes each in JSON
	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/repo/big.txt", "content": huge}))
	if response.Code != http.StatusRequestEntityTooLarge || body["code"] != "file_too_large" {
		t.Fatalf("oversize body: %d %v", response.Code, body["code"])
	}
	for _, raw := range []string{
		`{"path":"/workspace/repo/a.go"}`,
		`{"path":"/workspace/repo/a.go","content":"x","mode":"755"}`,
		`not json`,
	} {
		response, body = filesRequest(t, server, http.MethodPut, "content", nil, []byte(raw))
		if response.Code != http.StatusBadRequest || body["code"] != "invalid_request" {
			t.Fatalf("%s: %d %v", raw, response.Code, body)
		}
	}
	for _, p := range []string{"/workspace/repo/.git/config", "/workspace/../etc/passwd", "relative.txt"} {
		response, body = filesRequest(t, server, http.MethodPut, "content", nil, writeBody(t, map[string]any{"path": p, "content": "x"}))
		if response.Code != http.StatusBadRequest || body["code"] != "invalid_path" {
			t.Fatalf("%s: %d %v", p, response.Code, body)
		}
	}
	response, body = filesRequest(t, server, http.MethodPut, "content", nil,
		writeBody(t, map[string]any{"path": "/workspace/a", "content": "x", "expected_sha256": "nope"}))
	if response.Code != http.StatusBadRequest || body["code"] != "invalid_sha256" {
		t.Fatalf("bad sha: %d %v", response.Code, body)
	}
	if len(execer.calls) != calls {
		t.Fatal("refused writes reached the worker")
	}
}

func TestFileTreeAndChanges(t *testing.T) {
	head := strings.Repeat("b", 40)
	base := strings.Repeat("a", 40)
	execer := &fakeFileExecer{answers: []string{
		guestAnswer(map[string]string{"git": "1"}, map[string][]byte{
			"deleted": []byte("gone.go\x00"),
			"files":   []byte("go.mod\x00gone.go\x00src/main.go\x00"),
		}),
		guestAnswer(map[string]string{"head": head}, map[string][]byte{
			"status":    []byte("M\x00go.mod\x00"),
			"numstat":   []byte("2\t1\tgo.mod\x00"),
			"untracked": []byte("notes.md\x00"),
		}),
		guestAnswer(map[string]string{"error": "not_a_git_repo"}, nil),
	}}
	server := newFilesTestServer(t, plane.StateActive, "tenant-1", execer)

	response, body := filesRequest(t, server, http.MethodGet, "tree", url.Values{"root": {"/workspace/repo"}}, nil)
	files, _ := body["files"].([]any)
	if response.Code != http.StatusOK || body["root"] != "/workspace/repo" || body["git"] != true || body["truncated"] != false ||
		fmt.Sprint(files) != "[go.mod src/main.go]" {
		t.Fatalf("tree: %d %v", response.Code, body)
	}

	response, body = filesRequest(t, server, http.MethodGet, "changes", url.Values{"root": {"/workspace/repo"}, "base": {base}}, nil)
	changes, _ := body["files"].([]any)
	if response.Code != http.StatusOK || body["base"] != base || body["head"] != head || len(changes) != 2 {
		t.Fatalf("changes: %d %v", response.Code, body)
	}
	first, _ := changes[0].(map[string]any)
	second, _ := changes[1].(map[string]any)
	if first["path"] != "go.mod" || first["status"] != "M" || first["additions"] != float64(2) || first["deletions"] != float64(1) ||
		second["path"] != "notes.md" || second["status"] != "?" {
		t.Fatalf("changes files: %v", changes)
	}
	if _, hasFrom := first["from"]; hasFrom {
		t.Fatalf("from is only set for renames: %v", first)
	}

	response, body = filesRequest(t, server, http.MethodGet, "changes", url.Values{"root": {"/workspace"}, "base": {base}}, nil)
	if response.Code != http.StatusBadRequest || body["code"] != "not_a_git_repo" {
		t.Fatalf("not a repo: %d %v", response.Code, body)
	}
	for _, call := range execer.calls {
		if !call.passive {
			t.Fatal("tree and changes must not count as sandbox activity")
		}
	}
	calls := len(execer.calls)
	response, body = filesRequest(t, server, http.MethodGet, "changes", url.Values{"root": {"/workspace/repo"}, "base": {"main"}}, nil)
	if response.Code != http.StatusBadRequest || body["code"] != "invalid_rev" {
		t.Fatalf("bad base: %d %v", response.Code, body)
	}
	response, body = filesRequest(t, server, http.MethodGet, "tree", url.Values{"root": {"/"}}, nil)
	if response.Code != http.StatusBadRequest || body["code"] != "invalid_path" {
		t.Fatalf("root outside the workspace: %d %v", response.Code, body)
	}
	if len(execer.calls) != calls {
		t.Fatal("invalid requests reached the worker")
	}
}
