package handler_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
	"backend/internal/sandboxfiles"
	"backend/internal/terminal"
	"backend/internal/uploads"
)

// Handler tests for the spawn CLI's routes: terminal commands, uploads and
// the patch.

// cliExecer answers guest commands in order and records them. An answer
// that is an error fails that exec.
type cliExecer struct {
	mu       sync.Mutex
	answers  []any
	commands []string
	passive  []bool
	loud     int
}

func (f *cliExecer) ExecIfActive(_ context.Context, _, command string, _ int, opts plane.ExecOptions) (plane.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, command)
	f.passive = append(f.passive, opts.Passive)
	if !opts.Quiet {
		f.loud++
	}
	if len(f.answers) == 0 {
		return plane.ExecResult{}, fmt.Errorf("unexpected guest command %d", len(f.commands))
	}
	answer := f.answers[0]
	f.answers = f.answers[1:]
	if err, ok := answer.(error); ok {
		return plane.ExecResult{}, err
	}
	return plane.ExecResult{Stdout: answer.(string)}, nil
}

// decodedCommand is the script a guest command runs (each command is
// `printf %s <base64> | base64 -d | sh`).
func decodedCommand(t *testing.T, command string) string {
	t.Helper()
	fields := strings.Fields(command)
	if len(fields) < 3 || fields[0] != "printf" {
		t.Fatalf("unexpected guest command %q", command)
	}
	raw, err := base64.StdEncoding.DecodeString(fields[2])
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func cliRequest(t *testing.T, server http.Handler, method, target string, body []byte) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer ro_live_test-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	var decoded map[string]any
	if strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("%s %s: body is not JSON: %q", method, target, response.Body.String())
		}
	}
	return response, decoded
}

func cliServer(t *testing.T, state plane.SessionState, owner string, execer handler.FileExecer, store handler.UploadStore) http.Handler {
	t.Helper()
	sessions := newFakeSessionService()
	sessions.sessions[filesSandboxID] = &plane.SessionInfo{ID: filesSandboxID, UserID: owner, State: state}
	sessions.sessions["other-sandbox"] = &plane.SessionInfo{ID: "other-sandbox", UserID: "tenant-1", State: plane.StateActive}
	resolver := &fakePlatformService{record: platform.KeyRecord{ID: "key-1", UserID: "tenant-1", IsActive: true, BalanceUSD: 10}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{sandboxID}/files/patch", handler.FilePatchHandler(sessions, execer))
	mux.HandleFunc("POST /v1/sandboxes/{sandboxID}/uploads", handler.CreateUploadHandler(sessions, execer, store))
	mux.HandleFunc("POST /v1/sandboxes/{sandboxID}/uploads/{uploadID}/extract", handler.ExtractUploadHandler(sessions, execer, store))
	return middleware.Logging(middleware.Auth(resolver, testExecutionPolicy(), testBillingConfig())(mux))
}

// ---- terminal commands ----

func terminalCommandServer(t *testing.T, workers *fakeTerminalWorker) http.Handler {
	t.Helper()
	sessions := newFakeSessionService()
	sessions.sessions["sandbox-1"] = &plane.SessionInfo{ID: "sandbox-1", UserID: "tenant-1", State: plane.StateActive}
	placements := &fakePlacementResolver{placement: orchestrator.Placement{
		SandboxID: "sandbox-1", WorkerID: "worker-1", Endpoint: "http://10.0.0.4:9876",
	}}
	manager, err := terminal.NewManager(terminalTestSecret, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return newTerminalTestServer(t, sessions, placements, workers, manager)
}

func postTerminal(t *testing.T, server http.Handler, body string) (*httptest.ResponseRecorder, handler.APIError) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/sandbox-1/terminals", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer ro_live_test-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	var apiErr handler.APIError
	if response.Code != http.StatusCreated {
		if err := json.Unmarshal(response.Body.Bytes(), &apiErr); err != nil {
			t.Fatalf("error body is not JSON: %q", response.Body.String())
		}
	}
	return response, apiErr
}

func TestCreateTerminalPassesTheCommand(t *testing.T) {
	workers := &fakeTerminalWorker{}
	server := terminalCommandServer(t, workers)
	response, _ := postTerminal(t, server, `{"columns":100,"rows":30,"command":["claude","--continue"],"cwd":"/workspace/repo","env_file":"/var/lib/spawn/cli/env"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	want := terminal.Command{Argv: []string{"claude", "--continue"}, Cwd: "/workspace/repo", EnvFile: "/var/lib/spawn/cli/env"}
	if len(workers.starts) != 1 || fmt.Sprint(workers.starts[0]) != fmt.Sprint(want) {
		t.Fatalf("worker got %+v", workers.starts)
	}
	// Without the new fields the terminal is a plain shell, as before.
	if response, _ := postTerminal(t, server, `{"shell":"/bin/bash"}`); response.Code != http.StatusCreated || !workers.starts[1].IsZero() {
		t.Fatalf("plain terminal: %d %+v", response.Code, workers.starts)
	}
}

func TestCreateTerminalValidatesTheCommand(t *testing.T) {
	workers := &fakeTerminalWorker{}
	server := terminalCommandServer(t, workers)
	for _, body := range []string{
		`{"command":[]}`,
		`{"command":["-c","x"]}`,
		`{"command":[""]}`,
		`{"command":["claude"],"cwd":"/etc"}`,
		`{"command":["claude"],"cwd":"/workspace/../etc"}`,
		`{"command":["claude"],"cwd":"workspace"}`,
		`{"command":["claude"],"env_file":"/workspace/.env"}`,
		`{"command":["claude"],"env_file":"/var/lib/spawn"}`,
		`{"command":"claude"}`,
		`{"command":["claude"],"args":["x"]}`,
		`{"command":["a",` + strconv.Quote(strings.Repeat("x", terminal.MaxCommandBytes)) + `]}`,
	} {
		response, apiErr := postTerminal(t, server, body)
		if response.Code != http.StatusBadRequest || apiErr.Code != "invalid_terminal_options" {
			t.Errorf("%s: %d %+v", body, response.Code, apiErr)
		}
	}
	if workers.openCalls != 0 {
		t.Fatal("an invalid command reached the worker")
	}
}

func TestCreateTerminalRefusesWorkersWithoutCommands(t *testing.T) {
	workers := &fakeTerminalWorker{openErr: fmt.Errorf("open: %w", terminal.ErrCommandUnsupported)}
	server := terminalCommandServer(t, workers)
	response, apiErr := postTerminal(t, server, `{"command":["claude"]}`)
	if response.Code != http.StatusBadGateway || apiErr.Code != "terminal_command_unsupported" {
		t.Fatalf("%d %+v", response.Code, apiErr)
	}
	if strings.Contains(response.Body.String(), "attachment_token") {
		t.Fatal("a token was issued for a terminal that is not running the command")
	}
}

// ---- uploads ----

// fakeBlobs is blob storage in memory.
type fakeBlobs struct {
	mu      sync.Mutex
	sizes   map[string]int64
	deleted []string
}

func (f *fakeBlobs) PutURL(name string, expiry time.Time) (string, error) {
	return "https://acct.blob.core.windows.net/uploads/" + name + "?sp=cw&se=" + expiry.UTC().Format(time.RFC3339), nil
}

func (f *fakeBlobs) GetURL(name string, _ time.Time) (string, error) {
	return "https://acct.blob.core.windows.net/uploads/" + name + "?sp=r&sig=read-secret", nil
}

func (f *fakeBlobs) Size(_ context.Context, name string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	size, ok := f.sizes[name]
	return size, ok, nil
}

func (f *fakeBlobs) Delete(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sizes, name)
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeBlobs) ListOlder(context.Context, string, time.Time) ([]string, error) { return nil, nil }

func (f *fakeBlobs) put(name string, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sizes[name] = size
}

var archiveSHA = strings.Repeat("ab", 32)

func uploadsPath(rest string) string {
	return "/v1/sandboxes/" + filesSandboxID + "/uploads" + rest
}

func createUpload(t *testing.T, server http.Handler, size int64) map[string]any {
	t.Helper()
	response, body := cliRequest(t, server, http.MethodPost, uploadsPath(""), []byte(fmt.Sprintf(`{"size":%d,"sha256":%q}`, size, archiveSHA)))
	if response.Code != http.StatusCreated {
		t.Fatalf("create upload: %d %v", response.Code, body)
	}
	return body
}

func TestCreateUpload(t *testing.T) {
	blobs := &fakeBlobs{sizes: map[string]int64{}}
	store := uploads.New(blobs, 1000, testLogger())
	server := cliServer(t, plane.StateActive, "tenant-1", &cliExecer{}, store)

	body := createUpload(t, server, 1000)
	id, _ := body["upload_id"].(string)
	putURL, _ := body["put_url"].(string)
	headers, _ := body["put_headers"].(map[string]any)
	expires, err := time.Parse(time.RFC3339, fmt.Sprint(body["expires_at"]))
	if id == "" || !strings.Contains(putURL, "uploads/"+id+".tar.gz") || !strings.Contains(putURL, "sp=cw") ||
		headers["x-ms-blob-type"] != "BlockBlob" || err != nil || time.Until(expires) > uploads.PutTTL || body["max_bytes"] != float64(1000) {
		t.Fatalf("created = %v", body)
	}

	for _, c := range []struct {
		body string
		code string
		want int
	}{
		{`{"size":1001,"sha256":"` + archiveSHA + `"}`, "upload_too_large", http.StatusRequestEntityTooLarge},
		{`{"size":0,"sha256":"` + archiveSHA + `"}`, "invalid_size", http.StatusBadRequest},
		{`{"size":-5,"sha256":"` + archiveSHA + `"}`, "invalid_size", http.StatusBadRequest},
		{`{"size":10,"sha256":"` + strings.ToUpper(archiveSHA) + `"}`, "invalid_sha256", http.StatusBadRequest},
		{`{"size":10,"sha256":"abc"}`, "invalid_sha256", http.StatusBadRequest},
		{`{"size":10}`, "invalid_sha256", http.StatusBadRequest},
		{`{"size":"10","sha256":"` + archiveSHA + `"}`, "invalid_request", http.StatusBadRequest},
		{`{"size":10,"sha256":"` + archiveSHA + `","extra":1}`, "invalid_request", http.StatusBadRequest},
	} {
		response, decoded := cliRequest(t, server, http.MethodPost, uploadsPath(""), []byte(c.body))
		if response.Code != c.want || decoded["code"] != c.code {
			t.Errorf("%s: %d %v", c.body, response.Code, decoded)
		}
		if c.code == "upload_too_large" && decoded["max_bytes"] != float64(1000) {
			t.Errorf("too large without max_bytes: %v", decoded)
		}
	}

	// Pending uploads per sandbox are capped.
	for range uploads.MaxPendingPerSandbox - 1 {
		createUpload(t, server, 10)
	}
	response, decoded := cliRequest(t, server, http.MethodPost, uploadsPath(""), []byte(`{"size":10,"sha256":"`+archiveSHA+`"}`))
	if response.Code != http.StatusTooManyRequests || decoded["code"] != "too_many_uploads" {
		t.Fatalf("over the pending cap: %d %v", response.Code, decoded)
	}
}

func TestUploadsNeedOwnerActiveSandboxAndStorage(t *testing.T) {
	blobs := &fakeBlobs{sizes: map[string]int64{}}
	store := uploads.New(blobs, 1000, testLogger())
	createBody := []byte(`{"size":10,"sha256":"` + archiveSHA + `"}`)
	extractBody := []byte(`{"dest":"/workspace/repo"}`)
	for _, c := range []struct {
		name  string
		state plane.SessionState
		owner string
		store handler.UploadStore
		code  string
		want  int
	}{
		{"foreign", plane.StateActive, "tenant-2", store, "sandbox_not_found", http.StatusNotFound},
		{"paused", plane.StatePaused, "tenant-1", store, "sandbox_not_active", http.StatusConflict},
		{"not configured", plane.StateActive, "tenant-1", nil, "uploads_unavailable", http.StatusServiceUnavailable},
	} {
		execer := &cliExecer{}
		server := cliServer(t, c.state, c.owner, execer, c.store)
		for _, req := range []struct {
			path string
			body []byte
		}{{uploadsPath(""), createBody}, {uploadsPath("/some-id/extract"), extractBody}} {
			response, decoded := cliRequest(t, server, http.MethodPost, req.path, req.body)
			if response.Code != c.want || decoded["code"] != c.code {
				t.Errorf("%s %s: %d %v", c.name, req.path, response.Code, decoded)
			}
		}
		if len(execer.commands) != 0 {
			t.Errorf("%s: reached the guest", c.name)
		}
	}
}

func extractAnswer(fields ...string) string {
	return "spawn-files 1\n" + strings.Join(fields, "\n") + "\n"
}

func TestExtractUpload(t *testing.T) {
	blobs := &fakeBlobs{sizes: map[string]int64{}}
	store := uploads.New(blobs, 1000, testLogger())
	execer := &cliExecer{}
	server := cliServer(t, plane.StateActive, "tenant-1", execer, store)
	extract := func(id, dest string) (*httptest.ResponseRecorder, map[string]any) {
		return cliRequest(t, server, http.MethodPost, uploadsPath("/"+id+"/extract"), []byte(`{"dest":`+strconv.Quote(dest)+`}`))
	}

	id := createUpload(t, server, 100)["upload_id"].(string)
	blob := uploads.Prefix + id + ".tar.gz"

	// Not uploaded yet: retry later.
	if response, decoded := extract(id, "/workspace/repo"); response.Code != http.StatusConflict || decoded["code"] != "upload_incomplete" {
		t.Fatalf("before the PUT: %d %v", response.Code, decoded)
	}
	blobs.put(blob, 100)

	// Unknown, or another sandbox's: not found. A missing dest: invalid.
	if response, decoded := extract("nope", "/workspace/repo"); response.Code != http.StatusNotFound || decoded["code"] != "upload_not_found" {
		t.Fatalf("unknown upload: %d %v", response.Code, decoded)
	}
	other, decoded := cliRequest(t, server, http.MethodPost, "/v1/sandboxes/other-sandbox/uploads/"+id+"/extract", []byte(`{"dest":"/workspace/repo"}`))
	if other.Code != http.StatusNotFound || decoded["code"] != "upload_not_found" {
		t.Fatalf("another sandbox's upload: %d %v", other.Code, decoded)
	}
	if response, decoded := cliRequest(t, server, http.MethodPost, uploadsPath("/"+id+"/extract"), []byte(`{}`)); response.Code != http.StatusBadRequest {
		t.Fatalf("no dest: %d %v", response.Code, decoded)
	}

	// dest in use: reported, and the upload stays for a retry.
	execer.answers = []any{extractAnswer("error dest_not_empty")}
	if response, decoded := extract(id, "/workspace/repo"); response.Code != http.StatusConflict || decoded["code"] != "dest_not_empty" {
		t.Fatalf("dest in use: %d %v", response.Code, decoded)
	}
	// The sandbox paused meanwhile: the upload stays too.
	execer.answers = []any{fmt.Errorf("exec: %w", plane.ErrSandboxNotActive)}
	if response, decoded := extract(id, "/workspace/repo"); response.Code != http.StatusConflict || decoded["code"] != "sandbox_not_active" {
		t.Fatalf("paused meanwhile: %d %v", response.Code, decoded)
	}
	if len(blobs.deleted) != 0 {
		t.Fatalf("retryable failures deleted the blob: %v", blobs.deleted)
	}

	execer.answers = []any{extractAnswer("entries 3", "bytes 42", "ok")}
	response, body := extract(id, "/workspace/repo")
	if response.Code != http.StatusOK || body["entries"] != float64(3) || body["bytes"] != float64(42) ||
		body["dest"] != "/workspace/repo" || body["upload_id"] != id {
		t.Fatalf("extract: %d %v", response.Code, body)
	}
	script := decodedCommand(t, execer.commands[len(execer.commands)-1])
	for _, plain := range []string{"read-secret", "/workspace/repo", "acct.blob.core.windows.net"} {
		if strings.Contains(script, plain) {
			t.Errorf("%q is shell text in the guest script", plain)
		}
	}
	if execer.passive[len(execer.passive)-1] {
		t.Error("extract must count as sandbox activity")
	}
	if execer.loud != 0 {
		t.Errorf("%d extract commands were not quiet", execer.loud)
	}
	if len(blobs.deleted) != 1 || blobs.deleted[0] != blob {
		t.Fatalf("extracted blob not deleted: %v", blobs.deleted)
	}
	// Single use.
	if response, decoded := extract(id, "/workspace/other"); response.Code != http.StatusNotFound || decoded["code"] != "upload_not_found" {
		t.Fatalf("second extract: %d %v", response.Code, decoded)
	}
}

func TestExtractUploadDiscardsRefusedArchives(t *testing.T) {
	blobs := &fakeBlobs{sizes: map[string]int64{}}
	store := uploads.New(blobs, 1000, testLogger())
	execer := &cliExecer{}
	server := cliServer(t, plane.StateActive, "tenant-1", execer, store)

	id := createUpload(t, server, 100)["upload_id"].(string)
	blobs.put(uploads.Prefix+id+".tar.gz", 100)
	entry := base64.StdEncoding.EncodeToString([]byte("../evil"))
	execer.answers = []any{extractAnswer("entry "+entry, "error unsafe_path")}
	response, decoded := cliRequest(t, server, http.MethodPost, uploadsPath("/"+id+"/extract"), []byte(`{"dest":"/workspace/repo"}`))
	if response.Code != http.StatusUnprocessableEntity || decoded["code"] != "unsafe_path" || decoded["entry"] != "../evil" {
		t.Fatalf("unsafe archive: %d %v", response.Code, decoded)
	}
	if len(blobs.deleted) != 1 {
		t.Fatalf("refused archive kept: %v", blobs.deleted)
	}

	// A blob of another size than declared is refused before the guest.
	id = createUpload(t, server, 100)["upload_id"].(string)
	blobs.put(uploads.Prefix+id+".tar.gz", 5000)
	calls := len(execer.commands)
	response, decoded = cliRequest(t, server, http.MethodPost, uploadsPath("/"+id+"/extract"), []byte(`{"dest":"/workspace/repo"}`))
	if response.Code != http.StatusUnprocessableEntity || decoded["code"] != sandboxfiles.CodeSizeMismatch || len(execer.commands) != calls {
		t.Fatalf("oversized blob: %d %v", response.Code, decoded)
	}
	if len(blobs.deleted) != 2 {
		t.Fatalf("oversized blob kept: %v", blobs.deleted)
	}

	// Bad dests never reach the guest; the upload stays.
	id = createUpload(t, server, 100)["upload_id"].(string)
	blobs.put(uploads.Prefix+id+".tar.gz", 100)
	for _, dest := range []string{"/workspace", "/etc/x", "/workspace/a/.git/b", "relative"} {
		response, decoded := cliRequest(t, server, http.MethodPost, uploadsPath("/"+id+"/extract"), []byte(`{"dest":`+strconv.Quote(dest)+`}`))
		if response.Code != http.StatusBadRequest || decoded["code"] != sandboxfiles.CodeInvalidDest {
			t.Errorf("%s: %d %v", dest, response.Code, decoded)
		}
	}
	if len(execer.commands) != calls || len(blobs.deleted) != 2 {
		t.Fatalf("bad dests: %d guest calls, deleted %v", len(execer.commands)-calls, blobs.deleted)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// ---- patch ----

func patchAnswer(size int, sum string) string {
	return guestAnswer(map[string]string{"head": strings.Repeat("c", 40), "size": strconv.Itoa(size), "sha256": sum}, nil)
}

func chunkAnswer(chunk []byte) string {
	return guestAnswer(nil, map[string][]byte{"chunk": chunk})
}

func patchPath(query string) string {
	return "/v1/sandboxes/" + filesSandboxID + "/files/patch?" + query
}

func TestFilePatchStreams(t *testing.T) {
	// Three chunks: two full ones and a short one.
	patch := bytes.Repeat([]byte("diff --git a/x b/x\n+line\n"), (2*sandboxfiles.PatchChunkBytes+1000)/25)
	full := sandboxfiles.PatchChunkBytes
	execer := &cliExecer{answers: []any{
		patchAnswer(len(patch), hexSHA(patch)),
		chunkAnswer(patch[:full]), chunkAnswer(patch[full : 2*full]), chunkAnswer(patch[2*full:]),
		guestAnswer(nil, nil), // remove
	}}
	server := cliServer(t, plane.StateActive, "tenant-1", execer, nil)
	base := strings.Repeat("b", 40)
	response, _ := cliRequest(t, server, http.MethodGet, patchPath("root=/workspace/repo&base="+base), nil)
	h := response.Header()
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), patch) ||
		h.Get("Content-Length") != strconv.Itoa(len(patch)) || h.Get("X-Patch-Sha256") != hexSHA(patch) ||
		h.Get("X-Patch-Base") != base || h.Get("X-Patch-Head") != strings.Repeat("c", 40) ||
		h.Get("Content-Type") != "text/x-diff" || h.Get("Cache-Control") != "no-store" {
		t.Fatalf("patch: %d %v (%d bytes)", response.Code, h, response.Body.Len())
	}
	if len(execer.commands) != 5 || len(execer.answers) != 0 {
		t.Fatalf("%d guest calls", len(execer.commands))
	}
	for _, passive := range execer.passive {
		if !passive {
			t.Fatal("patches must not count as sandbox activity")
		}
	}
	if execer.loud != 0 {
		t.Fatalf("%d patch commands were not quiet (the patch would fill the log stream)", execer.loud)
	}
}

func TestFilePatchEmptyAndErrors(t *testing.T) {
	base := strings.Repeat("b", 40)
	empty := &cliExecer{answers: []any{patchAnswer(0, hexSHA(nil)), guestAnswer(nil, nil)}}
	response, _ := cliRequest(t, cliServer(t, plane.StateActive, "tenant-1", empty, nil), http.MethodGet, patchPath("root=/workspace/repo&base="+base), nil)
	if response.Code != http.StatusOK || response.Body.Len() != 0 || response.Header().Get("Content-Length") != "0" || len(empty.commands) != 2 {
		t.Fatalf("empty patch: %d %q", response.Code, response.Body.String())
	}

	tooLarge := &cliExecer{answers: []any{guestAnswer(map[string]string{"size": "52428801", "error": "patch_too_large"}, nil)}}
	response, decoded := cliRequest(t, cliServer(t, plane.StateActive, "tenant-1", tooLarge, nil), http.MethodGet, patchPath("root=/workspace/repo&base="+base), nil)
	if response.Code != http.StatusRequestEntityTooLarge || decoded["code"] != "patch_too_large" || decoded["size"] != float64(52428801) {
		t.Fatalf("too large: %d %v", response.Code, decoded)
	}

	none := &cliExecer{}
	server := cliServer(t, plane.StateActive, "tenant-1", none, nil)
	for query, code := range map[string]string{
		"root=/workspace/repo&base=HEAD": "invalid_rev",
		"root=/workspace/repo":           "invalid_rev",
		"root=/etc&base=" + base:         "invalid_path",
	} {
		response, decoded := cliRequest(t, server, http.MethodGet, patchPath(query), nil)
		if response.Code != http.StatusBadRequest || decoded["code"] != code {
			t.Errorf("%s: %d %v", query, response.Code, decoded)
		}
	}
	for _, c := range []struct {
		state plane.SessionState
		owner string
		code  string
	}{{plane.StateActive, "tenant-2", "sandbox_not_found"}, {plane.StatePaused, "tenant-1", "sandbox_not_active"}} {
		response, decoded := cliRequest(t, cliServer(t, c.state, c.owner, none, nil), http.MethodGet, patchPath("root=/workspace/repo&base="+base), nil)
		if decoded["code"] != c.code {
			t.Errorf("%s: %d %v", c.code, response.Code, decoded)
		}
	}
	if len(none.commands) != 0 {
		t.Fatal("refused patch requests reached the guest")
	}
}

func TestFilePatchCutShortOnFailure(t *testing.T) {
	patch := bytes.Repeat([]byte("x"), sandboxfiles.PatchChunkBytes+10)
	execer := &cliExecer{answers: []any{
		patchAnswer(len(patch), hexSHA(patch)),
		chunkAnswer(patch[:sandboxfiles.PatchChunkBytes]),
		errors.New("worker went away"),
		guestAnswer(nil, nil), // remove still runs
	}}
	server := cliServer(t, plane.StateActive, "tenant-1", execer, nil)
	response, _ := cliRequest(t, server, http.MethodGet, patchPath("root=/workspace/repo&base="+strings.Repeat("b", 40)), nil)
	if response.Code != http.StatusOK || response.Body.Len() >= len(patch) || response.Header().Get("Content-Length") != strconv.Itoa(len(patch)) {
		t.Fatalf("cut short: %d, %d of %s bytes", response.Code, response.Body.Len(), response.Header().Get("Content-Length"))
	}
	if len(execer.answers) != 0 {
		t.Fatal("the patch staging file was not removed")
	}
}
