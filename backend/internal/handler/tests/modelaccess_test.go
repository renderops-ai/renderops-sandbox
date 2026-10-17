package handler_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"

	"backend/internal/handler"
	"backend/internal/middleware"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/platform"
)

// Handler tests for POST /v1/sandboxes/{id}/model-access with env_file: the
// control plane writes the env file itself, so the proxy token is in no
// response, recorded command or log line.

const modelAccessSandboxID = "sandbox-1"

// sealSecret encrypts like the platform does (see internal/secrets).
func sealSecret(t *testing.T, key []byte, userID, kind string, values map[string]string) string {
	t.Helper()
	plain, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	return "v1:" + base64.StdEncoding.EncodeToString(append(nonce, gcm.Seal(nil, nonce, plain, []byte("secrets:"+userID+":"+kind))...))
}

type modelSecretStore struct{ secret string }

func (s modelSecretStore) GetSandbox(context.Context, string) (platform.SandboxRef, bool, error) {
	return platform.SandboxRef{}, false, nil
}

func (s modelSecretStore) ProviderSecret(_ context.Context, userID, kind string) (string, bool, error) {
	return s.secret, userID == "tenant-1" && kind == "anthropic", nil
}

type fakeModelWorker struct {
	mu     sync.Mutex
	grants []plane.ModelAccessRequest
}

func (f *fakeModelWorker) GrantModelAccess(_ context.Context, _, _ string, req plane.ModelAccessRequest) (plane.ModelAccessResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.grants = append(f.grants, req)
	return plane.ModelAccessResponse{BaseURL: "http://172.16.0.1:7081", ExpiresAt: req.ExpiresAt}, nil
}

func (f *fakeModelWorker) RevokeModelAccess(context.Context, string, string) error { return nil }

// token is the proxy token of the last grant.
func (f *fakeModelWorker) token(t *testing.T) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.grants) == 0 {
		t.Fatal("no grant reached the worker")
	}
	return f.grants[len(f.grants)-1].Token
}

type modelAccessFixture struct {
	server   http.Handler
	workers  *fakeModelWorker
	execer   *cliExecer
	resolver *fakePlatformService
	logs     *lockedBuffer
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newModelAccessFixture serves the model-access route behind the real auth
// and logging middleware, and captures every log line at debug level.
func newModelAccessFixture(t *testing.T, answers ...any) *modelAccessFixture {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	store := modelSecretStore{secret: sealSecret(t, key, "tenant-1", "anthropic",
		map[string]string{"ANTHROPIC_API_KEY": "sk-ant-test", "ANTHROPIC_MODEL": "claude-opus-5-5"})}
	sessions := newFakeSessionService()
	sessions.sessions[modelAccessSandboxID] = &plane.SessionInfo{ID: modelAccessSandboxID, UserID: "tenant-1", State: plane.StateActive}
	placements := &fakePlacementResolver{placement: orchestrator.Placement{
		SandboxID: modelAccessSandboxID, WorkerID: "worker-1", Endpoint: "http://10.0.0.4:9876",
	}}
	f := &modelAccessFixture{
		workers: &fakeModelWorker{},
		execer:  &cliExecer{answers: answers},
		resolver: &fakePlatformService{
			record: platform.KeyRecord{ID: "key-1", UserID: "tenant-1", IsActive: true, BalanceUSD: 10},
			runCh:  make(chan platform.SandboxRun, 8), sandboxLogCh: make(chan platform.SandboxLog, 8),
		},
		logs: &lockedBuffer{},
	}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sandboxes/{sandboxID}/model-access",
		handler.CreateModelAccessHandler(sessions, placements, f.workers, store, key, f.execer))
	f.server = middleware.Logging(middleware.Auth(f.resolver, testExecutionPolicy(), testBillingConfig())(mux))
	return f
}

func (f *modelAccessFixture) post(t *testing.T, body string) (int, string, map[string]any) {
	t.Helper()
	response, decoded := cliRequest(t, f.server, http.MethodPost, "/v1/sandboxes/"+modelAccessSandboxID+"/model-access", []byte(body))
	return response.Code, response.Body.String(), decoded
}

// nothingRecorded checks no run, sandbox log row or usage log was recorded.
func (f *modelAccessFixture) nothingRecorded(t *testing.T) {
	t.Helper()
	select {
	case run := <-f.resolver.runCh:
		t.Fatalf("a run was recorded: %+v", run)
	case row := <-f.resolver.sandboxLogCh:
		t.Fatalf("a sandbox log row was recorded: %+v", row)
	default:
	}
	f.resolver.mu.Lock()
	defer f.resolver.mu.Unlock()
	if len(f.resolver.logs) != 0 {
		t.Fatalf("usage logs were recorded: %+v", f.resolver.logs)
	}
}

// envFileContent is the env file a write command carries: the base64
// payload of the `printf %s <payload> | base64 -d > "$t"` line in its
// script.
func envFileContent(t *testing.T, command string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^printf %s ([A-Za-z0-9+/=]+) \| base64 -d > "\$t"$`).FindStringSubmatch(decodedCommand(t, command))
	if m == nil {
		t.Fatalf("no env payload in the script:\n%s", decodedCommand(t, command))
	}
	raw, err := base64.StdEncoding.DecodeString(m[1])
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestModelAccessEnvFileKeepsTheTokenOutOfResponsesCommandsAndLogs(t *testing.T) {
	f := newModelAccessFixture(t, "spawn-env-file-ok\n")
	code, body, decoded := f.post(t, `{"provider":"anthropic","env_file":"/var/lib/spawn/cli/env"}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d: %s", code, body)
	}
	token := f.workers.token(t)
	if len(token) < 32 {
		t.Fatalf("token = %q", token)
	}

	// The response: no token, no env; the file is reported written.
	if strings.Contains(body, token) {
		t.Fatalf("the response holds the token: %s", body)
	}
	if _, ok := decoded["token"]; ok {
		t.Errorf("response has a token field: %s", body)
	}
	if _, ok := decoded["env"]; ok {
		t.Errorf("response has an env field: %s", body)
	}
	if decoded["env_file_written"] != true || decoded["env_file"] != "/var/lib/spawn/cli/env" ||
		decoded["model"] != "claude-opus-5-5" || decoded["base_url"] != "http://172.16.0.1:7081" || decoded["status"] != "success" {
		t.Errorf("response = %s", body)
	}

	// One quiet, non-passive exec carries the file; the token is not plain
	// text in it.
	if len(f.execer.commands) != 1 || f.execer.loud != 0 || f.execer.passive[0] {
		t.Fatalf("execs = %d (loud %d, passive %v)", len(f.execer.commands), f.execer.loud, f.execer.passive)
	}
	command := f.execer.commands[0]
	if strings.Contains(command, token) {
		t.Fatal("the guest command holds the token in plain text")
	}
	script := decodedCommand(t, command)
	for _, want := range []string{"umask 077\n", "f='/var/lib/spawn/cli/env'\n", "d='/var/lib/spawn/cli'\n", "chmod 700 \"$d\"\n", "chmod 600 \"$t\"\n", "mv -f \"$t\" \"$f\"\n"} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
	content := envFileContent(t, command)
	for _, want := range []string{
		"export ANTHROPIC_AUTH_TOKEN='" + token + "'\n",
		"export ANTHROPIC_BASE_URL='http://172.16.0.1:7081'\n",
		"export ANTHROPIC_MODEL='claude-opus-5-5'\n",
		"export HOME='/root'\n",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("env file lacks %q:\n%s", want, content)
		}
	}
	if lines := strings.Count(content, "\n"); lines != 9 {
		t.Errorf("env file has %d lines, want 9 (the grant's 8 and HOME):\n%s", lines, content)
	}

	// Nothing recorded, and no log line holds the token.
	f.nothingRecorded(t)
	logs := f.logs.String()
	if strings.Contains(logs, token) {
		t.Fatalf("a log line holds the token:\n%s", logs)
	}
	if !strings.Contains(logs, "model access env file written") {
		t.Errorf("no write log line:\n%s", logs)
	}
}

func TestModelAccessEnvFileIsValidatedBeforeTheGrant(t *testing.T) {
	for _, path := range []string{
		"relative/env", "/etc/profile", "/var/lib/spawn", "/var/lib/spawn/", "/var/lib/spawn/../etc/profile",
		"/var/lib/spawn/cli/../../../etc/profile", "/var/lib/spawnx/env", "/workspace/.env", "/var/lib/spawn/cli//env",
	} {
		f := newModelAccessFixture(t)
		body, err := json.Marshal(map[string]string{"provider": "anthropic", "env_file": path})
		if err != nil {
			t.Fatal(err)
		}
		code, raw, decoded := f.post(t, string(body))
		if code != http.StatusBadRequest || decoded["code"] != "invalid_env_file" {
			t.Errorf("%q: %d %s", path, code, raw)
		}
		if len(f.workers.grants) != 0 || len(f.execer.commands) != 0 {
			t.Errorf("%q: %d grants, %d execs", path, len(f.workers.grants), len(f.execer.commands))
		}
	}
}

func TestModelAccessEnvFileWriteFailure(t *testing.T) {
	for name, c := range map[string]struct {
		answer any
		status int
		code   string
	}{
		"exec error":     {errors.New("worker unreachable"), http.StatusBadGateway, "env_file_failed"},
		"script failed":  {"", http.StatusBadGateway, "env_file_failed"},
		"sandbox paused": {plane.ErrSandboxNotActive, http.StatusConflict, "sandbox_not_active"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newModelAccessFixture(t, c.answer)
			code, body, decoded := f.post(t, `{"provider":"anthropic","env_file":"/var/lib/spawn/cli/env"}`)
			if code != c.status || decoded["code"] != c.code {
				t.Fatalf("got %d %s, want %d %s", code, body, c.status, c.code)
			}
			token := f.workers.token(t)
			if strings.Contains(body, token) || strings.Contains(f.logs.String(), token) {
				t.Fatal("the token leaked into the response or the logs")
			}
			f.nothingRecorded(t)
		})
	}
}

// Without env_file the response is unchanged: the token and env are returned
// and nothing runs in the guest.
func TestModelAccessWithoutEnvFileReturnsTheToken(t *testing.T) {
	f := newModelAccessFixture(t)
	code, body, decoded := f.post(t, `{"provider":"anthropic"}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d: %s", code, body)
	}
	token := f.workers.token(t)
	env, _ := decoded["env"].(map[string]any)
	if decoded["token"] != token || env["ANTHROPIC_AUTH_TOKEN"] != token {
		t.Fatalf("response = %s", body)
	}
	if _, ok := decoded["env_file_written"]; ok {
		t.Errorf("env_file_written without env_file: %s", body)
	}
	if len(f.execer.commands) != 0 {
		t.Fatalf("%d guest execs without env_file", len(f.execer.commands))
	}
}
