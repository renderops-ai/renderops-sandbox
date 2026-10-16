package agentsession

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/controlplane"
	"backend/internal/gitworkspace"
	"backend/internal/modelaccess"
	"backend/internal/orchestrator"
	"backend/internal/plane"
)

// fakeSandboxes records what the runtime asks of the control plane.
type fakeSandboxes struct {
	mu        sync.Mutex
	log       *[]string
	state     plane.SessionState
	owner     string
	createErr error
	specs     []controlplane.SandboxSpec
	scripts   []string
	exec      func(script string) plane.ExecResult
}

func (f *fakeSandboxes) record(s string) {
	if f.log != nil {
		*f.log = append(*f.log, s)
	}
}

func (f *fakeSandboxes) CreateWithID(_ context.Context, spec controlplane.SandboxSpec) (*plane.SessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("create")
	f.specs = append(f.specs, spec)
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &plane.SessionInfo{ID: spec.ID, UserID: spec.UserID, State: f.state}, nil
}

func (f *fakeSandboxes) GetSession(_ context.Context, id string) (*plane.SessionInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == "" {
		return nil, false
	}
	owner := f.owner
	if owner == "" {
		owner = "user-1"
	}
	return &plane.SessionInfo{ID: id, UserID: owner, State: f.state}, true
}

func (f *fakeSandboxes) Resume(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("resume")
	f.state = plane.StateActive
	return nil
}

func (f *fakeSandboxes) Pause(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("pause")
	f.state = plane.StatePaused
	return nil
}

func (f *fakeSandboxes) Exec(_ context.Context, _ string, command string, _ int) (plane.ExecResult, error) {
	b64, ok := strings.CutPrefix(command, "printf %s ")
	if !ok || !strings.HasSuffix(b64, " | base64 -d | sh") {
		return plane.ExecResult{}, fmt.Errorf("not the guest command form: %q", command)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(b64, " | base64 -d | sh"))
	if err != nil {
		return plane.ExecResult{}, err
	}
	script := string(raw)
	f.mu.Lock()
	f.scripts = append(f.scripts, script)
	f.record("exec:" + scriptKind(script))
	hook := f.exec
	f.mu.Unlock()
	if hook != nil {
		return hook(script), nil
	}
	return plane.ExecResult{}, nil
}

func scriptKind(script string) string {
	switch {
	case strings.Contains(script, "workspace missing"):
		return "setup"
	case strings.Contains(script, "spawn-poll"):
		return "poll"
	case strings.Contains(script, "start_failed"):
		return "deliver"
	case strings.Contains(script, "kill -s"):
		return "stop"
	case strings.Contains(script, "switch -q -c"):
		return "workspace"
	case strings.Contains(script, "/ports.published'\nprintf"):
		return "ports"
	case strings.Contains(script, `mv -f "$f.tmp" "$f"`):
		return "env"
	case strings.Contains(script, `echo "published $port $bind"`):
		return "published"
	}
	return "other"
}

type fakeModels struct {
	mu     sync.Mutex
	tokens []string
	model  string
	err    error
}

func (f *fakeModels) Grant(_ context.Context, userID, sandboxID, provider, token string) (modelaccess.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return modelaccess.Grant{}, f.err
	}
	f.tokens = append(f.tokens, token)
	model := f.model
	if model == "" {
		model = "claude-opus-5-5"
	}
	return modelaccess.Grant{Provider: provider, Model: model, BaseURL: "http://172.16.0.1:7081", Token: token,
		ExpiresAt: time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC),
		Env:       modelaccess.ClaudeCodeEnv("http://172.16.0.1:7081", token, model)}, nil
}

type fakeWorkspaces struct {
	grant WorkspaceGrant
	err   error
	calls int
}

func (f *fakeWorkspaces) WorkspaceGrant(context.Context, Lease) (WorkspaceGrant, error) {
	f.calls++
	return f.grant, f.err
}

type fakeWorkers struct{}

func (fakeWorkers) Worker(context.Context, string) (gitworkspace.Worker, error) { return nil, nil }

var headSHA = strings.Repeat("a", 40)

type runtimeEnv struct {
	rt         *SandboxRuntime
	sandboxes  *fakeSandboxes
	models     *fakeModels
	workspaces *fakeWorkspaces
	pins       []gitworkspace.Pin
	log        []string
	events     []Event
	now        time.Time
}

func newRuntime(t *testing.T) *runtimeEnv {
	t.Helper()
	env := &runtimeEnv{now: time.Date(2026, 10, 16, 4, 0, 0, 0, time.UTC)}
	env.sandboxes = &fakeSandboxes{log: &env.log, state: plane.StateActive, exec: func(script string) plane.ExecResult {
		switch scriptKind(script) {
		case "setup":
			return plane.ExecResult{Stdout: "workspace missing\nsize 0\n"}
		case "workspace":
			return plane.ExecResult{Stdout: "head " + headSHA + "\n"}
		case "deliver":
			return plane.ExecResult{Stdout: "started\n"}
		case "stop":
			return plane.ExecResult{Stdout: "stopped\n"}
		case "poll":
			return plane.ExecResult{ExitCode: exitNoChange}
		}
		return plane.ExecResult{}
	}}
	env.models = &fakeModels{}
	env.workspaces = &fakeWorkspaces{grant: WorkspaceGrant{Repository: "octo/app", HeadSHA: headSHA, BaseSHA: headSHA,
		Token: "ghs_secret", ExpiresAt: env.now.Add(time.Hour)}}
	env.rt = &SandboxRuntime{
		Sandboxes: env.sandboxes, Models: env.models, Workspaces: env.workspaces, Workers: fakeWorkers{},
		Prepare: func(_ context.Context, _ gitworkspace.Worker, _ string, pin gitworkspace.Pin) (gitworkspace.Result, error) {
			env.log = append(env.log, "prepare")
			env.pins = append(env.pins, pin)
			return gitworkspace.Result{}, nil
		},
		BillingModel: "payg", TokenKey: DeriveTokenKey("worker-token"), TurnTimeout: 45 * time.Minute,
		PreviewDomain: "dev-sandbox.renderops.com",
		Now:           func() time.Time { return env.now },
	}
	return env
}

func (env *runtimeEnv) emit(_ context.Context, events ...Event) error {
	env.log = append(env.log, "emit")
	env.events = append(env.events, events...)
	return nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestEnsureCreatesTheSandboxAndPreparesTheWorkspace(t *testing.T) {
	env := newRuntime(t)
	s := newSession()
	guest, err := env.rt.Ensure(context.Background(), s, env.emit)
	if err != nil || guest == nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got := strings.Join(env.log, " "); got != "emit create exec:setup emit prepare exec:workspace emit" {
		t.Fatalf("calls = %q", got)
	}
	// provisioning and the sandbox are recorded before the VM is created.
	if env.events[0].Payload["status"] != "provisioning" || env.events[1].Type != "sandbox" {
		t.Fatalf("first events = %+v", env.events[:2])
	}
	sandboxID := env.events[1].Payload["sandboxId"].(string)
	if !uuidPattern.MatchString(sandboxID) || s.SandboxID != sandboxID {
		t.Fatalf("sandbox id %q, session has %q", sandboxID, s.SandboxID)
	}
	spec := env.sandboxes.specs[0]
	want := controlplane.SandboxSpec{ID: sandboxID, UserID: "user-1", Name: "agent-6a1f0c2e", BillingModel: "payg", Image: "agents",
		VCPUs: 2, MemoryMB: 4096, DiskGB: 20, Internet: true, IdleTimeout: 20 * time.Minute, MaxLifetime: 24 * time.Hour}
	if fmt.Sprint(spec) != fmt.Sprint(want) {
		t.Fatalf("spec = %+v", spec)
	}
	if titles := []any{env.events[2].Payload["title"], env.events[3].Payload["title"]}; titles[0] != "Checking out octo/app at aaaaaaa" ||
		titles[1] != "Workspace ready on branch spawn/6a1f0c2e" {
		t.Fatalf("steps = %v", titles)
	}
	if len(env.pins) != 1 || env.pins[0].Repository != "octo/app" || env.pins[0].Token != "ghs_secret" || env.pins[0].HeadSHA != headSHA {
		t.Fatalf("pins = %+v", env.pins)
	}

	token := env.models.tokens[0]
	if len(token) != 64 || token != modelToken(env.rt.TokenKey, s.ID, sandboxID) {
		t.Fatalf("model token %q", token)
	}
	setup := env.sandboxes.scripts[0]
	for _, secret := range []string{token, "ghs_secret"} {
		if strings.Contains(setup, secret) {
			t.Fatal("a secret appears in clear in the setup script")
		}
	}
	if !strings.Contains(setup, b64(envFile(modelaccess.ClaudeCodeEnv("http://172.16.0.1:7081", token, "claude-opus-5-5")))) {
		t.Fatal("setup does not write the granted env")
	}
	for _, script := range env.sandboxes.scripts {
		if strings.Contains(script, "ghs_secret") {
			t.Fatal("the repository token reached the guest")
		}
	}

	// A second claim reuses the same sandbox and the same token.
	env2 := newRuntime(t)
	s.Status = "running"
	env2.sandboxes.exec = func(script string) plane.ExecResult {
		return plane.ExecResult{Stdout: "workspace ok\nsize 10\n"}
	}
	if _, err := env2.rt.Ensure(context.Background(), s, env2.emit); err != nil {
		t.Fatal(err)
	}
	if env2.models.tokens[0] != token || env2.sandboxes.specs[0].ID != sandboxID || len(env2.events) != 0 {
		t.Fatalf("second claim: token %v spec %v events %v", env2.models.tokens, env2.sandboxes.specs[0].ID, env2.events)
	}
}

// A retried Ensure (sandbox busy) does not record the sandbox twice.
func TestEnsureRetryDoesNotRepeatEvents(t *testing.T) {
	env := newRuntime(t)
	env.sandboxes.createErr = fmt.Errorf("x: %w", controlplane.ErrSandboxBusy)
	s := newSession()
	if _, err := env.rt.Ensure(context.Background(), s, env.emit); !isTransient(err) {
		t.Fatalf("first Ensure = %v", err)
	}
	env.sandboxes.createErr = nil
	if _, err := env.rt.Ensure(context.Background(), s, env.emit); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, e := range env.events {
		kinds = append(kinds, e.Type)
	}
	if got := strings.Join(kinds, " "); got != "status sandbox step step" {
		t.Fatalf("events = %q", got)
	}
	if env.sandboxes.specs[0].ID != env.sandboxes.specs[1].ID {
		t.Fatal("the retry used another sandbox")
	}
}

func TestEnsureResumesAPausedSandboxAndSkipsAReadyWorkspace(t *testing.T) {
	env := newRuntime(t)
	env.sandboxes.state = plane.StatePaused
	env.sandboxes.exec = func(string) plane.ExecResult { return plane.ExecResult{Stdout: "workspace ok\nsize 4096\n"} }
	s := newSession()
	s.Status, s.SandboxID, s.RuntimeCursor = "running", "0E9A6D51-2D1B-4E8C-9F3A-7B6C5D4E3F2A", 4096
	if _, err := env.rt.Ensure(context.Background(), s, env.emit); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(env.log, " "); got != "create resume exec:setup" {
		t.Fatalf("calls = %q", got)
	}
	if env.sandboxes.specs[0].ID != "0e9a6d51-2d1b-4e8c-9f3a-7b6c5d4e3f2a" || env.workspaces.calls != 0 {
		t.Fatalf("spec id %q, workspace grants %d", env.sandboxes.specs[0].ID, env.workspaces.calls)
	}
}

func TestEnsureFailures(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(env *runtimeEnv, s *Session)
		message string
	}{
		{"no capacity", func(env *runtimeEnv, _ *Session) {
			env.sandboxes.createErr = fmt.Errorf("provision: %w", orchestrator.ErrNoCapacity)
		}, "No sandbox capacity"},
		{"sandbox gone", func(env *runtimeEnv, _ *Session) {
			env.sandboxes.createErr = fmt.Errorf("x: %w", controlplane.ErrSandboxGone)
		}, "expired or was deleted"},
		{"no credentials", func(env *runtimeEnv, _ *Session) { env.models.err = modelaccess.ErrCredentialsMissing }, "Secrets page"},
		{"repository access revoked", func(env *runtimeEnv, _ *Session) {
			env.workspaces.err = &WorkspaceGrantError{Status: 403, Message: "The GitHub App no longer has access to octo/app."}
		}, "no longer has access"},
		{"commit gone", func(env *runtimeEnv, _ *Session) {
			env.rt.Prepare = func(context.Context, gitworkspace.Worker, string, gitworkspace.Pin) (gitworkspace.Result, error) {
				return gitworkspace.Result{}, fmt.Errorf("%w: fetch failed", gitworkspace.ErrCommitUnavailable)
			}
		}, "pinned commit is no longer available"},
		{"journal lost", func(env *runtimeEnv, s *Session) { s.RuntimeCursor = 99 }, "history"},
		{"bad sandbox id", func(env *runtimeEnv, s *Session) { s.SandboxID = "nope" }, "sandbox record is invalid"},
	}
	for _, c := range cases {
		env := newRuntime(t)
		s := newSession()
		c.prepare(env, s)
		_, err := env.rt.Ensure(context.Background(), s, env.emit)
		if err == nil || !strings.Contains(userMessage(err, "fallback"), c.message) {
			t.Errorf("%s: err %v, message %q", c.name, err, userMessage(err, "fallback"))
		}
		if isTransient(err) {
			t.Errorf("%s: treated as transient", c.name)
		}
	}
	env := newRuntime(t)
	env.sandboxes.createErr = fmt.Errorf("x: %w", controlplane.ErrSandboxBusy)
	if _, err := env.rt.Ensure(context.Background(), newSession(), env.emit); !isTransient(err) {
		t.Fatalf("busy sandbox not transient: %v", err)
	}
}

func TestPollRegrantsAfterResumeAndBeforeExpiry(t *testing.T) {
	env := newRuntime(t)
	s := newSession()
	s.Status = "running"
	env.sandboxes.exec = func(script string) plane.ExecResult {
		switch scriptKind(script) {
		case "setup":
			return plane.ExecResult{Stdout: "workspace ok\nsize 0\n"}
		case "poll":
			return plane.ExecResult{ExitCode: exitNoChange}
		}
		return plane.ExecResult{}
	}
	guest, err := env.rt.Ensure(context.Background(), s, env.emit)
	if err != nil {
		t.Fatal(err)
	}
	poll := func() {
		t.Helper()
		res, err := guest.Poll(context.Background(), 0, "sig")
		if err != nil || res.Changed {
			t.Fatalf("poll = %+v, %v", res, err)
		}
	}
	poll()
	if n := len(env.models.tokens); n != 1 {
		t.Fatalf("grants = %d", n)
	}
	// Paused under the session: resume, grant the same token again.
	env.sandboxes.mu.Lock()
	env.sandboxes.state = plane.StatePaused
	env.sandboxes.mu.Unlock()
	poll()
	if n := len(env.models.tokens); n != 2 || env.models.tokens[1] != env.models.tokens[0] {
		t.Fatalf("grants after resume = %v", env.models.tokens)
	}
	// Close to the grant's end: grant again; a changed model rewrites env.
	env.now = time.Date(2026, 10, 16, 7, 30, 0, 0, time.UTC)
	env.models.model = "claude-sonnet-5"
	poll()
	if n := len(env.models.tokens); n != 3 {
		t.Fatalf("grants before expiry = %d", n)
	}
	if !strings.Contains(strings.Join(env.log, " "), "exec:env") {
		t.Fatalf("env not rewritten: %v", env.log)
	}
	env.now = time.Date(2026, 10, 16, 5, 0, 0, 0, time.UTC)
	poll()
	if n := len(env.models.tokens); n != 3 {
		t.Fatalf("granted again too early (%d)", n)
	}
}

func TestPollParsesTheReportAndMasksTheToken(t *testing.T) {
	env := newRuntime(t)
	s := newSession()
	s.Status = "running"
	var token string
	env.sandboxes.exec = func(script string) plane.ExecResult {
		switch scriptKind(script) {
		case "setup":
			return plane.ExecResult{Stdout: "workspace ok\nsize 0\n"}
		case "poll":
			data := `{"type":"assistant","message":{"content":[{"type":"text","text":"token is ` + token + `"}]}}` + "\n"
			return plane.ExecResult{Stdout: "spawn-poll 1\nsize " + fmt.Sprint(len(data)) + "\nalive 1\nturn 2\nsig 99\nport 00000000:1435\ndata 0\n" +
				base64.StdEncoding.EncodeToString([]byte(data)) + "\nend\n"}
		}
		return plane.ExecResult{}
	}
	guest, err := env.rt.Ensure(context.Background(), s, env.emit)
	if err != nil {
		t.Fatal(err)
	}
	token = env.models.tokens[0]
	res, err := guest.Poll(context.Background(), 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changed || !res.Alive || res.LastTurn != 2 || res.PortsSig != "99" || len(res.Ports) != 1 || res.Ports[0] != (Port{5173, "0.0.0.0"}) {
		t.Fatalf("poll = %+v", res)
	}
	if strings.Contains(string(res.Data), token) || !strings.Contains(string(res.Data), strings.Repeat("*", 64)) || int64(len(res.Data)) != res.Size {
		t.Fatalf("data = %q", res.Data)
	}
}

func TestDeliverStopAndFinish(t *testing.T) {
	env := newRuntime(t)
	s := newSession()
	s.Status = "running"
	env.sandboxes.exec = func(script string) plane.ExecResult {
		switch scriptKind(script) {
		case "setup":
			return plane.ExecResult{Stdout: "workspace ok\nsize 0\n"}
		case "deliver":
			return plane.ExecResult{Stdout: "started\n"}
		case "stop":
			return plane.ExecResult{Stdout: "stopped\n"}
		case "published":
			return plane.ExecResult{Stdout: "published 5173 0.0.0.0\npublished bogus x\n"}
		case "ports":
			return plane.ExecResult{}
		}
		return plane.ExecResult{ExitCode: 1, Stderr: "boom"}
	}
	guest, err := env.rt.Ensure(context.Background(), s, env.emit)
	if err != nil {
		t.Fatal(err)
	}
	if err := guest.RecordPorts(context.Background(), []Port{{5173, "0.0.0.0"}, {8080, "::"}}); err != nil {
		t.Fatal(err)
	}
	if record := env.sandboxes.scripts[len(env.sandboxes.scripts)-1]; !strings.Contains(record, b64("5173 0.0.0.0\n8080 ::\n")) {
		t.Fatalf("record script = %q", record)
	}
	if err := guest.Deliver(context.Background(), 4, "--fix it"); err != nil {
		t.Fatal(err)
	}
	deliver := env.sandboxes.scripts[len(env.sandboxes.scripts)-1]
	if !strings.Contains(deliver, "seq=4") || !strings.Contains(deliver, b64(" --fix it")) {
		t.Fatalf("deliver script = %q", deliver)
	}
	if err := guest.Deliver(context.Background(), 0, "x"); err == nil {
		t.Fatal("delivered seq 0")
	}
	if err := guest.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	env.log = nil
	ports, err := env.rt.Finish(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(env.log, " "); got != "exec:stop exec:published pause" {
		t.Fatalf("finish calls = %q", got)
	}
	if fmt.Sprint(ports) != "[{5173 0.0.0.0}]" {
		t.Fatalf("finish ports = %v", ports)
	}
	env.log = nil
	if _, err := env.rt.Finish(context.Background(), s); err != nil || len(env.log) != 0 {
		t.Fatalf("finish of a paused sandbox = %v, %v", env.log, err)
	}
	env.sandboxes.state, env.sandboxes.owner = plane.StateActive, "someone-else"
	if _, err := env.rt.Finish(context.Background(), s); err != nil || len(env.log) != 0 {
		t.Fatalf("finish touched another user's sandbox: %v", env.log)
	}
}

// The first poll after a resume reports every port (no signature) and says
// so; the first poll of a claim carries the ports an earlier claim published.
func TestPollReportsResumeAndEarlierPorts(t *testing.T) {
	env := newRuntime(t)
	env.sandboxes.state = plane.StatePaused
	s := newSession()
	s.Status = "running"
	var pollScripts []string
	env.sandboxes.exec = func(script string) plane.ExecResult {
		switch scriptKind(script) {
		case "setup":
			return plane.ExecResult{Stdout: "workspace ok\nsize 0\npublished 8080 0.0.0.0\n"}
		case "poll":
			pollScripts = append(pollScripts, script)
			return plane.ExecResult{Stdout: "spawn-poll 1\nsize 0\nalive 0\nturn 0\nsig 7\n"}
		}
		return plane.ExecResult{}
	}
	guest, err := env.rt.Ensure(context.Background(), s, env.emit)
	if err != nil {
		t.Fatal(err)
	}
	first, err := guest.Poll(context.Background(), 0, "7")
	if err != nil {
		t.Fatal(err)
	}
	if !first.Resumed || fmt.Sprint(first.Previous) != "[{8080 0.0.0.0}]" || !strings.Contains(pollScripts[0], "lastsig=''") {
		t.Fatalf("first poll = %+v, script sig forced: %v", first, strings.Contains(pollScripts[0], "lastsig=''"))
	}
	second, err := guest.Poll(context.Background(), 0, "7")
	if err != nil {
		t.Fatal(err)
	}
	if second.Resumed || second.Previous != nil || !strings.Contains(pollScripts[1], "lastsig='7'") {
		t.Fatalf("second poll = %+v", second)
	}
}

// Ensure writes the platform's system text (or the fallback) to system.txt,
// and the first poll carries the journal's last turn_end from setup.
func TestEnsureWritesTheInstructionsAndReportsTheLastTurnEnd(t *testing.T) {
	env := newRuntime(t)
	s := newSession()
	s.Status, s.Instructions = "running", testInstructions
	env.sandboxes.exec = func(script string) plane.ExecResult {
		switch scriptKind(script) {
		case "setup":
			return plane.ExecResult{Stdout: "workspace ok\nsize 0\nlastend " + `{"type":"spawn","event":"turn_end","seq":4,"exit":124,"reason":"timeout"}` + "\n"}
		case "poll":
			return plane.ExecResult{Stdout: "spawn-poll 1\nsize 0\nalive 0\nturn 4\nsig 7\n"}
		}
		return plane.ExecResult{}
	}
	guest, err := env.rt.Ensure(context.Background(), s, env.emit)
	if err != nil {
		t.Fatal(err)
	}
	setup := env.sandboxes.scripts[0]
	if !strings.Contains(setup, `put "$base/system.txt" `+b64("Rules.")+" 600") ||
		!strings.Contains(setup, `put "$base/settings.json" `+b64(claudeSettings)+" 600") {
		t.Fatalf("setup script does not write the platform instructions and settings:\n%s", setup)
	}
	first, err := guest.Poll(context.Background(), 0, "7")
	if err != nil {
		t.Fatal(err)
	}
	if first.LastTurnEnd == nil || *first.LastTurnEnd != (TurnEnd{Seq: 4, Exit: 124, Reason: "timeout"}) {
		t.Fatalf("first poll last turn end = %+v", first.LastTurnEnd)
	}
	if second, err := guest.Poll(context.Background(), 0, "7"); err != nil || second.LastTurnEnd != nil {
		t.Fatalf("second poll = %+v, %v", second, err)
	}

	env = newRuntime(t)
	s = newSession()
	if _, err := env.rt.Ensure(context.Background(), s, env.emit); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(env.sandboxes.scripts[0], b64(fallbackSystemPrompt("spawn/6a1f0c2e"))) {
		t.Fatal("setup script does not write the fallback system prompt")
	}
}

func TestDerivedIdentifiersAreStable(t *testing.T) {
	a, derived, err := sessionSandboxID(&Session{ID: sessionID})
	b, _, _ := sessionSandboxID(&Session{ID: sessionID})
	c, _, _ := sessionSandboxID(&Session{ID: testSessionID})
	if err != nil || !derived || a != b || a == c || !uuidPattern.MatchString(a) {
		t.Fatalf("derived ids %q %q %q", a, b, c)
	}
	key := DeriveTokenKey("w")
	if modelToken(key, sessionID, a) != modelToken(key, sessionID, a) || modelToken(key, sessionID, a) == modelToken(DeriveTokenKey("x"), sessionID, a) {
		t.Fatal("model token is not a stable keyed derivation")
	}
	if branchName(sessionID) != "spawn/6a1f0c2e" {
		t.Fatalf("branch = %q", branchName(sessionID))
	}
	if !errors.Is(fmt.Errorf("x: %w", asUserError("m", ErrLeaseLost)), ErrLeaseLost) {
		t.Fatal("userError does not unwrap")
	}
}
