package agentsession

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"backend/internal/agent"
	"backend/internal/controlplane"
	"backend/internal/gitworkspace"
	"backend/internal/modelaccess"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	"backend/internal/vmsize"

	"github.com/google/uuid"
)

const (
	defaultRepoDir = gitworkspace.DefaultDir
	agentImage     = "agents"
	// Sandbox lifecycle for agent sessions (dev limits).
	sandboxIdleTimeout = 20 * time.Minute
	sandboxMaxLifetime = 24 * time.Hour
	// regrantBefore: model access is granted again this long before it expires.
	regrantBefore = time.Hour

	setupTimeoutS     = 60
	pollTimeoutS      = 30
	deliverTimeoutS   = 30
	stopTimeoutS      = 30
	workspaceTimeoutS = 60
)

// Sandboxes is the part of the control plane the runtime drives
// (controlplane.Service).
type Sandboxes interface {
	CreateWithID(ctx context.Context, spec controlplane.SandboxSpec) (*plane.SessionInfo, error)
	GetSession(ctx context.Context, id string) (*plane.SessionInfo, bool)
	Resume(ctx context.Context, id string) error
	Pause(ctx context.Context, id string) error
	Exec(ctx context.Context, id, command string, timeoutSec int) (plane.ExecResult, error)
}

// ModelAccess grants a sandbox's agent its owner's model (modelaccess.Granter).
type ModelAccess interface {
	Grant(ctx context.Context, userID, sandboxID, provider, token string) (modelaccess.Grant, error)
}

// WorkspaceGrants issues a session's checkout grant (HTTPQueue).
type WorkspaceGrants interface {
	WorkspaceGrant(ctx context.Context, l Lease) (WorkspaceGrant, error)
}

// SandboxWorkers resolves the worker that runs a sandbox, for the git grant
// and checkout (PlacementWorkers).
type SandboxWorkers interface {
	Worker(ctx context.Context, sandboxID string) (gitworkspace.Worker, error)
}

// PrepareFunc checks out a pinned commit in a sandbox (gitworkspace.Prepare).
type PrepareFunc func(ctx context.Context, worker gitworkspace.Worker, sandboxID string, pin gitworkspace.Pin) (gitworkspace.Result, error)

// SandboxRuntime runs sessions in Spawn sandboxes through in-process control
// plane primitives.
type SandboxRuntime struct {
	Sandboxes  Sandboxes
	Models     ModelAccess
	Workspaces WorkspaceGrants
	Workers    SandboxWorkers
	// Prepare defaults to gitworkspace.Prepare.
	Prepare PrepareFunc
	// BillingModel is recorded on the sandboxes the runtime creates.
	BillingModel string
	// TokenKey derives each session's model proxy token (DeriveTokenKey).
	TokenKey      []byte
	TurnTimeout   time.Duration
	PreviewDomain string
	// Now defaults to time.Now.
	Now func() time.Time
	// paths overrides guest paths in tests.
	paths func(sessionID string) guestPaths
}

// userError carries the message a failed session shows its user.
type userError struct {
	message string
	err     error
}

func (e *userError) Error() string { return e.message + ": " + e.err.Error() }
func (e *userError) Unwrap() error { return e.err }

func asUserError(message string, err error) error { return &userError{message: message, err: err} }

// userMessage is what the session shows for err.
func userMessage(err error, fallback string) string {
	var ue *userError
	if errors.As(err, &ue) {
		return ue.message
	}
	if errors.Is(err, errJournalLost) {
		return "The sandbox no longer has this session's history, so it cannot continue."
	}
	return fallback
}

// isTransient reports whether Ensure is worth retrying.
func isTransient(err error) bool {
	var ue *userError
	switch {
	case errors.As(err, &ue), errors.Is(err, ErrLeaseLost), errors.Is(err, ErrQueueAuth), errors.Is(err, ErrRejected),
		errors.Is(err, errJournalLost), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	}
	return true
}

// DeriveTokenKey derives the key for model proxy tokens from the worker token.
func DeriveTokenKey(workerToken string) []byte {
	sum := sha256.Sum256([]byte("renderops-agent-model-token-v1\x00" + workerToken))
	return sum[:]
}

// modelToken is the session's proxy token. It is the same on every claim, so
// a turn that outlives its runner keeps working when the next runner grants
// again (after a resume or before the TTL), without reading it from the guest.
func modelToken(key []byte, sessionID, sandboxID string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(sessionID + "\x00" + sandboxID))
	return hex.EncodeToString(mac.Sum(nil))
}

// sessionSandboxID is the session's sandbox: the one the platform recorded,
// or one derived from the session ID, so a retry never creates a second VM.
func sessionSandboxID(s *Session) (string, bool, error) {
	if s.SandboxID != "" {
		id, err := uuid.Parse(s.SandboxID)
		if err != nil {
			return "", false, fmt.Errorf("session has an invalid sandbox id %q: %w", s.SandboxID, err)
		}
		return id.String(), false, nil
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("renderops:agent-session-sandbox:"+s.ID)).String(), true, nil
}

func branchName(sessionID string) string {
	short := strings.ReplaceAll(sessionID, "-", "")
	if len(short) > 8 {
		short = short[:8]
	}
	return "spawn/" + short
}

func (rt *SandboxRuntime) now() time.Time {
	if rt.Now != nil {
		return rt.Now()
	}
	return time.Now()
}

func (rt *SandboxRuntime) guestPaths(sessionID string) guestPaths {
	if rt.paths != nil {
		return rt.paths(sessionID)
	}
	return defaultPaths(sessionID)
}

// Ensure prepares the session's sandbox: create it with the session's ID (or
// reuse/resume it), grant model access, write the guest files and, once,
// check out the pinned commit on branch spawn/<sid8>.
func (rt *SandboxRuntime) Ensure(ctx context.Context, s *Session, emit EmitFunc) (Guest, error) {
	sandboxID, derived, err := sessionSandboxID(s)
	if err != nil {
		return nil, asUserError("This session's sandbox record is invalid.", err)
	}
	log := slog.With("component", "agentsession", "session_id", s.ID, "sandbox_id", sandboxID)
	var first []Event
	if s.Status == "queued" {
		first = append(first, Event{Type: "status", Actor: "system", Payload: map[string]any{"status": "provisioning"}})
	}
	// Record the sandbox before creating it, so a retry reuses the same one.
	if derived {
		first = append(first, Event{Type: "sandbox", Actor: "system", Payload: map[string]any{"sandboxId": sandboxID}})
	}
	if len(first) > 0 {
		if err := emit(ctx, first...); err != nil {
			return nil, err
		}
		// What the platform now has; an Ensure retry does not repeat it.
		s.SandboxID = sandboxID
		if s.Status == "queued" {
			s.Status = "provisioning"
		}
	}

	info, err := rt.Sandboxes.CreateWithID(ctx, controlplane.SandboxSpec{
		ID:           sandboxID,
		UserID:       s.OwnerUserID,
		Name:         "agent-" + strings.TrimPrefix(branchName(s.ID), "spawn/"),
		BillingModel: rt.BillingModel,
		Image:        agentImage,
		VCPUs:        vmsize.Large.VCPUs,
		MemoryMB:     vmsize.Large.MemoryMB,
		DiskGB:       vmsize.Large.DiskGB,
		Internet:     true,
		IdleTimeout:  sandboxIdleTimeout,
		MaxLifetime:  sandboxMaxLifetime,
	})
	if err != nil {
		switch {
		case errors.Is(err, orchestrator.ErrNoCapacity):
			return nil, asUserError("No sandbox capacity is available right now. Start the session again in a few minutes.", err)
		case errors.Is(err, controlplane.ErrSandboxGone):
			return nil, asUserError("This session's sandbox has expired or was deleted.", err)
		case errors.Is(err, controlplane.ErrSandboxIDConflict):
			return nil, asUserError("This session's sandbox belongs to someone else.", err)
		}
		return nil, fmt.Errorf("create sandbox: %w", err)
	}
	g := &sandboxGuest{
		rt: rt, s: s, sandboxID: sandboxID, log: log,
		paths: rt.guestPaths(s.ID),
		token: modelToken(rt.TokenKey, s.ID, sandboxID),
	}
	if info.State == plane.StatePaused {
		log.Info("resuming the session sandbox")
		if err := rt.Sandboxes.Resume(ctx, sandboxID); err != nil {
			return nil, fmt.Errorf("resume sandbox: %w", err)
		}
		g.resumed = true
	}
	if err := g.grantModelAccess(ctx); err != nil {
		return nil, err
	}

	system := systemPrompt(s, branchName(s.ID), rt.PreviewDomain)
	turn := turnScript(g.paths, s.ID, orDefault(rt.TurnTimeout, defaultTurnTimeout))
	out, err := g.exec(ctx, setupScript(g.paths, envFile(g.env), system, turn), setupTimeoutS)
	if err != nil {
		return nil, fmt.Errorf("write guest files: %w", err)
	}
	report, err := parseSetup(out)
	if err != nil {
		return nil, err
	}
	if report.Size < s.RuntimeCursor {
		return nil, fmt.Errorf("%w: journal has %d bytes, the platform committed %d", errJournalLost, report.Size, s.RuntimeCursor)
	}
	g.previous = report.Published
	if !report.WorkspaceReady {
		if err := g.prepareWorkspace(ctx, emit); err != nil {
			return nil, err
		}
	}
	log.Info("agent session sandbox ready", "resumed", g.resumed, "workspace_prepared", !report.WorkspaceReady)
	return g, nil
}

// Finish stops any running turn and pauses the sandbox (which also revokes
// its model access). It returns the ports the session last showed as open,
// which pausing closes. A sandbox that is not running is left as it is.
func (rt *SandboxRuntime) Finish(ctx context.Context, s *Session) ([]Port, error) {
	sandboxID, _, err := sessionSandboxID(s)
	if err != nil {
		return nil, err
	}
	info, ok := rt.Sandboxes.GetSession(ctx, sandboxID)
	if !ok || info.UserID != s.OwnerUserID || info.State != plane.StateActive {
		return nil, nil
	}
	g := &sandboxGuest{rt: rt, s: s, sandboxID: sandboxID, paths: rt.guestPaths(s.ID),
		log: slog.With("component", "agentsession", "session_id", s.ID, "sandbox_id", sandboxID)}
	if err := g.Stop(ctx); err != nil {
		g.log.Warn("stopping the turn of a cancelled session failed", "err", err)
	}
	out, err := g.exec(ctx, publishedScript(g.paths), stopTimeoutS)
	if err != nil {
		g.log.Warn("reading the session's published ports failed", "err", err)
	}
	if err := rt.Sandboxes.Pause(ctx, sandboxID); err != nil {
		return nil, fmt.Errorf("pause sandbox: %w", err)
	}
	return parsePublished(out), nil
}

// sandboxGuest is one session's agent in its sandbox.
type sandboxGuest struct {
	rt        *SandboxRuntime
	s         *Session
	sandboxID string
	paths     guestPaths
	token     string
	log       *slog.Logger

	mu           sync.Mutex
	env          map[string]string
	grantExpires time.Time
	// resumed: the sandbox was resumed since the last poll, so servers
	// started before the pause may be gone (the next poll says so).
	resumed bool
	// previous: ports published as open by an earlier claim, handed to the
	// first poll.
	previous     []Port
	previousSent bool
}

func (g *sandboxGuest) grantModelAccess(ctx context.Context) error {
	grant, err := g.rt.Models.Grant(ctx, g.s.OwnerUserID, g.sandboxID, "anthropic", g.token)
	if err != nil {
		switch {
		case errors.Is(err, modelaccess.ErrCredentialsMissing):
			return asUserError("Save your Anthropic credentials on the Secrets page, then start the session again.", err)
		case errors.Is(err, modelaccess.ErrUnavailable):
			return asUserError("Model access is not configured on this server.", err)
		}
		return fmt.Errorf("grant model access: %w", err)
	}
	if err := validateGateway(grant.BaseURL, grant.Token); err != nil {
		return fmt.Errorf("model access grant: %w", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.env = grant.Env
	g.grantExpires = grant.ExpiresAt
	return nil
}

// refresh resumes a sandbox that was paused under the session and grants
// model access again when pausing revoked it or the grant nears its end.
func (g *sandboxGuest) refresh(ctx context.Context) error {
	info, ok := g.rt.Sandboxes.GetSession(ctx, g.sandboxID)
	if !ok {
		return errors.New("the sandbox is not available")
	}
	regrant := false
	if info.State == plane.StatePaused {
		g.log.Info("sandbox was paused under a running session; resuming it")
		if err := g.rt.Sandboxes.Resume(ctx, g.sandboxID); err != nil {
			return fmt.Errorf("resume sandbox: %w", err)
		}
		regrant = true
	}
	g.mu.Lock()
	g.resumed = g.resumed || regrant
	expires, oldEnv := g.grantExpires, g.env
	g.mu.Unlock()
	if !regrant && g.rt.now().Before(expires.Add(-regrantBefore)) {
		return nil
	}
	if err := g.grantModelAccess(ctx); err != nil {
		return err
	}
	g.mu.Lock()
	env := g.env
	g.mu.Unlock()
	if !maps.Equal(env, oldEnv) {
		// The next turn reads the new grant (the token itself never changes).
		if _, err := g.exec(ctx, envScript(g.paths, envFile(env)), setupTimeoutS); err != nil {
			return fmt.Errorf("rewrite guest env: %w", err)
		}
	}
	return nil
}

func (g *sandboxGuest) prepareWorkspace(ctx context.Context, emit EmitFunc) error {
	grant, err := g.rt.Workspaces.WorkspaceGrant(ctx, g.s.Lease())
	if err != nil {
		var denied *WorkspaceGrantError
		if errors.As(err, &denied) {
			return asUserError(denied.Message, err)
		}
		return fmt.Errorf("workspace grant: %w", err)
	}
	if err := emit(ctx, step(fmt.Sprintf("Checking out %s at %s", clean(grant.Repository, 140), shortSHA(grant.HeadSHA)))); err != nil {
		return err
	}
	worker, err := g.rt.Workers.Worker(ctx, g.sandboxID)
	if err != nil {
		return fmt.Errorf("resolve sandbox worker: %w", err)
	}
	prepare := g.rt.Prepare
	if prepare == nil {
		prepare = gitworkspace.Prepare
	}
	if _, err := prepare(ctx, worker, g.sandboxID, gitworkspace.Pin{
		Repository: grant.Repository, HeadSHA: grant.HeadSHA, BaseSHA: grant.BaseSHA,
		Token: grant.Token, TokenExpiresAt: grant.ExpiresAt,
	}); err != nil {
		switch {
		case errors.Is(err, gitworkspace.ErrCommitUnavailable):
			return asUserError("The pinned commit is no longer available from the repository.", err)
		case errors.Is(err, gitworkspace.ErrInvalidSpec):
			return asUserError("The session's pinned commit is invalid.", err)
		}
		return fmt.Errorf("check out the workspace: %w", err)
	}
	branch := branchName(g.s.ID)
	if _, err := g.exec(ctx, workspaceScript(g.paths, branch), workspaceTimeoutS); err != nil {
		return fmt.Errorf("create branch %s: %w", branch, err)
	}
	return emit(ctx, step("Workspace ready on branch "+branch))
}

// Poll runs the poll script; see pollScript.
func (g *sandboxGuest) Poll(ctx context.Context, cursor int64, portsSig string) (*PollResult, error) {
	if err := g.refresh(ctx); err != nil {
		return nil, err
	}
	g.mu.Lock()
	resumed := g.resumed
	g.mu.Unlock()
	if resumed {
		portsSig = "" // report every port again after a resume
	}
	res, err := g.rt.Sandboxes.Exec(ctx, g.sandboxID, shellCommand(pollScript(g.paths, cursor, portsSig)), pollTimeoutS)
	if err != nil {
		return nil, fmt.Errorf("poll guest: %w", err)
	}
	switch res.ExitCode {
	case exitNoChange:
		return &PollResult{}, nil
	case 0:
	default:
		return nil, fmt.Errorf("poll guest: exit %d: %s", res.ExitCode, tail(clean(res.Stderr, -1), 500))
	}
	out, err := parsePoll(res.Stdout)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	out.Resumed, g.resumed = resumed, false
	if !g.previousSent {
		out.Previous, g.previousSent = append([]Port{}, g.previous...), true
	}
	g.mu.Unlock()
	if out.Size < cursor {
		return nil, fmt.Errorf("%w: journal has %d bytes, cursor is %d", errJournalLost, out.Size, cursor)
	}
	if len(out.Data) > 0 && out.From != cursor {
		return nil, fmt.Errorf("poll returned data from %d, asked for %d", out.From, cursor)
	}
	// Defense in depth: the proxy token never reaches session events, even if
	// the agent prints it. Same length, so journal offsets stay valid.
	if g.token != "" {
		out.Data = bytes.ReplaceAll(out.Data, []byte(g.token), bytes.Repeat([]byte("*"), len(g.token)))
	}
	return out, nil
}

// Deliver starts the turn for user message seq; see deliverScript.
func (g *sandboxGuest) Deliver(ctx context.Context, seq int64, prompt string) error {
	if seq <= 0 {
		return fmt.Errorf("invalid turn seq %d", seq)
	}
	// A leading dash would be parsed as a Claude Code option.
	if strings.HasPrefix(prompt, "-") {
		prompt = " " + prompt
	}
	out, err := g.exec(ctx, deliverScript(g.paths, seq, prompt), deliverTimeoutS)
	if err != nil {
		return fmt.Errorf("start turn %d: %w", seq, err)
	}
	switch strings.TrimSpace(out) {
	case "started", "already":
		return nil
	case "start_failed":
		// turn_end is in the journal; the session continues with a message.
		g.log.Warn("agent turn did not start", "seq", seq)
		return nil
	}
	return fmt.Errorf("start turn %d: unexpected output %q", seq, clean(out, 200))
}

// RecordPorts remembers in the guest which ports the session shows as open,
// for the next claim.
func (g *sandboxGuest) RecordPorts(ctx context.Context, ports []Port) error {
	if _, err := g.exec(ctx, portsScript(g.paths, ports), setupTimeoutS); err != nil {
		return fmt.Errorf("record published ports: %w", err)
	}
	return nil
}

// Stop interrupts the running turn; see stopScript.
func (g *sandboxGuest) Stop(ctx context.Context) error {
	if _, err := g.exec(ctx, stopScript(g.paths), stopTimeoutS); err != nil {
		return fmt.Errorf("stop turn: %w", err)
	}
	return nil
}

// exec runs a script in the guest and returns its stdout; a non-zero exit is
// an error carrying the end of its stderr.
func (g *sandboxGuest) exec(ctx context.Context, script string, timeoutS int) (string, error) {
	res, err := g.rt.Sandboxes.Exec(ctx, g.sandboxID, shellCommand(script), timeoutS)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("exit %d: %s", res.ExitCode, tail(clean(res.Stderr, -1), 500))
	}
	return res.Stdout, nil
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// placementLookup resolves a sandbox's worker (orchestrator.Client).
type placementLookup interface {
	Placement(ctx context.Context, sandboxID string) (orchestrator.Placement, error)
}

// PlacementWorkers resolves a sandbox's worker client for gitworkspace.Prepare.
type PlacementWorkers struct {
	Placements  placementLookup
	WorkerToken string
}

// Worker returns a client for the worker that runs sandboxID.
func (p PlacementWorkers) Worker(ctx context.Context, sandboxID string) (gitworkspace.Worker, error) {
	placement, err := p.Placements.Placement(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("resolve sandbox %s placement: %w", sandboxID, err)
	}
	return agent.NewClient(placement.Endpoint, p.WorkerToken), nil
}
