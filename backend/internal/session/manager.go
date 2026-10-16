package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"backend/internal/cgroup"
	"backend/internal/checkpoint"
	"backend/internal/executor"
	"backend/internal/executor/firecracker"
	"backend/internal/sandboximage"
	"backend/internal/terminal"
	"backend/internal/vmsize"

	"github.com/google/uuid"
)

const (
	// A short scan interval keeps user-selected idle deadlines reasonably accurate without
	// creating one timer goroutine per sandbox.
	reaperInterval = 10 * time.Second
	pauseTTL       = 7 * 24 * time.Hour
	consoleLogTail = 16 * 1024
)

func logTail(value string) string {
	if len(value) <= consoleLogTail {
		return value
	}
	return value[len(value)-consoleLogTail:]
}

// StateHook is called on every session lifecycle transition — manual (endpoint) AND
// automatic (reaper idle-pause, on-demand resume, TTL destroy) — so an external store
// (the sandboxes DB) can stay in sync. state is "active" | "paused" | "destroyed".
type StateHook func(sessionID, userID, state string) error

// MeterSample is one accrual interval's RAW resource-time for a sandbox (no cost).
type MeterSample struct {
	UserID        string
	SandboxID     string
	BillingModel  string
	Bucket        time.Time
	VCPUSeconds   float64
	RAMGBSeconds  float64
	DiskGBSeconds float64
}

// MeterHook receives raw usage deltas from the accrual ticker to persist (usage_meters).
type MeterHook func(MeterSample)

type CheckpointWriter interface {
	Save(context.Context, checkpoint.Input) (string, error)
}

// CheckpointDeleter removes all durable checkpoints of a sandbox. The checkpoint
// writer implements it; Destroy uses it so a deleted sandbox leaves no data behind.
type CheckpointDeleter interface {
	DeleteSandbox(context.Context, string) (int, error)
}

type CheckpointReader interface {
	Restore(context.Context, string, string, checkpoint.RestorePaths) (checkpoint.RestoreResult, error)
}

type Manager struct {
	logMu            sync.RWMutex
	logStart         func(string, string)
	logStop          func(string)
	slotGrantMu      sync.RWMutex
	slotRevokers     []func(string)
	slotBindMu       sync.Mutex // see BindSlotGrant
	store            *Store
	onState          StateHook
	onMeter          MeterHook
	vmManager        *firecracker.FireCrackerManager
	template         *firecracker.SnapshotTemplate
	idleTimeout      time.Duration
	maxLifetime      time.Duration
	vmConfig         firecracker.VMConfig
	sizePools        map[string]*firecracker.VMPool           // keyed by vmsize.Key — one pool per resource shape
	sizeTemplates    map[string]*firecracker.SnapshotTemplate // keyed by vmsize.Key — per-size baked device names (for pause/resume)
	pauseDir         string                                   // disk-backed dir for per-session pause snapshots (NOT /dev/shm)
	legacyPauseDir   string                                   // pause dir before it moved next to the writable disks
	manifestPath     string                                   // recovery manifest of paused sessions
	manifestMu       sync.Mutex                               // serializes concurrent pause manifest rewrites
	createLocksMu    sync.Mutex
	createLocks      map[string]*createLock
	meterMu          sync.Mutex
	meterLast        map[string]time.Time
	meterTotals      map[string]MeterSample
	maxTerminals     int
	checkpoints      CheckpointWriter
	checkpointReader CheckpointReader
}

type createLock struct {
	mu   sync.Mutex
	refs int
}

func NewManager(
	vmManager *firecracker.FireCrackerManager,
	template *firecracker.SnapshotTemplate,
	vmConfig firecracker.VMConfig,
	maxSessions int,
	idleTimeout time.Duration,
	maxLifetime time.Duration,
	sizePools map[string]*firecracker.VMPool,
	sizeTemplates map[string]*firecracker.SnapshotTemplate,
	onState StateHook,
	onMeter MeterHook,
) *Manager {
	m := &Manager{
		store:         NewStore(maxSessions),
		onState:       onState,
		onMeter:       onMeter,
		vmManager:     vmManager,
		template:      template,
		vmConfig:      vmConfig,
		idleTimeout:   idleTimeout,
		maxLifetime:   maxLifetime,
		sizePools:     sizePools,
		sizeTemplates: sizeTemplates,
		// Next to the writable disks: each pause's memory file starts as a reflink of the
		// previous one, which needs that reflink-capable filesystem.
		pauseDir:       filepath.Join(vmManager.WritableDiskRoot(), "pause"),
		legacyPauseDir: filepath.Join(vmManager.SocketDir, "pause"),
		manifestPath:   filepath.Join(vmManager.SocketDir, "paused-sessions.json"),
		createLocks:    make(map[string]*createLock),
		meterLast:      make(map[string]time.Time),
		meterTotals:    make(map[string]MeterSample),
		maxTerminals:   8,
	}
	if err := os.MkdirAll(m.pauseDir, 0o755); err != nil {
		slog.Error("failed to create pause snapshot dir", "dir", m.pauseDir, "err", err)
	}
	// Firecracker drops to FCUid and writes the snapshot files itself, so the dir must be
	// owned by that user (the backend runs as root and creates the dir).
	if vmManager.FCUid > 0 {
		os.Chown(m.pauseDir, vmManager.FCUid, vmManager.FCGid)
	}
	m.recoverPaused()
	for _, sess := range m.store.All() {
		m.meterLast[sess.ID] = time.Now()
	}
	m.cleanupOrphanedDisks()
	go m.reaper()
	if onMeter != nil {
		go m.meterTicker()
	}
	return m
}

func (m *Manager) SetMaxTerminalsPerSandbox(maxTerminals int) {
	if maxTerminals > 0 {
		m.maxTerminals = maxTerminals
	}
}

func (m *Manager) SetCheckpointWriter(writer CheckpointWriter) {
	m.checkpoints = writer
}

func (m *Manager) SetCheckpointReader(reader CheckpointReader) {
	m.checkpointReader = reader
}

// meterTicker accrues RAW resource-time for every live session once a minute and hands each
// delta to onMeter (which persists it to usage_meters). Compute (vCPU-s + RAM GB-s) accrues
// while ACTIVE; disk (GB-s) accrues while the sandbox exists (active OR paused). No cost is
// computed here; pricing is applied later at read time via pricing_rates.
func (m *Manager) meterTicker() {
	const interval = time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		for _, sess := range m.store.All() {
			sess.mu.Lock()
			m.meterSession(sess, sess.State, now)
			sess.mu.Unlock()
		}
	}
}

// meterSession records cumulative usage for the current UTC minute. Re-emitting
// the same bucket is safe because ingestion persists the maximum observed total.
// Callers that already hold sess.mu may call this directly.
func (m *Manager) meterSession(sess *Session, state SessionState, now time.Time) {
	if m.onMeter == nil || sess == nil || sess.UserID == "" {
		return
	}
	m.meterMu.Lock()
	last, ok := m.meterLast[sess.ID]
	if !ok {
		last = sess.CreatedAt
		if last.IsZero() || last.After(now) {
			last = now
		}
	}
	elapsed := now.Sub(last).Seconds()
	m.meterLast[sess.ID] = now
	if elapsed <= 0 {
		m.meterMu.Unlock()
		return
	}

	bucket := now.UTC().Truncate(time.Minute)
	total := m.meterTotals[sess.ID]
	if !total.Bucket.Equal(bucket) {
		total = MeterSample{
			UserID:       sess.UserID,
			SandboxID:    sess.ID,
			BillingModel: sess.BillingModel,
			Bucket:       bucket,
		}
	}
	total.DiskGBSeconds += float64(sess.DiskGB) * elapsed
	if state == StateActive {
		total.VCPUSeconds += float64(sess.VCPUs) * elapsed
		total.RAMGBSeconds += (float64(sess.MemoryMB) / 1024.0) * elapsed
	}
	m.meterTotals[sess.ID] = total
	m.meterMu.Unlock()
	m.onMeter(total)
}

func (m *Manager) forgetMeter(sessionID string) {
	m.meterMu.Lock()
	delete(m.meterLast, sessionID)
	delete(m.meterTotals, sessionID)
	m.meterMu.Unlock()
}

// notifyState persists a lifecycle transition before the worker acknowledges it.
func (m *Manager) notifyState(sess *Session, state string) error {
	if m.onState != nil && sess != nil {
		if err := m.onState(sess.ID, sess.UserID, state); err != nil {
			return fmt.Errorf("report session %s state %s: %w", sess.ID, state, err)
		}
	}
	return nil
}

// recoverPaused rebuilds the store's paused sessions from the on-disk manifest after a
// process restart. Sessions whose snapshot/disk files survived (reboot/deploy) come back
// as paused and resume on next access; those whose files are gone (host loss) are dropped.
func (m *Manager) recoverPaused() {
	sessions, err := readManifest(m.manifestPath)
	if err != nil {
		slog.Error("failed to read paused-session manifest", "path", m.manifestPath, "err", err)
		return
	}
	recovered := 0
	for _, sess := range sessions {
		if sess.RootfsPathAtPause == "" && m.template != nil {
			sess.RootfsPathAtPause = m.template.RootfsPath
		}
		if err := m.store.Add(sess); err != nil {
			slog.Warn("could not recover paused session", "session_id", sess.ID, "err", err)
			continue
		}
		recovered++
	}
	if recovered > 0 {
		slog.Info("recovered paused sessions from manifest", "count", recovered)
		m.persistManifest()
	}
}

// cleanupOrphanedDisks removes leaked per-VM writable disks and stale pause
// snapshots left behind by ungraceful VM/worker death — e.g. a `systemctl restart`
// kills running VMs without ever calling teardown, so their writable-*.ext4 (and
// any half-written pause snapshot) are never removed. Run ONCE at startup, AFTER
// recoverPaused: at that point no live/pool VMs exist yet, so the only files worth
// keeping are those owned by a recovered PAUSED session. Anything else is an orphan.
// Without this, orphans accumulate until the disk fills and pause snapshots fail
// with ENOSPC.
func (m *Manager) cleanupOrphanedDisks() {
	// Keep-set: writable disks + pause dirs we must NOT delete.
	keepDisk := make(map[string]bool)
	keepPause := make(map[string]bool)
	// (a) The template GOLDEN writable disks — every pool restore reflink-clones
	// from these, so deleting them breaks all restores (create + resume). They
	// live as writable-*.ext4 too, but belong to no session.
	if m.template != nil && m.template.WritableDiskPath != "" {
		keepDisk[m.template.WritableDiskPath] = true
	}
	for _, t := range m.sizeTemplates {
		if t != nil && t.WritableDiskPath != "" {
			keepDisk[t.WritableDiskPath] = true
		}
	}
	// (b) Disks + pause snapshots owned by recovered paused sessions.
	for _, s := range m.store.All() {
		if s.State != StatePaused {
			continue
		}
		if s.WritableDiskPath != "" {
			keepDisk[s.WritableDiskPath] = true
		}
		keepPause[s.ID] = true
	}

	// 1. Orphaned per-VM writable disks in the configured active disk store.
	removedDisks := 0
	writables, err := m.vmManager.ListWritableDisks(context.Background())
	if err != nil {
		slog.Warn("failed to list writable disks during startup cleanup", "err", err)
	}
	for _, p := range writables {
		if keepDisk[p] {
			continue
		}
		if err := m.vmManager.DeleteWritableDisk(context.Background(), p); err == nil {
			removedDisks++
		} else {
			slog.Warn("failed to remove orphaned writable disk", "path", p, "err", err)
		}
	}

	// 2. Stale pause snapshot dirs (pause/<session-id>) with no matching paused session.
	removedPauses := 0
	for _, dir := range []string{m.pauseDir, m.legacyPauseDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || keepPause[e.Name()] {
				continue
			}
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err == nil {
				removedPauses++
			}
		}
	}

	if removedDisks > 0 || removedPauses > 0 {
		slog.Info("cleaned orphaned VM disks + stale pause snapshots at startup",
			"writable_disks_removed", removedDisks,
			"pause_snapshots_removed", removedPauses,
			"kept_paused_sessions", len(keepPause))
	}
}

// persistManifest rewrites the recovery manifest from the current set of paused sessions.
func (m *Manager) persistManifest() {
	m.manifestMu.Lock()
	defer m.manifestMu.Unlock()

	var paused []*Session
	for _, s := range m.store.All() {
		if s.State == StatePaused {
			paused = append(paused, s)
		}
	}
	if err := writeManifest(m.manifestPath, paused); err != nil {
		slog.Error("failed to persist paused-session manifest", "err", err)
	}
}

// Create boots a VM and binds it to a new session. env vars are injected into
// the persistent shell so commands like git can access GITHUB_TOKEN etc.
func (m *Manager) Create(ctx context.Context, userID, billingModel, image string, env map[string]string, vcpus, memoryMB, diskGB int, internet bool, idleTimeout, maxLifetime time.Duration) (*Session, error) {
	return m.CreateWithID(ctx, uuid.NewString(), userID, billingModel, image, env, vcpus, memoryMB, diskGB, internet, idleTimeout, maxLifetime)
}

// CreateWithID creates a session under the control-plane-assigned UUID. Calls
// for the same UUID are serialized and idempotent, so a retry cannot boot a
// second VM or consume a second capacity reservation.
func (m *Manager) CreateWithID(ctx context.Context, sandboxID, userID, billingModel, image string, env map[string]string, vcpus, memoryMB, diskGB int, internet bool, idleTimeout, maxLifetime time.Duration) (*Session, error) {
	if _, err := uuid.Parse(sandboxID); err != nil {
		return nil, fmt.Errorf("invalid sandbox id %q: %w", sandboxID, err)
	}
	image, err := sandboximage.Normalize(image)
	if err != nil {
		return nil, err
	}
	unlock := m.lockCreate(sandboxID)
	defer unlock()

	if existing, ok := m.store.Get(sandboxID); ok {
		if existing.UserID != userID ||
			existing.BillingModel != billingModel ||
			existing.Image != image ||
			existing.VCPUs != vcpus ||
			existing.MemoryMB != memoryMB ||
			existing.DiskGB != diskGB ||
			existing.Internet != internet {
			return nil, fmt.Errorf("sandbox %s already exists with different configuration", sandboxID)
		}
		return existing, nil
	}

	t0 := time.Now()

	pool := m.sizePools[sandboximage.PoolKey(image, vmsize.Key(vcpus, memoryMB, diskGB))]
	if pool == nil {
		return nil, fmt.Errorf("no VM pool for image %q and size %dvcpu/%dMB/%dGB", image, vcpus, memoryMB, diskGB)
	}

	pvm, warm, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire VM: %w", err)
	}

	// Apply network policy on the VM's slot. When internet=false this blocks egress
	// in the slot's netns; it also normalizes any stale rule on a reused slot. If we
	// can't enforce a requested isolation, fail rather than hand back a leaky sandbox.
	if err := m.vmManager.SetSlotEgress(pvm.VM.Slot, internet); err != nil {
		pool.Release(pvm)
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	sess := &Session{
		ID:           sandboxID,
		UserID:       userID,
		Image:        image,
		VM:           pvm.VM,
		Cgroup:       pvm.Cgroup,
		PooledVM:     pvm,
		Pool:         pool,
		BillingModel: billingModel,
		VCPUs:        vcpus,
		MemoryMB:     memoryMB,
		DiskGB:       diskGB,
		Env:          env,
		Internet:     internet,
		IdleTimeout:  idleTimeout,
		MaxLifetime:  maxLifetime,
		CreatedAt:    time.Now(),
		LastUsed:     time.Now(),
		State:        StateActive,
		Terminals:    make(map[string]struct{}),
	}
	if err := m.store.Add(sess); err != nil {
		pool.Release(pvm)
		return nil, err
	}
	m.meterMu.Lock()
	m.meterLast[sess.ID] = sess.CreatedAt
	m.meterMu.Unlock()

	// Hold ONE persistent vsock connection for the whole session, reused for every exec/run.
	// A snapshot-restored VM accepts exactly one vsock connection, so per-connection execs
	// don't work — the persistent connection is the correct (and only) model. Pause closes
	// it (agent returns to accept, snapshot captures that state); resume opens a fresh one.
	vsockClient := firecracker.NewVsockClient(pvm.VM.VsockPath)
	conn, err := vsockClient.Connect()
	if err != nil {
		pool.Release(pvm)
		m.store.Delete(sess.ID)
		return nil, fmt.Errorf("vsock connect failed: %w", err)
	}
	sess.VsockConn = conn

	// Inject env vars into the VM's persistent shell if provided
	if len(env) > 0 {
		if err := vsockClient.SetEnvOnConn(conn, env); err != nil {
			slog.Warn("set_env failed", "session_id", sess.ID, "err", err)
		}
	}

	slog.Info("session created", "session_id", sess.ID, "vm_id", pvm.VM.ID, "billing_model", billingModel, "warm", warm, "ms", time.Since(t0).Milliseconds())
	m.startLogs(sess.ID, sess.VM.VsockPath)
	return sess, nil
}

func (m *Manager) OpenTerminal(ctx context.Context, sessionID, terminalID, shell string, columns, rows uint16) error {
	return m.OpenTerminalWithEnv(ctx, sessionID, terminalID, shell, columns, rows, nil)
}

// OpenTerminalWithEnv is OpenTerminal with extra environment for this
// terminal's shell, over the sandbox's own env (a terminal command's startup,
// see terminal.Command.StartupEnv). The sandbox's env is not changed.
func (m *Manager) OpenTerminalWithEnv(ctx context.Context, sessionID, terminalID, shell string, columns, rows uint16, env map[string]string) error {
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.State != StateActive || sess.VM == nil || sess.VsockConn == nil {
		return fmt.Errorf("session %s is not active", sessionID)
	}
	if _, exists := sess.Terminals[terminalID]; exists {
		return fmt.Errorf("terminal %s already exists", terminalID)
	}
	if len(sess.Terminals) >= m.maxTerminals {
		return fmt.Errorf("terminal limit reached (%d)", m.maxTerminals)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	terminalEnv := sess.Env
	if len(env) > 0 {
		terminalEnv = make(map[string]string, len(sess.Env)+len(env))
		maps.Copy(terminalEnv, sess.Env)
		maps.Copy(terminalEnv, env)
	}
	client := firecracker.NewVsockClient(sess.VM.VsockPath)
	if err := client.OpenTerminalOnConn(sess.VsockConn, firecracker.TerminalOpenRequest{
		TerminalID: terminalID,
		Hostname:   sessionID,
		Shell:      shell,
		Columns:    columns,
		Rows:       rows,
		Env:        terminalEnv,
	}); err != nil {
		return err
	}
	if sess.Terminals == nil {
		sess.Terminals = make(map[string]struct{})
	}
	sess.Terminals[terminalID] = struct{}{}
	sess.LastUsed = time.Now()
	return nil
}

func (m *Manager) CloseTerminal(ctx context.Context, sessionID, terminalID string) error {
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.State != StateActive || sess.VM == nil || sess.VsockConn == nil {
		return fmt.Errorf("session %s is not active", sessionID)
	}
	if _, exists := sess.Terminals[terminalID]; !exists {
		return fmt.Errorf("terminal %s not found", terminalID)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	client := firecracker.NewVsockClient(sess.VM.VsockPath)
	err := client.CloseTerminalOnConn(sess.VsockConn, terminalID)
	delete(sess.Terminals, terminalID)
	return err
}

func (m *Manager) AttachTerminal(ctx context.Context, sessionID, terminalID string, stream terminal.Stream) error {
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	sess.mu.Lock()
	if sess.State != StateActive || sess.VM == nil || sess.VsockConn == nil {
		sess.mu.Unlock()
		return fmt.Errorf("session %s is not active", sessionID)
	}
	if _, exists := sess.Terminals[terminalID]; !exists {
		sess.mu.Unlock()
		return fmt.Errorf("terminal %s not found", terminalID)
	}
	vsockPath := sess.VM.VsockPath
	sess.LastUsed = time.Now()
	sess.mu.Unlock()

	return firecracker.NewVsockClient(vsockPath).AttachTerminal(ctx, terminalID, stream)
}

func (m *Manager) lockCreate(sandboxID string) func() {
	m.createLocksMu.Lock()
	lock := m.createLocks[sandboxID]
	if lock == nil {
		lock = &createLock{}
		m.createLocks[sandboxID] = lock
	}
	lock.refs++
	m.createLocksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.createLocksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(m.createLocks, sandboxID)
		}
		m.createLocksMu.Unlock()
	}
}

// ErrNotActive means the session is not running and the command asked not to
// resume it (ExecOptions.ActiveOnly).
var ErrNotActive = errors.New("session is not active")

// ExecOptions changes how ExecWithOptions treats the session's lifecycle and
// the command's output. The zero value is Exec's behavior.
type ExecOptions struct {
	// ActiveOnly fails with ErrNotActive instead of resuming a paused session.
	ActiveOnly bool
	// Passive does not count the command as activity: LastUsed (the idle
	// timer), RunCount and LastExitCode are left alone.
	Passive bool
	// Quiet asks the guest to keep the command's output out of the sandbox's
	// log stream; the output is still returned. Guests built before this
	// option ignore it.
	Quiet bool
}

// Exec runs a shell command in the session's persistent bash process
func (m *Manager) Exec(ctx context.Context, sessionID, command string, timeoutSec int) (executor.ExecutionResult, error) {
	return m.ExecWithOptions(ctx, sessionID, command, timeoutSec, ExecOptions{})
}

// ExecQuiet is Exec for platform-internal commands (the IDE's): the guest
// keeps the command's output out of the sandbox's log stream.
func (m *Manager) ExecQuiet(ctx context.Context, sessionID, command string, timeoutSec int) (executor.ExecutionResult, error) {
	return m.ExecWithOptions(ctx, sessionID, command, timeoutSec, ExecOptions{Quiet: true})
}

// ExecWithOptions is Exec with exec options (see ExecOptions).
func (m *Manager) ExecWithOptions(ctx context.Context, sessionID, command string, timeoutSec int, opts ExecOptions) (executor.ExecutionResult, error) {
	if !opts.ActiveOnly {
		// Transparently resume a paused session before running.
		if err := m.ensureActive(ctx, sessionID); err != nil {
			return executor.ExecutionResult{}, fmt.Errorf("resume session: %w", err)
		}
	}

	sess, ok := m.store.Get(sessionID)
	if !ok {
		return executor.ExecutionResult{}, fmt.Errorf("session %s not found", sessionID)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	// Checked under the lock: a pause that finished while this command waited
	// leaves no VM to run it on.
	if opts.ActiveOnly && (sess.State != StateActive || sess.VM == nil || sess.VsockConn == nil) {
		return executor.ExecutionResult{}, fmt.Errorf("%w: session %s is %s", ErrNotActive, sessionID, sess.State)
	}

	if !opts.Passive {
		sess.LastUsed = time.Now()
	}

	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	vsockClient := firecracker.NewVsockClient(sess.VM.VsockPath)
	resp, err := vsockClient.ExecOnConnWithOptions(sess.VsockConn, command, timeoutSec, firecracker.ExecOptions{Quiet: opts.Quiet})
	if err != nil {
		return executor.ExecutionResult{}, fmt.Errorf("exec failed: %w", err)
	}

	if !opts.Passive {
		sess.RunCount++
		exitCode := resp.ExitCode
		sess.LastExitCode = &exitCode
	}

	return executor.ExecutionResult{
		OutputLogsCaptured: m.logsEnabled(),
		Stdout:             resp.Stdout,
		Stderr:             resp.Stderr,
		Duration:           resp.Duration,
		GuestDuration:      resp.Duration,
		ExitCode:           int64(resp.ExitCode),
		TerminationReason:  "success",
	}, nil
}

// Execute runs code inside an existing session's VM
func (m *Manager) Execute(ctx context.Context, sessionID, code, language string, timeoutSec int) (executor.ExecutionResult, error) {
	// Transparently resume a paused session before running.
	if err := m.ensureActive(ctx, sessionID); err != nil {
		return executor.ExecutionResult{}, fmt.Errorf("resume session: %w", err)
	}

	sess, ok := m.store.Get(sessionID)
	if !ok {
		return executor.ExecutionResult{}, fmt.Errorf("session %s not found", sessionID)
	}

	// serialize concurrent calls on the same session
	sess.mu.Lock()
	defer sess.mu.Unlock()

	sess.LastUsed = time.Now()

	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	vsockClient := firecracker.NewVsockClient(sess.VM.VsockPath)
	resp, err := vsockClient.ExecuteOnConn(sess.VsockConn, code, language, "stateful", timeoutSec)
	if err != nil {
		return executor.ExecutionResult{}, fmt.Errorf("execution failed: %w", err)
	}

	sess.RunCount++
	sess.TotalExecutionMs += resp.Duration * 1000
	exitCode := resp.ExitCode
	sess.LastExitCode = &exitCode

	return executor.ExecutionResult{
		OutputLogsCaptured: m.logsEnabled(),
		Stdout:             resp.Stdout,
		Stderr:             resp.Stderr,
		Duration:           resp.Duration,
		GuestDuration:      resp.Duration,
		ExitCode:           int64(resp.ExitCode),
		TerminationReason:  "success",
	}, nil
}

// Destroy tears down a session. For an active session this releases its VM; for a paused
// session there is no live VM, so it just deletes the on-disk snapshot + writable disk.
func (m *Manager) Destroy(ctx context.Context, sessionID string) error {
	m.stopLogs(sessionID)
	// Remove and revoke together under slotBindMu so a concurrent BindSlotGrant
	// either binds first (and is revoked here) or sees the session gone.
	m.slotBindMu.Lock()
	sess, ok := m.store.Delete(sessionID)
	m.revokeSlotGrants(sessionID)
	m.slotBindMu.Unlock()
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	m.meterSession(sess, sess.State, time.Now())

	if sess.State == StatePaused {
		m.cleanupPausedFiles(sess)
		m.purgeCheckpoints(sessionID)
		m.persistManifest()
		if err := m.notifyState(sess, "destroyed"); err != nil {
			return err
		}
		m.forgetMeter(sessionID)
		slog.Info("paused session destroyed", "session_id", sessionID)
		return nil
	}

	if sess.VsockConn != nil && len(sess.Terminals) > 0 {
		client := firecracker.NewVsockClient(sess.VM.VsockPath)
		if err := client.CloseAllTerminalsOnConn(sess.VsockConn); err != nil {
			slog.Warn("close terminals before destroy failed", "session_id", sessionID, "err", err)
		}
		sess.Terminals = nil
	}
	if sess.VsockConn != nil {
		sess.VsockConn.Close()
	}

	if sess.PooledVM != nil && sess.Pool != nil {
		// pool handles cgroup destruction and replenishment
		sess.Pool.Release(sess.PooledVM)
	} else {
		if sess.Cgroup != nil {
			sess.Cgroup.Destroy()
		}
		if sess.VM != nil {
			if err := m.vmManager.Destroy(ctx, sess.VM.ID); err != nil {
				return fmt.Errorf("failed to destroy VM: %w", err)
			}
		}
	}
	// A sandbox paused and resumed earlier still has its memory file locally and
	// checkpoints in Blob.
	m.removePauseDirs(sessionID)
	m.purgeCheckpoints(sessionID)

	if err := m.notifyState(sess, "destroyed"); err != nil {
		return err
	}
	m.forgetMeter(sessionID)
	slog.Info("session destroyed", "session_id", sessionID)
	return nil
}

// Pause snapshots an active session's memory to disk and releases its VM, network slot,
// RAM and cgroup — keeping its writable disk. The session stays in the store as paused
// and resumes transparently on next use. Idempotent: a non-active session is a no-op.
func (m *Manager) Pause(ctx context.Context, sessionID string) error {
	return m.pause(ctx, sessionID, time.Time{})
}

// pause performs a manual pause when idleCheckAt is zero. For an automatic pause,
// it rechecks idleness while holding the session lock so new work cannot race the reaper.
func (m *Manager) pause(ctx context.Context, sessionID string, idleCheckAt time.Time) error {
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}

	if m.template == nil {
		return fmt.Errorf("cannot pause session %s: no snapshot template (cold-boot mode)", sessionID)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !idleCheckAt.IsZero() {
		idleDeadline := sess.LastUsed.Add(sess.IdleTimeout)
		if sess.State != StateActive || sess.IdleTimeout <= 0 || idleCheckAt.Before(idleDeadline) || len(sess.Terminals) > 0 {
			return nil
		}
	}
	if sess.State == StatePaused {
		return m.notifyState(sess, "paused")
	}
	if sess.State != StateActive || sess.VM == nil {
		return nil
	}
	if sess.VsockConn != nil && len(sess.Terminals) > 0 {
		client := firecracker.NewVsockClient(sess.VM.VsockPath)
		if err := client.CloseAllTerminalsOnConn(sess.VsockConn); err != nil {
			return fmt.Errorf("close terminals before pause: %w", err)
		}
		sess.Terminals = nil
	}
	m.stopLogs(sessionID)
	// The slot is released on pause; a grant must never outlive it. The runner
	// re-grants after resume if it still needs the repository.
	m.revokeSlotGrants(sessionID)

	t0 := time.Now()
	snapDir := filepath.Join(m.pauseDir, sessionID)
	defer func() {
		if sess.State == StateActive && sess.VM != nil {
			m.startLogs(sess.ID, sess.VM.VsockPath)
		}
	}()
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return fmt.Errorf("create pause dir: %w", err)
	}
	// The Firecracker process (fcvm) writes the snapshot files, so it must own the dir.
	if m.vmManager.FCUid > 0 {
		os.Chown(snapDir, m.vmManager.FCUid, m.vmManager.FCGid)
	}
	snapPath := filepath.Join(snapDir, "snap")
	// A new name each pause: the previous memory file still backs the running VM and is
	// the base this one is built from.
	memPath := filepath.Join(snapDir, "mem-"+uuid.NewString())

	// Capture resume info before tearing the VM down. The session snapshot bakes the
	// device paths the VM was restored with (Firecracker carries the vsock UDS path + TAP
	// name forward across restore — not the per-VM runtime slot names), so resume must
	// restore against those. Take them from the VM itself: the current template's names
	// differ after a template rebuild (e.g. kernel rollout), and a resumed session still
	// carries its original ones. Persisted in the manifest for recovery.
	vmID := sess.VM.ID
	if sess.VM.SnapshotVsockPath != "" {
		sess.VsockPathAtPause = sess.VM.SnapshotVsockPath
		sess.TapNameAtPause = sess.VM.SnapshotTapName
	} else {
		// Fallback for a VM without recorded restore paths: the session's own size template.
		tmpl := m.sizeTemplates[sandboximage.PoolKey(sess.Image, vmsize.Key(sess.VCPUs, sess.MemoryMB, sess.DiskGB))]
		if tmpl == nil {
			tmpl = m.template
		}
		sess.VsockPathAtPause = tmpl.VsockPath
		sess.TapNameAtPause = tmpl.TapName
	}
	sess.WritableDiskPath = sess.VM.WritableDiskPath
	sess.RootfsPathAtPause = sess.VM.Config.RootfsPath

	// Close the persistent connection and let the guest agent fall back into accept()
	// BEFORE we freeze the VM. On a live (not-yet-frozen) VM the close propagates normally,
	// so the agent's blocked read returns and it loops back to accept() — the state a
	// snapshot restores cleanly from. (We must snapshot with the agent in accept(): a VM
	// captured mid-read can't be talked to after resume.)
	if sess.VsockConn != nil {
		sess.VsockConn.Close()
		sess.VsockConn = nil
	}
	time.Sleep(400 * time.Millisecond)

	// Snapshot: pauses the vCPUs and writes VM state + the full RAM file to disk (NOT
	// /dev/shm), writing only the memory that changed since the VM's base memory file.
	previousMem := sess.VM.MemBasePath
	snapshotStart := time.Now()
	if err := m.vmManager.SnapshotMemory(ctx, vmID, snapPath, memPath); err != nil {
		return fmt.Errorf("snapshot for pause: %w", err)
	}
	snapshotTime := time.Since(snapshotStart)
	// memPath now holds all of the VM's memory; the previous pause's file is not needed.
	if previousMem != "" && previousMem != memPath && ownedBySession(previousMem, sessionID) {
		if err := os.Remove(previousMem); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("remove previous pause memory", "session_id", sessionID, "path", previousMem, "err", err)
		}
	}
	checkpointStart := time.Now()
	if m.checkpoints != nil {
		checkpointRef, err := m.checkpoints.Save(ctx, checkpoint.Input{
			SandboxID:        sessionID,
			VCPUs:            sess.VCPUs,
			MemoryMB:         sess.MemoryMB,
			DiskGB:           sess.DiskGB,
			RootfsPath:       sess.RootfsPathAtPause,
			VsockPath:        sess.VsockPathAtPause,
			TapName:          sess.TapNameAtPause,
			SnapshotPath:     snapPath,
			MemoryPath:       memPath,
			WritableDiskPath: sess.WritableDiskPath,
		})
		if err != nil {
			return m.rollbackFailedCheckpoint(ctx, sess, vmID, snapPath, memPath, err)
		}
		sess.CheckpointRef = checkpointRef
	}
	checkpointTime := time.Since(checkpointStart)
	m.meterSession(sess, StateActive, time.Now())

	// Detach from the pool so its capacity slot is freed, tear the VM down keeping the
	// writable disk, then drop the (now empty) cgroup.
	if sess.Pool != nil {
		sess.Pool.Forget(vmID)
	}
	if err := m.vmManager.TeardownKeepDisk(ctx, vmID); err != nil {
		slog.Error("pause teardown failed", "session_id", sessionID, "err", err)
	}
	if sess.Cgroup != nil {
		sess.Cgroup.Destroy()
	}

	sess.SnapPath = snapPath
	sess.MemPath = memPath
	sess.BootIDAtPause = bootID()
	sess.State = StatePaused
	sess.PausedAt = time.Now()
	sess.VM = nil
	sess.VsockConn = nil
	sess.PooledVM = nil
	sess.Pool = nil
	sess.Cgroup = nil

	m.persistManifest()
	if err := m.notifyState(sess, "paused"); err != nil {
		return err
	}
	slog.Info("session paused", "session_id", sessionID, "ms", time.Since(t0).Milliseconds(),
		"snapshot_ms", snapshotTime.Milliseconds(), "checkpoint_ms", checkpointTime.Milliseconds())
	return nil
}

func (m *Manager) rollbackFailedCheckpoint(ctx context.Context, sess *Session, vmID, snapPath, memPath string, uploadErr error) error {
	if err := m.vmManager.ResumeLive(ctx, vmID); err != nil {
		return fmt.Errorf("durable checkpoint failed: %v; resume live VM: %w", uploadErr, err)
	}
	time.Sleep(500 * time.Millisecond)
	client := firecracker.NewVsockClient(sess.VM.VsockPath)
	conn, err := client.Connect()
	if err != nil {
		return fmt.Errorf("durable checkpoint failed: %v; reconnect resumed VM: %w", uploadErr, err)
	}
	if len(sess.Env) > 0 {
		if err := client.SetEnvOnConn(conn, sess.Env); err != nil {
			conn.Close()
			return fmt.Errorf("durable checkpoint failed: %v; restore resumed VM environment: %w", uploadErr, err)
		}
	}
	sess.VsockConn = conn
	// memPath stays: the snapshot reset Firecracker's dirty tracking, so it is the
	// running VM's base for its next pause.
	os.Remove(snapPath)
	return fmt.Errorf("durable checkpoint failed; sandbox resumed: %w", uploadErr)
}

// Resume restores a paused session from its own snapshot, reattaching its writable disk
// in place into a fresh slot. Idempotent: an already-active session is a no-op.
func (m *Manager) Resume(ctx context.Context, sessionID string) error {
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.State == StateActive {
		return m.notifyState(sess, "active")
	}
	if !fileExists(sess.SnapPath) || !fileExists(sess.MemPath) || !fileExists(sess.WritableDiskPath) {
		if err := m.hydrateCheckpoint(ctx, sess); err != nil {
			return fmt.Errorf("restore durable checkpoint: %w", err)
		}
	}

	t0 := time.Now()
	rootfsPath := sess.RootfsPathAtPause
	if rootfsPath == "" {
		rootfsPath = m.template.RootfsPath
	}
	tmpl := &firecracker.SnapshotTemplate{
		SnapPath:         sess.SnapPath,
		MemPath:          sess.MemPath,
		RootfsPath:       rootfsPath,
		WritableDiskPath: sess.WritableDiskPath, // the session's own disk, attached in place
		VsockPath:        sess.VsockPathAtPause,
		TapName:          sess.TapNameAtPause,
	}

	cfg := m.vmConfig

	vm, err := m.vmManager.ResumeFromSnapshot(ctx, cfg, tmpl)
	if err != nil {
		return fmt.Errorf("resume from snapshot: %w", err)
	}
	msRestore := time.Since(t0).Milliseconds()

	if err := m.vmManager.SetSlotEgress(vm.Slot, sess.Internet); err != nil {
		if cleanupErr := m.vmManager.TeardownKeepDisk(ctx, vm.ID); cleanupErr != nil {
			slog.Error("failed to clean up resumed VM after network policy failure", "vm_id", vm.ID, "err", cleanupErr)
		}
		return fmt.Errorf("re-apply network policy on resume: %w", err)
	}

	// Recreate the cgroup for the new VM process, sized from the session's own resources
	// (matches the pool's per-size session cgroup on the original create).
	sz := vmsize.Size{VCPUs: sess.VCPUs, MemoryMB: sess.MemoryMB, DiskGB: sess.DiskGB}
	cgCfg := cgroup.Config{
		CPUQuotaUS:  sz.CgroupCPUQuotaUS(),
		CPUPeriodUS: vmsize.CgroupCPUPeriodUS,
		MemMaxBytes: sz.CgroupMemMaxBytes(),
	}
	var cg *cgroup.Cgroup
	if vm.Process != nil && vm.Process.Process != nil {
		cg, err = cgroup.New("default", vm.ID, cgCfg)
		if err != nil {
			slog.Warn("failed to create cgroup on resume", "vm_id", vm.ID, "err", err)
		} else if err = cg.AddPID(vm.Process.Process.Pid); err != nil {
			slog.Warn("failed to add pid to cgroup on resume", "vm_id", vm.ID, "err", err)
			cg.Destroy()
			cg = nil
		}
	}
	// Let the guest agent re-initialize its vsock listener after the restore (accept()
	// breaks on restore → the agent rebuilds its listener) before we open the connection.
	time.Sleep(500 * time.Millisecond)
	msPreConnect := time.Since(t0).Milliseconds()

	// Open a fresh persistent connection — this is the ONE vsock connection the restored VM
	// allows, reused for every subsequent exec/run (its CONNECT waits for the agent to accept).
	vsockClient := firecracker.NewVsockClient(vm.VsockPath)
	conn, err := vsockClient.Connect()
	msConnect := time.Since(t0).Milliseconds()
	if err != nil {
		if cg != nil {
			cg.Destroy()
		}
		if cleanupErr := m.vmManager.TeardownKeepDisk(ctx, vm.ID); cleanupErr != nil {
			slog.Error("failed to clean up resumed VM after vsock connection failure", "vm_id", vm.ID, "err", cleanupErr)
		}
		return fmt.Errorf("vsock connect on resume: %w", err)
	}

	// Reset all stateful runtimes so in-memory interpreter state is consistently cleared on
	// pause across every language (a snapshot restore can also leave ZMQ Python kernels
	// degraded). Then re-inject the session's env so config still persists across the pause.
	resetStarted := time.Now()
	slog.Info("guest runtime reset started", "session_id", sessionID, "vm_id", vm.ID)
	if err := vsockClient.ResetRuntimesOnConn(conn); err != nil {
		slog.Error("guest runtime reset failed", "session_id", sessionID, "vm_id", vm.ID, "duration_ms", time.Since(resetStarted).Milliseconds(), "err", err)
		_ = conn.Close()
		if cg != nil {
			cg.Destroy()
		}
		if cleanupErr := m.vmManager.TeardownKeepDisk(ctx, vm.ID); cleanupErr != nil {
			slog.Error("failed to clean up resumed VM after guest readiness failure", "vm_id", vm.ID, "err", cleanupErr)
		} else {
			slog.Error("guest runtime reset console", "session_id", sessionID, "vm_id", vm.ID, "stdout", logTail(vm.Stdout.String()), "stderr", logTail(vm.Stderr.String()))
		}
		return fmt.Errorf("verify guest agent on resume: %w", err)
	}
	slog.Info("guest runtime reset completed", "session_id", sessionID, "vm_id", vm.ID, "duration_ms", time.Since(resetStarted).Milliseconds())
	msReset := time.Since(t0).Milliseconds()
	if len(sess.Env) > 0 {
		if err := vsockClient.SetEnvOnConn(conn, sess.Env); err != nil {
			_ = conn.Close()
			if cg != nil {
				cg.Destroy()
			}
			if cleanupErr := m.vmManager.TeardownKeepDisk(ctx, vm.ID); cleanupErr != nil {
				slog.Error("failed to clean up resumed VM after environment restore failure", "vm_id", vm.ID, "err", cleanupErr)
			}
			return fmt.Errorf("restore guest environment on resume: %w", err)
		}
	}
	msSetEnv := time.Since(t0).Milliseconds()

	sess.VM = vm
	sess.Cgroup = cg
	sess.VsockConn = conn
	sess.PooledVM = nil // resumed VM is standalone; Destroy releases its slot + disk directly
	sess.Pool = nil
	m.meterSession(sess, StatePaused, time.Now())
	sess.State = StateActive
	sess.LastUsed = time.Now()
	m.startLogs(sess.ID, vm.VsockPath)

	// The snap file is consumed by the restore. The mem file stays: it backs the VM's
	// memory and is the base its next pause builds on (deleted then, or on destroy).
	os.Remove(sess.SnapPath)
	sess.SnapPath = ""
	sess.MemPath = ""

	m.persistManifest()
	if err := m.notifyState(sess, "active"); err != nil {
		return err
	}
	// Per-phase timing (cumulative ms from t0) to locate slow resumes, esp. cold restore
	// after a process restart. connect_ms - preconnect_ms is the vsock CONNECT wait.
	slog.Info("session resumed", "session_id", sessionID, "vm_id", vm.ID,
		"ms", time.Since(t0).Milliseconds(),
		"restore_ms", msRestore, "preconnect_ms", msPreConnect,
		"connect_ms", msConnect, "reset_ms", msReset, "setenv_ms", msSetEnv)
	return nil
}

func (m *Manager) hydrateCheckpoint(ctx context.Context, sess *Session) error {
	if m.checkpointReader == nil {
		return fmt.Errorf("local checkpoint artifacts are missing and durable checkpoint reads are disabled")
	}
	pauseDir := filepath.Join(m.pauseDir, sess.ID)
	// Firecracker (FCUid) opens the restored files, so it must be able to enter the
	// dir; the restore would otherwise create it root-only.
	if err := os.MkdirAll(pauseDir, 0o755); err != nil {
		return fmt.Errorf("create pause dir: %w", err)
	}
	if m.vmManager.FCUid > 0 {
		if err := os.Chown(pauseDir, m.vmManager.FCUid, m.vmManager.FCGid); err != nil {
			return fmt.Errorf("set Firecracker ownership on %s: %w", pauseDir, err)
		}
	}
	paths := checkpoint.RestorePaths{
		Snapshot:     filepath.Join(pauseDir, "snap"),
		Memory:       filepath.Join(pauseDir, "mem"),
		WritableDisk: filepath.Join(m.vmManager.WritableDiskRoot(), "writable-"+sess.ID+".ext4"),
	}
	result, err := m.checkpointReader.Restore(ctx, sess.ID, sess.CheckpointRef, paths)
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = os.Remove(paths.Snapshot)
		_ = os.Remove(paths.Memory)
		_ = os.Remove(paths.WritableDisk)
	}
	if (sess.VCPUs != 0 && result.Resources.VCPUs != sess.VCPUs) ||
		(sess.MemoryMB != 0 && result.Resources.MemoryMB != sess.MemoryMB) ||
		(sess.DiskGB != 0 && result.Resources.DiskGB != sess.DiskGB) {
		cleanup()
		return fmt.Errorf("checkpoint resource metadata does not match session")
	}
	if m.vmManager.FCUid > 0 {
		for _, name := range []string{paths.Snapshot, paths.Memory, paths.WritableDisk} {
			if err := os.Chown(name, m.vmManager.FCUid, m.vmManager.FCGid); err != nil {
				cleanup()
				return fmt.Errorf("set Firecracker ownership on %s: %w", name, err)
			}
		}
	}
	sess.SnapPath = paths.Snapshot
	sess.MemPath = paths.Memory
	sess.WritableDiskPath = paths.WritableDisk
	sess.RootfsPathAtPause = result.Resume.RootfsPath
	sess.VsockPathAtPause = result.Resume.VsockPath
	sess.TapNameAtPause = result.Resume.TapName
	sess.CheckpointRef = result.ManifestKey
	m.persistManifest()
	return nil
}

// ensureActive resumes the session if it is paused so callers transparently get a live VM.
func (m *Manager) ensureActive(ctx context.Context, sessionID string) error {
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	if sess.State == StatePaused {
		return m.Resume(ctx, sessionID)
	}
	return nil
}

// cleanupPausedFiles deletes a paused session's on-disk artifacts (snapshot + writable disk).
// purgeCheckpoints deletes a destroyed sandbox's durable checkpoints in the
// background, retrying transient object-storage errors, so Destroy never waits
// on Blob. A failure that outlasts the retries is logged for cleanup.
func (m *Manager) purgeCheckpoints(sessionID string) {
	deleter, ok := m.checkpoints.(CheckpointDeleter)
	if !ok {
		return
	}
	go func() {
		delays := []time.Duration{0, 10 * time.Second, time.Minute, 5 * time.Minute}
		for attempt, delay := range delays {
			time.Sleep(delay)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			deleted, err := deleter.DeleteSandbox(ctx, sessionID)
			cancel()
			if err == nil {
				if deleted > 0 {
					slog.Info("sandbox checkpoints deleted", "session_id", sessionID, "objects", deleted)
				}
				return
			}
			slog.Warn("delete sandbox checkpoints failed", "session_id", sessionID, "attempt", attempt+1, "err", err)
		}
		slog.Error("sandbox checkpoints left in object storage", "session_id", sessionID)
	}()
}

func (m *Manager) cleanupPausedFiles(sess *Session) {
	if sess.WritableDiskPath != "" {
		if err := m.vmManager.DeleteWritableDisk(context.Background(), sess.WritableDiskPath); err != nil {
			slog.Warn("failed to delete paused writable disk", "session_id", sess.ID, "path", sess.WritableDiskPath, "err", err)
		}
	}
	m.removePauseDirs(sess.ID)
}

// removePauseDirs deletes a session's pause snapshot dir in both pause locations.
func (m *Manager) removePauseDirs(sessionID string) {
	if sessionID == "" {
		return
	}
	for _, dir := range []string{m.pauseDir, m.legacyPauseDir} {
		if err := os.RemoveAll(filepath.Join(dir, sessionID)); err != nil {
			slog.Warn("remove pause dir", "session_id", sessionID, "dir", dir, "err", err)
		}
	}
}

// ownedBySession reports whether path is in the session's pause dir (pause/<id>/),
// as opposed to a template's memory file shared by many VMs.
func ownedBySession(path, sessionID string) bool {
	return sessionID != "" && filepath.Base(filepath.Dir(path)) == sessionID
}

// PauseAllActive snapshots active sessions with bounded concurrency. It returns
// every failure so a drain controller can refuse a destructive scale-in.
func (m *Manager) PauseAllActive(ctx context.Context, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}

	var sessionIDs []string
	for _, sess := range m.store.All() {
		if sess.State == StateActive {
			sessionIDs = append(sessionIDs, sess.ID)
		}
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	if concurrency > len(sessionIDs) {
		concurrency = len(sessionIDs)
	}

	jobs := make(chan string)
	failures := make(chan error, len(sessionIDs))
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case sessionID, ok := <-jobs:
					if !ok {
						return
					}
					if err := m.Pause(ctx, sessionID); err != nil {
						slog.Error("graceful pause failed", "session_id", sessionID, "err", err)
						failures <- fmt.Errorf("pause session %s: %w", sessionID, err)
					}
				}
			}
		}()
	}

schedule:
	for _, sessionID := range sessionIDs {
		select {
		case jobs <- sessionID:
		case <-ctx.Done():
			break schedule
		}
	}
	close(jobs)
	workers.Wait()
	close(failures)

	var result []error
	for err := range failures {
		result = append(result, err)
	}
	if err := ctx.Err(); err != nil {
		result = append(result, fmt.Errorf("graceful pause deadline: %w", err))
	}
	return errors.Join(result...)
}

func (m *Manager) GetSession(id string) (*Session, bool) {
	return m.store.Get(id)
}

// ResolvePortTarget returns a consistent network-slot snapshot for a live
// sandbox. A lifecycle transition immediately afterward may still make the dial fail.
func (m *Manager) ResolvePortTarget(id string) (int, error) {
	sess, ok := m.store.Get(id)
	if !ok {
		return 0, fmt.Errorf("session %s not found", id)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.State != StateActive || sess.VM == nil || sess.VM.Slot < 0 {
		return 0, fmt.Errorf("session %s is not active", id)
	}
	return sess.VM.Slot, nil
}

// Sessions returns a point-in-time view used to rebuild worker-local admission
// state after a process restart.
func (m *Manager) Sessions() []*Session {
	return m.store.All()
}

func (m *Manager) Stats() map[string]int {
	active := 0
	for _, sess := range m.store.All() {
		if sess.State == StateActive {
			active++
		}
	}
	return map[string]int{
		"active_sessions": active,
		"total_sessions":  m.store.Count(),
	}
}

type reapAction uint8

const (
	reapNone reapAction = iota
	reapPause
	reapDestroy
)

// reapActionFor is called with sess.mu held.
func reapActionFor(sess *Session, now time.Time) reapAction {
	if sess.MaxLifetime > 0 && !now.Before(sess.CreatedAt.Add(sess.MaxLifetime)) {
		return reapDestroy
	}

	switch sess.State {
	case StateActive:
		if sess.IdleTimeout > 0 && len(sess.Terminals) == 0 && !now.Before(sess.LastUsed.Add(sess.IdleTimeout)) {
			return reapPause
		}
	case StatePaused:
		if !now.Before(sess.PausedAt.Add(pauseTTL)) {
			return reapDestroy
		}
	}
	return reapNone
}

func (m *Manager) reapOnce(ctx context.Context, now time.Time) {
	for _, sess := range m.store.All() {
		sess.mu.Lock()
		action := reapActionFor(sess, now)
		sessionID := sess.ID
		sess.mu.Unlock()

		switch action {
		case reapPause:
			if err := m.pause(ctx, sessionID, now); err != nil {
				slog.Error("reaper auto-pause failed", "session_id", sessionID, "err", err)
			}
		case reapDestroy:
			if err := m.Destroy(ctx, sessionID); err != nil {
				slog.Error("reaper destroy failed", "session_id", sessionID, "err", err)
			}
		}
	}
}

// reaper periodically PAUSES idle active sessions (freeing RAM/slot while
// keeping state on disk), destroys sessions past their max lifetime, and hard-deletes
// paused sessions past the retention TTL.
func (m *Manager) reaper() {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for range ticker.C {
		m.reapOnce(context.Background(), time.Now())
	}
}

func (m *Manager) Shutdown(ctx context.Context) {
	sessions := m.store.All()
	for _, sess := range sessions {
		if sess.State == StatePaused {
			continue // already snapshotted to disk; left for recovery on next start
		}
		if sess.PooledVM != nil {
			continue // pool.Shutdown handles these
		}
		if sess.Cgroup != nil {
			sess.Cgroup.Destroy()
		}
		if sess.VM != nil {
			m.vmManager.Destroy(ctx, sess.VM.ID)
		}
	}
	for _, p := range m.sizePools {
		if p != nil {
			p.Shutdown()
		}
	}
}
