// Command browser-bridge owns the shared browser inside a sandbox VM: a
// virtual display (Xvnc) and a headed Chromium drawn on it. It is controlled
// through a small JSON API on loopback and imports nothing from the backend.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	stopGrace      = 5 * time.Second
	minBackoff     = time.Second
	maxBackoff     = 30 * time.Second
	stableAfter    = time.Minute
	readyPollEvery = 200 * time.Millisecond
	maxLineBytes   = 64 << 10
)

const (
	stateStopped    = "stopped"
	stateStarting   = "starting"
	stateRunning    = "running"
	stateRestarting = "restarting"
	stateStopping   = "stopping"
	stateCrashed    = "crashed"
)

var errStopped = errors.New("browser stack stop requested")

type config struct {
	apiAddr      string
	viewerAddr   string
	noVNCDir     string
	xvncPath     string
	display      string
	vncPort      int
	width        int
	height       int
	chromiumPath string
	cdpPort      int
	profileDir   string
	readyTimeout time.Duration
}

func main() {
	logger := newLogger()
	if err := run(logger); err != nil {
		logger.Error("browser bridge exited", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	browser := newStack(cfg, logger)
	servers := []*http.Server{
		{
			Addr:              cfg.apiAddr,
			Handler:           newAPI(browser, 2*cfg.readyTimeout, logger),
			ReadHeaderTimeout: 5 * time.Second,
		},
		{
			// No read/write timeouts: viewer websockets stay open for the
			// whole session.
			Addr:              cfg.viewerAddr,
			Handler:           newViewer(browser, cfg, logger),
			ReadHeaderTimeout: 5 * time.Second,
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	serveErr := make(chan error, len(servers))
	for _, server := range servers {
		go func() { serveErr <- server.ListenAndServe() }()
	}
	logger.Info("browser bridge listening",
		"api_addr", cfg.apiAddr, "viewer_addr", cfg.viewerAddr, "novnc_dir", cfg.noVNCDir,
		"display", cfg.display, "vnc_port", cfg.vncPort, "cdp_port", cfg.cdpPort,
		"profile_dir", cfg.profileDir)

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("browser bridge shutting down")
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("serving: %w", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*stopGrace)
	defer shutdownCancel()
	for _, server := range servers {
		// Shutdown does not wait for hijacked viewer websockets; they end
		// when the browser stack stops below and Xvnc closes its side.
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutting down listener", "addr", server.Addr, "err", err)
		}
	}
	if err := browser.Stop(shutdownCtx); err != nil {
		logger.Error("stopping browser stack", "err", err)
	}
	return runErr
}

func newLogger() *slog.Logger {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv("BRIDGE_LOG_LEVEL"), "debug") {
		level = slog.LevelDebug
	}
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("component", "browser-bridge")
}

func loadConfig() (config, error) {
	cfg := config{
		apiAddr:      envOr("BRIDGE_API_ADDR", "127.0.0.1:9230"),
		viewerAddr:   envOr("BRIDGE_VIEWER_ADDR", "0.0.0.0:3002"),
		noVNCDir:     envOr("BRIDGE_NOVNC_DIR", "/usr/share/novnc"),
		xvncPath:     envOr("BRIDGE_XVNC_PATH", "Xvnc"),
		display:      envOr("BRIDGE_DISPLAY", ":1"),
		chromiumPath: envOr("BRIDGE_CHROMIUM_PATH", "chromium"),
		profileDir:   envOr("BRIDGE_PROFILE_DIR", "/workspace/.renderops/browser"),
		readyTimeout: 30 * time.Second,
	}
	if _, err := strconv.Atoi(strings.TrimPrefix(cfg.display, ":")); err != nil || !strings.HasPrefix(cfg.display, ":") {
		return config{}, fmt.Errorf("BRIDGE_DISPLAY must look like :1, got %q", cfg.display)
	}
	var err error
	if cfg.vncPort, err = envPort("BRIDGE_VNC_PORT", 5901); err != nil {
		return config{}, err
	}
	if cfg.cdpPort, err = envPort("BRIDGE_CDP_PORT", 9223); err != nil {
		return config{}, err
	}
	if cfg.width, cfg.height, err = parseScreen(envOr("BRIDGE_SCREEN", "1280x800")); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envPort(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("%s must be a port between 1 and 65535, got %q", key, value)
	}
	return port, nil
}

func parseScreen(value string) (int, int, error) {
	widthText, heightText, ok := strings.Cut(value, "x")
	width, widthErr := strconv.Atoi(widthText)
	height, heightErr := strconv.Atoi(heightText)
	if !ok || widthErr != nil || heightErr != nil || width < 320 || height < 240 {
		return 0, 0, fmt.Errorf("BRIDGE_SCREEN must look like 1280x800, got %q", value)
	}
	return width, height, nil
}

// procStatus is the externally visible state of one supervised process.
type procStatus struct {
	State     string    `json:"state"`
	PID       int       `json:"pid,omitempty"`
	StartedAt time.Time `json:"started_at,omitzero"`
	Restarts  int       `json:"restarts"`
	LastError string    `json:"last_error,omitempty"`
}

// status is the JSON document returned by the internal API.
type status struct {
	State    string     `json:"state"`
	Display  procStatus `json:"display"`
	Chromium procStatus `json:"chromium"`
	VNCPort  int        `json:"vnc_port"`
	CDPPort  int        `json:"cdp_port"`
}

// stack supervises the display and Chromium. Chromium depends on the
// display: if the display exits, Chromium is restarted with it; if only
// Chromium exits, the display keeps running.
type stack struct {
	cfg    config
	logger *slog.Logger

	mu       sync.Mutex
	state    string
	display  procStatus
	chromium procStatus
	stop     chan struct{} // closed to stop the run loop; nil when no loop is active
	done     chan struct{} // closed when the run loop has exited
	changed  chan struct{} // closed and replaced on every status change

	// Owned by the run goroutine.
	displayProc *process
	browserProc *process
}

func newStack(cfg config, logger *slog.Logger) *stack {
	return &stack{
		cfg:      cfg,
		logger:   logger,
		state:    stateStopped,
		display:  procStatus{State: stateStopped},
		chromium: procStatus{State: stateStopped},
		changed:  make(chan struct{}),
	}
}

// Start launches the stack if needed and waits until both processes are
// ready. If ctx ends first the stack keeps starting in the background.
func (s *stack) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.stop == nil && s.done != nil {
		previous := s.done
		s.mu.Unlock()
		select {
		case <-previous:
		case <-ctx.Done():
			return fmt.Errorf("waiting for previous stop to finish: %w", ctx.Err())
		}
		s.mu.Lock()
	}
	if s.stop == nil {
		s.stop = make(chan struct{})
		s.done = make(chan struct{})
		s.state = stateStarting
		s.notifyLocked()
		go s.run(s.stop, s.done)
	}
	s.mu.Unlock()
	return s.waitRunning(ctx)
}

// Stop terminates Chromium and the display and waits for the run loop to exit.
func (s *stack) Stop(ctx context.Context) error {
	s.mu.Lock()
	stop, done := s.stop, s.done
	if stop != nil {
		close(stop)
		s.stop = nil
		s.state = stateStopping
		s.notifyLocked()
	}
	s.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for browser stack to stop: %w", ctx.Err())
	}
}

func (s *stack) Status() status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return status{
		State:    s.state,
		Display:  s.display,
		Chromium: s.chromium,
		VNCPort:  s.cfg.vncPort,
		CDPPort:  s.cfg.cdpPort,
	}
}

func (s *stack) waitRunning(ctx context.Context) error {
	for {
		s.mu.Lock()
		state, stopped, changed := s.state, s.stop == nil, s.changed
		lastErr := s.chromium.LastError
		if lastErr == "" {
			lastErr = s.display.LastError
		}
		s.mu.Unlock()

		if state == stateRunning {
			return nil
		}
		if stopped {
			return errors.New("browser stack was stopped while starting")
		}
		select {
		case <-changed:
		case <-ctx.Done():
			if lastErr != "" {
				return fmt.Errorf("browser stack not ready (last error: %s): %w", lastErr, ctx.Err())
			}
			return fmt.Errorf("browser stack not ready: %w", ctx.Err())
		}
	}
}

func (s *stack) run(stop <-chan struct{}, done chan<- struct{}) {
	defer func() {
		s.browserProc.terminate(s.logger)
		s.displayProc.terminate(s.logger)
		s.browserProc, s.displayProc = nil, nil
		s.update(func() {
			s.state = stateStopped
			s.display.State, s.display.PID = stateStopped, 0
			s.chromium.State, s.chromium.PID = stateStopped, 0
		})
		s.logger.Info("browser stack stopped")
		close(done)
	}()

	backoff := minBackoff
	for {
		err := s.startAll(stop)
		switch {
		case errors.Is(err, errStopped):
			return
		case err != nil:
			s.logger.Warn("browser stack failed to start", "err", err, "retry_in", backoff.String())
		default:
			s.update(func() { s.state = stateRunning })
			s.logger.Info("browser stack running",
				"display_pid", s.displayProc.pid(), "chromium_pid", s.browserProc.pid())
			runningSince := time.Now()
			select {
			case <-stop:
				return
			case <-s.displayProc.exited:
				s.recordExit(&s.display, s.displayProc)
				s.browserProc.terminate(s.logger)
				s.browserProc = nil
				s.update(func() { s.chromium.State, s.chromium.PID = stateStopped, 0 })
				s.displayProc.terminate(s.logger)
				s.displayProc = nil
			case <-s.browserProc.exited:
				s.recordExit(&s.chromium, s.browserProc)
				s.browserProc.terminate(s.logger)
				s.browserProc = nil
			}
			if time.Since(runningSince) >= stableAfter {
				backoff = minBackoff
			}
		}

		s.update(func() { s.state = stateRestarting })
		select {
		case <-stop:
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// startAll brings up whatever is not running: the display first, then Chromium.
func (s *stack) startAll(stop <-chan struct{}) error {
	if s.displayProc != nil {
		select {
		case <-s.displayProc.exited:
			s.recordExit(&s.display, s.displayProc)
			s.displayProc.terminate(s.logger)
			s.displayProc = nil
		default:
		}
	}
	if s.displayProc == nil {
		proc, err := s.startDisplay(stop)
		if err != nil {
			return err
		}
		s.displayProc = proc
	}
	proc, err := s.startChromium(stop)
	if err != nil {
		return err
	}
	s.browserProc = proc
	return nil
}

// procSpec describes how to launch one supervised process.
type procSpec struct {
	name  string
	path  string
	args  []string
	env   []string
	ready func(context.Context) error
}

// launch starts one process and waits until spec.ready reports it usable.
func (s *stack) launch(stop <-chan struct{}, ps *procStatus, spec procSpec) (*process, error) {
	s.update(func() { ps.State, ps.PID = stateStarting, 0 })
	proc, err := startProcess(spec, s.logger.With("process", spec.name))
	if err != nil {
		s.fail(ps, err)
		return nil, err
	}
	if err := waitReady(stop, proc, s.cfg.readyTimeout, spec.ready); err != nil {
		proc.terminate(s.logger)
		if errors.Is(err, errStopped) {
			return nil, err
		}
		s.fail(ps, err)
		return nil, err
	}
	s.update(func() {
		ps.State, ps.PID, ps.StartedAt, ps.LastError = stateRunning, proc.pid(), time.Now().UTC(), ""
	})
	return proc, nil
}

func (s *stack) fail(ps *procStatus, err error) {
	s.update(func() { ps.State, ps.PID, ps.LastError = stateStopped, 0, err.Error() })
}

func (s *stack) recordExit(ps *procStatus, proc *process) {
	err := proc.exitErr()
	s.logger.Warn("process exited unexpectedly", "process", proc.name, "pid", proc.pid(), "err", err)
	s.update(func() {
		ps.State, ps.PID, ps.LastError = stateCrashed, 0, err.Error()
		ps.Restarts++
	})
}

func (s *stack) update(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn()
	s.notifyLocked()
}

func (s *stack) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// process is one running child. It runs in its own process group so that
// helper processes (Chromium spawns many) are signalled together.
type process struct {
	name   string
	cmd    *exec.Cmd
	exited chan struct{}
	err    error // set before exited is closed
}

func startProcess(spec procSpec, logger *slog.Logger) (*process, error) {
	cmd := exec.Command(spec.path, spec.args...)
	cmd.Env = append(os.Environ(), spec.env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	output := &lineLogger{logger: logger}
	cmd.Stdout, cmd.Stderr = output, output
	// Leftover helpers can keep the output pipes open after the main process
	// exits; bound how long Wait blocks on them.
	cmd.WaitDelay = stopGrace
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting %s: %w", spec.name, err)
	}
	proc := &process{name: spec.name, cmd: cmd, exited: make(chan struct{})}
	go func() {
		proc.err = cmd.Wait()
		close(proc.exited)
	}()
	logger.Info("process started", "pid", cmd.Process.Pid)
	return proc, nil
}

func (p *process) pid() int {
	if p == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *process) exitErr() error {
	if p.err != nil {
		return p.err
	}
	return errors.New("exited with status 0")
}

// terminate stops the whole process group: SIGTERM, then SIGKILL after
// stopGrace. It is safe to call on a nil or already exited process.
func (p *process) terminate(logger *slog.Logger) {
	if p == nil {
		return
	}
	pgid := p.cmd.Process.Pid
	signalGroup(pgid, syscall.SIGTERM, logger)
	select {
	case <-p.exited:
	case <-time.After(stopGrace):
		signalGroup(pgid, syscall.SIGKILL, logger)
		<-p.exited
	}
	// Clear helpers that outlived the group leader.
	signalGroup(pgid, syscall.SIGKILL, logger)
}

func signalGroup(pgid int, sig syscall.Signal, logger *slog.Logger) {
	if err := syscall.Kill(-pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		logger.Warn("signalling process group", "pgid", pgid, "signal", sig.String(), "err", err)
	}
}

func waitReady(stop <-chan struct{}, proc *process, timeout time.Duration, ready func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(readyPollEvery)
	defer ticker.Stop()
	for {
		checkCtx, checkCancel := context.WithTimeout(ctx, time.Second)
		err := ready(checkCtx)
		checkCancel()
		if err == nil {
			return nil
		}
		select {
		case <-stop:
			return errStopped
		case <-proc.exited:
			return fmt.Errorf("%s exited before becoming ready: %w", proc.name, proc.exitErr())
		case <-ctx.Done():
			return fmt.Errorf("%s not ready after %s: %w", proc.name, timeout, err)
		case <-ticker.C:
		}
	}
}

// lineLogger forwards a child's output to slog one line at a time.
type lineLogger struct {
	mu     sync.Mutex
	buf    []byte
	logger *slog.Logger
}

func (l *lineLogger) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}
		l.logger.Debug("process output", "line", string(l.buf[:i]))
		l.buf = l.buf[i+1:]
	}
	if len(l.buf) > maxLineBytes {
		l.logger.Debug("process output", "line", string(l.buf))
		l.buf = l.buf[:0]
	}
	return len(p), nil
}
