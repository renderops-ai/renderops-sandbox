// Worker (host agent): owns Firecracker + the execution engine, exposed over a
// private HTTP API for the control plane to dispatch to. No DB, no user auth —
// the control plane handles those; the worker just runs sandboxes on this host.
package main

import (
	"backend/internal/cgroup"
	"backend/internal/checkpoint"
	"backend/internal/executor/firecracker"
	"backend/internal/gitproxy"
	"backend/internal/ide"
	"backend/internal/logagent"
	"backend/internal/logstream"
	"backend/internal/metering"
	"backend/internal/modelproxy"
	"backend/internal/orchestrator"
	"backend/internal/plane"
	workerv1 "backend/internal/rpc/worker/v1"
	"backend/internal/sandboximage"
	"backend/internal/session"
	"backend/internal/template"
	"backend/internal/vmsize"
	"backend/internal/worker"
	"backend/internal/workerplane/host"
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

func setupLogger() {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if os.Getenv("LOG_FORMAT") == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(h))
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 1 {
			return n
		}
	}
	return def
}

// envDuration reads a Go duration ("15m"); "0" is allowed. A malformed or
// negative value is logged and the default is used.
func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		slog.Warn("invalid duration in environment, using default", "key", key, "value", v, "default", def.String(), "err", err)
		return def
	}
	return d
}

// inventoryInterval is how often the worker sends the orchestrator the
// sandboxes it holds, so rows of sandboxes it lost (e.g. in a restart) are
// marked lost. It is also sent right after every registration.
const inventoryInterval = time.Minute

// workerInventory is what the session manager holds, for the orchestrator.
func workerInventory(mgr *session.Manager) orchestrator.WorkerInventory {
	held, lost, complete := mgr.Inventory()
	inventory := orchestrator.WorkerInventory{
		Sandboxes:          held,
		DurableCheckpoints: mgr.DurableCheckpoints(),
		Complete:           complete,
	}
	if inventory.Sandboxes == nil {
		inventory.Sandboxes = []string{}
	}
	for _, sandbox := range lost {
		inventory.Lost = append(inventory.Lost, orchestrator.LostSandbox{ID: sandbox.ID, Reason: sandbox.Reason})
	}
	return inventory
}

// reportInventory sends the inventory and acknowledges the lost sandboxes the
// orchestrator received.
func reportInventory(ctx context.Context, client *orchestrator.Client, workerID string, mgr *session.Manager) error {
	inventory := workerInventory(mgr)
	result, err := client.ReportInventory(ctx, workerID, inventory)
	if err != nil {
		return err
	}
	lostIDs := make([]string, 0, len(inventory.Lost))
	for _, sandbox := range inventory.Lost {
		lostIDs = append(lostIDs, sandbox.ID)
	}
	mgr.AckLost(lostIDs)
	if len(result.MarkedLost) > 0 {
		slog.Warn("orchestrator marked sandboxes missing from this worker as lost",
			"worker_id", workerID, "sandbox_ids", result.MarkedLost)
	}
	return nil
}

func startOrchestratorRegistration(ctx context.Context, maxSessions int, physicalVCPUs int, supportedImages []string, capacity func() plane.Capacity, sessions *session.Manager) {
	baseURL := os.Getenv("ORCHESTRATOR_URL")
	if baseURL == "" {
		slog.Warn("ORCHESTRATOR_URL not set; worker registration and heartbeat are disabled")
		return
	}
	currentCapacity := capacity()
	registration := orchestrator.WorkerRegistration{
		ID:                  os.Getenv("WORKER_ID"),
		Endpoint:            os.Getenv("WORKER_ADVERTISE_URL"),
		Pool:                os.Getenv("WORKER_POOL"),
		AllocatableVCPUs:    physicalVCPUs,
		AllocatableMemoryMB: envInt("WORKER_ALLOCATABLE_MEMORY_MB", 0),
		AllocatableDiskGB:   currentCapacity.AllocatableDiskGB,
		MaxSandboxes:        maxSessions,
		SupportedImages:     supportedImages,
	}
	if registration.ID == "" ||
		registration.Endpoint == "" ||
		registration.AllocatableVCPUs <= 0 ||
		registration.AllocatableMemoryMB <= 0 ||
		registration.AllocatableDiskGB <= 0 {
		slog.Error(
			"worker registration disabled because identity or allocatable capacity is missing",
			"worker_id", registration.ID,
			"endpoint", registration.Endpoint,
		)
		return
	}

	client := orchestrator.NewClient(baseURL, os.Getenv("WORKER_TOKEN"))
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		registered := false
		var lastInventory time.Time
		for {
			if !registered {
				if err := client.RegisterWorker(ctx, registration); err != nil {
					slog.Warn("worker registration failed", "worker_id", registration.ID, "err", err)
				} else {
					registered = true
					lastInventory = time.Time{} // reconcile right after (re)registering
					slog.Info("worker registered", "worker_id", registration.ID, "endpoint", registration.Endpoint)
					if err := client.Heartbeat(ctx, registration.ID, capacity()); err != nil {
						registered = false
						slog.Warn("initial worker heartbeat failed", "worker_id", registration.ID, "err", err)
					}
				}
			} else if err := client.Heartbeat(ctx, registration.ID, capacity()); err != nil {
				registered = false
				slog.Warn("worker heartbeat failed; registration will be retried", "worker_id", registration.ID, "err", err)
			}
			if registered && time.Since(lastInventory) >= inventoryInterval {
				if err := reportInventory(ctx, client, registration.ID, sessions); err != nil {
					slog.Warn("worker inventory report failed; will retry", "worker_id", registration.ID, "err", err)
				} else {
					lastInventory = time.Now()
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// createTemplateWithRetry wraps CreateTemplate with bounded retries. Template VM
// warm-up occasionally fails because the guest-agent vsock isn't ready in time —
// especially during the startup burst, when several size templates warm up back
// to back and the previous template VM's kernel-side teardown (netns / vsock CID /
// TAP) is still settling. A short settle delay plus a retry turns that transient
// flake into a reliably warm template, so a size never silently drops to cold-boot
// (which disables pause/resume for that size). This mirrors the resume-path vsock
// handshake retry (2103f8f); the host has ample spare capacity, so the retry cost
// is negligible on the rare flaky boot.
func createTemplateWithRetry(mgr *firecracker.FireCrackerManager, cfg firecracker.VMConfig, snapDir string, attempts int) (*firecracker.SnapshotTemplate, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(2 * time.Second) // let the previous template VM's teardown settle
		}
		tmpl, err := mgr.CreateTemplate(context.Background(), cfg, snapDir)
		if err == nil {
			if i > 0 {
				slog.Info("template creation succeeded on retry", "attempt", i+1, "snap_dir", snapDir)
			}
			return tmpl, nil
		}
		lastErr = err
		slog.Warn("template creation failed, retrying", "attempt", i+1, "attempts", attempts, "snap_dir", snapDir, "err", err)
	}
	return nil, lastErr
}

// guestKernelVersion returns the release from the "Linux version ..." banner
// embedded in a vmlinux image, or "unknown". Used only for the startup log.
func guestKernelVersion(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("read guest kernel version failed", "path", path, "err", err)
		return "unknown"
	}
	const banner = "Linux version "
	i := bytes.Index(data, []byte(banner))
	if i < 0 {
		return "unknown"
	}
	release := data[i+len(banner):]
	if end := bytes.IndexAny(release, " \x00\n"); end >= 0 && end <= 64 {
		return string(release[:end])
	}
	return "unknown"
}

func main() {
	setupLogger()

	// Shared host bring-up (unpack+validate assets, provision networking/user,
	// cgroup init, Firecracker manager). Identical to before — just extracted so
	// the template-builder can reuse the exact same host readiness.
	h, err := host.Init()
	if err != nil {
		slog.Error("worker startup failed", "err", err)
		os.Exit(1)
	}
	cfg := h.Config
	vmManager := h.VMManager
	slotCount := h.SlotCount
	maxSessions := h.Capacity.MaxSessions
	resolvedRootfs, err := filepath.EvalSymlinks(cfg.RootfsPath)
	if err != nil {
		slog.Error("resolve immutable rootfs failed", "path", cfg.RootfsPath, "err", err)
		os.Exit(1)
	}
	cfg.RootfsPath = resolvedRootfs
	// Same for the kernel: pin this process to the activated version so a later
	// activate-kernel.sh only takes effect on restart.
	resolvedKernel, err := filepath.EvalSymlinks(cfg.KernelPath)
	if err != nil {
		slog.Error("resolve immutable kernel failed", "path", cfg.KernelPath, "err", err)
		os.Exit(1)
	}
	cfg.KernelPath = resolvedKernel
	slog.Info("guest kernel", "path", cfg.KernelPath, "version", guestKernelVersion(cfg.KernelPath))

	// Base config derives from the DEFAULT size (vmsize.Default) so the default-size
	// template is actually built at that shape — not a hardcoded 256MB/10GB.
	def := vmsize.Default()
	baseCfg := firecracker.VMConfig{
		VCPUCount:  def.VCPUs,
		MemSizeMiB: def.MemoryMB,
		DiskGB:     def.DiskGB,
		Timeout:    30 * time.Second,
		KernelPath: cfg.KernelPath,
		RootfsPath: cfg.RootfsPath,
		InitrdPath: cfg.InitrdPath,
		BootArgs:   "console=ttyS0 reboot=k panic=1 pci=off random.trust_cpu=on rng_core.default_quality=1024",
	}

	// Templates: download a prebuilt release from object storage (TEMPLATE_SOURCE=
	// prebuilt — restore-only) or build them locally at startup (default). Fail closed
	// on resolution failure — production must not silently cold-boot.
	resolvedTemplates, err := resolveTemplates(context.Background(), cfg, vmManager, baseCfg)
	if err != nil {
		slog.Error("template resolution failed", "err", err)
		os.Exit(1)
	}
	// Pools and registration are derived from the boot catalog so worker
	// capabilities cannot drift from the images the worker can actually start.
	imageConfigs := map[string]firecracker.VMConfig{
		sandboximage.Default: baseCfg,
	}
	defaultTemplate := resolvedTemplates[vmsize.Default().Key()]
	sizeTemplates := make(map[string]*firecracker.SnapshotTemplate, len(resolvedTemplates))
	for sizeKey, tmpl := range resolvedTemplates {
		sizeTemplates[sandboximage.PoolKey(sandboximage.Default, sizeKey)] = tmpl
	}
	// Pinned extra images (WORKER_IMAGES, e.g. agents); one that fails is left out.
	for key, tmpl := range addExtraImages(context.Background(), cfg, vmManager, baseCfg, imageConfigs) {
		sizeTemplates[key] = tmpl
	}

	// One session pool per size (vmsize.Sizes is the single source of truth). cgroup
	// limits are derived from the size, matching the monolith.
	sizePools := make(map[string]*firecracker.VMPool, len(imageConfigs)*len(vmsize.Sizes))
	for image, imageCfg := range imageConfigs {
		for _, sz := range imageSizes(image) {
			szCfg := imageCfg
			szCfg.VCPUCount = sz.VCPUs
			szCfg.MemSizeMiB = sz.MemoryMB
			szCfg.DiskGB = sz.DiskGB
			szCgroup := cgroup.Config{
				CPUQuotaUS:  sz.CgroupCPUQuotaUS(),
				CPUPeriodUS: vmsize.CgroupCPUPeriodUS,
				MemMaxBytes: sz.CgroupMemMaxBytes(),
			}
			key := sandboximage.PoolKey(image, sz.Key())
			sizePools[key] = firecracker.NewVMPoolWithSnapshot(0, slotCount, szCfg, vmManager, szCgroup, sizeTemplates[key], false, false)
		}
	}
	supportedImages := make([]string, 0, len(imageConfigs))
	for image := range imageConfigs {
		supportedImages = append(supportedImages, image)
	}
	sort.Strings(supportedImages)
	slog.Info("worker execution engine initialized", "pools", len(sizePools), "images", supportedImages, "template_source", env("TEMPLATE_SOURCE", "build"))

	admission := worker.NewAdmission(
		h.Capacity.PhysicalVCPUs,
		envInt("WORKER_ALLOCATABLE_MEMORY_MB", 0),
		1,
		maxSessions,
		h.Capacity.CPUOvercommitRatio,
		envFloat("WORKER_MEMORY_OVERCOMMIT_RATIO", 1),
	)
	diskCapacity := worker.NewHostDiskCapacity(
		cfg.ActiveDiskDir,
		envInt("WORKER_DISK_CAP_GB", 0),
		envInt("WORKER_DISK_RESERVE_GB", 0),
	)
	admission.SetDiskCapacityProvider(diskCapacity.CapacityGB)
	initialCapacity := admission.Capacity()
	if initialCapacity.AllocatableDiskGB <= 0 {
		slog.Error("worker filesystem has no schedulable disk capacity", "root", cfg.ActiveDiskDir)
		os.Exit(1)
	}
	slog.Info(
		"worker disk capacity detected",
		"root", cfg.ActiveDiskDir,
		"allocatable_disk_gb", initialCapacity.AllocatableDiskGB,
	)

	var usageReporter *metering.Reporter
	var onMeter session.MeterHook
	if controlPlaneURL, workerID := os.Getenv("CONTROL_PLANE_INTERNAL_URL"), os.Getenv("WORKER_ID"); controlPlaneURL != "" && workerID != "" {
		usageReporter = metering.NewReporter(
			metering.NewClient(controlPlaneURL, os.Getenv("WORKER_TOKEN")),
			workerID,
		)
		onMeter = func(sample session.MeterSample) {
			usageReporter.Record(metering.Sample{
				SandboxID:     sample.SandboxID,
				Bucket:        sample.Bucket.Format(time.RFC3339),
				VCPUSeconds:   sample.VCPUSeconds,
				RAMGBSeconds:  sample.RAMGBSeconds,
				DiskGBSeconds: sample.DiskGBSeconds,
			})
		}
	} else {
		slog.Warn("CONTROL_PLANE_INTERNAL_URL or WORKER_ID not set; raw usage reporting is disabled")
	}

	var onState session.StateHook
	// A lost sandbox frees its local reservation; with an orchestrator, its
	// final usage is flushed and the loss reported (the row becomes "lost").
	onLost := func(sandboxID, _, _ string) error {
		admission.Release(sandboxID)
		return nil
	}
	if orchestratorURL, workerID := os.Getenv("ORCHESTRATOR_URL"), os.Getenv("WORKER_ID"); orchestratorURL != "" && workerID != "" {
		orchestrationClient := orchestrator.NewClient(orchestratorURL, os.Getenv("WORKER_TOKEN"))
		onLost = func(sandboxID, _, reason string) error {
			admission.Release(sandboxID)
			// Usage is accepted only from the placed worker, so flush before
			// the report releases the placement.
			if usageReporter != nil {
				flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := usageReporter.Flush(flushCtx); err != nil {
					slog.Error("flush final usage of lost sandbox failed", "sandbox_id", sandboxID, "err", err)
				}
				flushCancel()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return orchestrationClient.ReportWorkerLost(ctx, workerID, sandboxID, reason)
		}
		onState = func(sandboxID, _ string, state string) error {
			switch state {
			case "active":
				admission.MarkActive(sandboxID)
			case "paused":
				admission.MarkPaused(sandboxID)
			case "destroyed":
				admission.Release(sandboxID)
			}
			if state == "destroyed" && usageReporter != nil {
				flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := usageReporter.Flush(flushCtx); err != nil {
					slog.Error("flush final usage before placement release failed", "sandbox_id", sandboxID, "err", err)
				}
				flushCancel()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return orchestrationClient.ReportWorkerState(ctx, workerID, sandboxID, state)
		}
	}

	// The worker never accesses the DB. Lifecycle events go to the orchestrator;
	// raw usage goes to the control plane's authenticated metering endpoint.
	sessionMgr := session.NewManager(
		vmManager,
		defaultTemplate,
		baseCfg,
		maxSessions,
		5*time.Minute, // default idle timeout (per-session values from the request override)
		24*time.Hour,  // default max lifetime
		sizePools,
		sizeTemplates,
		onState,
		onMeter,
	)
	if account := env("BLOB_STORAGE_ACCOUNT", ""); account != "" && env("SANDBOX_CHECKPOINTS_ENABLED", "true") != "false" {
		store, err := template.NewAzureBlobStore(account, env("BLOB_SECRET_KEY", ""), env("SANDBOX_CHECKPOINT_CONTAINER_NAME", env("BLOB_CONTAINER_NAME", "")))
		if err != nil {
			slog.Error("configure durable sandbox checkpoints", "err", err)
			os.Exit(1)
		}
		writer, err := checkpoint.NewWriter(store, env("SANDBOX_CHECKPOINT_PREFIX", "sandbox-checkpoints"))
		if err != nil {
			slog.Error("configure durable sandbox checkpoint writer", "err", err)
			os.Exit(1)
		}
		reader, err := checkpoint.NewReader(store, env("SANDBOX_CHECKPOINT_PREFIX", "sandbox-checkpoints"))
		if err != nil {
			slog.Error("configure durable sandbox checkpoint reader", "err", err)
			os.Exit(1)
		}
		sessionMgr.SetCheckpointWriter(writer)
		sessionMgr.SetCheckpointReader(reader)
		slog.Info("durable sandbox checkpoints enabled", "container", env("SANDBOX_CHECKPOINT_CONTAINER_NAME", env("BLOB_CONTAINER_NAME", "")))
	} else {
		slog.Warn("durable sandbox checkpoints disabled; paused state remains worker-local")
	}
	sessionMgr.SetLostHook(onLost)
	sessionMgr.SetMaxTerminalsPerSandbox(envInt("WORKER_MAX_TERMINALS_PER_SANDBOX", 8))
	// A detached agent that is still working keeps its idle sandbox awake;
	// "0" turns this off.
	sessionMgr.SetAgentBusyGrace(envDuration("AGENT_BUSY_GRACE", session.DefaultAgentBusyGrace))
	slog.Info("agent busy check", "agent_busy_grace", sessionMgr.AgentBusyGrace().String())
	for _, recovered := range sessionMgr.Sessions() {
		admission.Restore(
			recovered.ID,
			recovered.VCPUs,
			recovered.MemoryMB,
			recovered.DiskGB,
			recovered.State == session.StatePaused,
		)
	}

	bind := os.Getenv("WORKER_BIND")
	if bind == "" {
		bind = plane.DefaultAgentAddr
	}
	token := os.Getenv("WORKER_TOKEN")
	if token == "" {
		slog.Warn("WORKER_TOKEN not set — worker API is UNAUTHENTICATED (dev only)")
	}
	workerServer := worker.NewServerWithAdmission(sessionMgr, token, admission)
	// Read-only git access for agent sessions: the proxy keeps short-lived,
	// repository-scoped tokens in memory and serves each guest on its gateway.
	gitProxy, err := gitproxy.New(gitproxy.Config{Upstream: env("GIT_PROXY_UPSTREAM", "https://github.com")})
	if err != nil {
		slog.Error("configure git proxy", "err", err)
		os.Exit(1)
	}
	defer gitProxy.Close()
	workerServer.SetGitProxy(gitProxy)
	sessionMgr.AddSlotRevoker(gitProxy.Unbind)
	// Model access for agent sessions: the proxy holds only per-grant proxy tokens
	// and forwards to the control plane's relay, which holds the provider keys.
	if relayURL := os.Getenv("CONTROL_PLANE_INTERNAL_URL"); relayURL != "" && token != "" {
		modelProxy, err := modelproxy.New(modelproxy.Config{RelayURL: relayURL, WorkerToken: token})
		if err != nil {
			slog.Error("configure model proxy", "err", err)
			os.Exit(1)
		}
		defer modelProxy.Close()
		workerServer.SetModelProxy(modelProxy)
		sessionMgr.AddSlotRevoker(modelProxy.Unbind)
	} else {
		slog.Warn("CONTROL_PLANE_INTERNAL_URL or WORKER_TOKEN not set; model access for agent sessions is disabled")
	}
	// Reuse the host worker process, with an independently configurable endpoint.
	if endpoint := env("WORKER_LOG_INGEST_URL", ""); endpoint != "" {
		collector, err := logagent.New(env("WORKER_LOG_SPOOL_DIR", filepath.Join(cfg.RootDirectory, "logs", "pending")), endpoint, token, os.Getenv("WORKER_ID"))
		if err != nil {
			slog.Error("configure output log collector", "err", err)
			os.Exit(1)
		}
		logCtx, stopLogs := context.WithCancel(context.Background())
		defer stopLogs()
		defer collector.Close()
		go collector.Run(logCtx)
		sessionMgr.SetLogCollector(collector.Watch, collector.Stop)
		slog.Info("worker output collector enabled", "endpoint", endpoint, "retention_hours", int(logstream.Retention.Hours()))
	} else {
		slog.Warn("worker output collector disabled; set WORKER_LOG_INGEST_URL")
	}
	ideManager, err := ide.NewService(sessionMgr, ide.Config{
		Binary:    env("IDE_BINARY", "/opt/openvscode-server/bin/openvscode-server"),
		Workspace: env("IDE_WORKSPACE", "/workspace"),
		Port:      uint16(envInt("IDE_PORT", int(ide.DefaultPort))),
	})
	if err != nil {
		slog.Error("configure worker IDE service", "err", err)
		os.Exit(1)
	}
	workerMux := http.NewServeMux()
	workerMux.Handle(plane.RouteSandboxIDE, ide.NewWorkerHandler(ideManager, token))
	workerMux.Handle("/", workerServer.Handler())
	srv := &http.Server{Addr: bind, Handler: workerMux}
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		slog.Error("worker listen failed", "addr", bind, "err", err)
		os.Exit(1)
	}

	multiplexer := cmux.New(listener)
	grpcListener := multiplexer.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpListener := multiplexer.Match(cmux.Any())
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(worker.UnaryTokenAuth(token)),
		grpc.StreamInterceptor(worker.StreamTokenAuth(token)),
	)
	workerv1.RegisterWorkerTerminalServiceServer(grpcServer, worker.NewTerminalGRPCServer(sessionMgr))

	go func() {
		slog.Info("worker HTTP and gRPC listening", "addr", bind)
		if err := grpcServer.Serve(grpcListener); err != nil {
			slog.Error("worker gRPC server failed", "err", err)
		}
	}()
	go func() {
		if err := srv.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			slog.Error("worker HTTP server failed", "err", err)
		}
	}()
	go func() {
		if err := multiplexer.Serve(); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			slog.Error("worker listener failed", "err", err)
		}
	}()

	registrationCtx, stopRegistration := context.WithCancel(context.Background())
	startOrchestratorRegistration(registrationCtx, maxSessions, h.Capacity.PhysicalVCPUs, supportedImages, admission.Capacity, sessionMgr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	slog.Info("worker shutting down")

	// Close every admission path before snapshotting. The local gate handles a
	// placement request already in flight while the durable orchestrator flag
	// removes this worker from subsequent placement queries.
	workerServer.BeginDrain()
	stopRegistration()
	if orchestratorURL, workerID := os.Getenv("ORCHESTRATOR_URL"), os.Getenv("WORKER_ID"); orchestratorURL != "" && workerID != "" {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := orchestrator.NewClient(orchestratorURL, os.Getenv("WORKER_TOKEN")).
			SetWorkerDraining(drainCtx, workerID, true)
		drainCancel()
		if err != nil {
			slog.Error("mark worker draining failed", "worker_id", workerID, "err", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("worker HTTP shutdown failed", "err", err)
	}
	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
	case <-time.After(5 * time.Second):
		slog.Warn("forcing worker gRPC streams closed during drain")
		grpcServer.Stop()
		<-grpcStopped
	}
	cancel()

	pauseTimeout := time.Duration(envInt("SHUTDOWN_GRACE_PERIOD_SECONDS", 300)) * time.Second
	pauseCtx, pauseCancel := context.WithTimeout(context.Background(), pauseTimeout)
	if err := sessionMgr.PauseAllActive(
		pauseCtx,
		envInt("SHUTDOWN_PAUSE_CONCURRENCY", 4),
	); err != nil {
		slog.Error("worker drained with unpaused sessions", "err", err)
	}
	pauseCancel()
	// Whatever is still running dies with this process: report it lost now so
	// its row does not stay active.
	loseCtx, loseCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if lost := sessionMgr.LoseUnpaused(loseCtx, session.LostReasonShutdown); len(lost) > 0 {
		slog.Error("sandboxes lost at shutdown", "count", len(lost), "sandbox_ids", lost)
	}
	loseCancel()

	sessionMgr.Shutdown(context.Background())
	if usageReporter != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := usageReporter.Shutdown(flushCtx); err != nil {
			slog.Error("usage reporter shutdown failed", "err", err)
		}
		flushCancel()
	}
}
