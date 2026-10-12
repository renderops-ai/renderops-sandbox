package config

import (
	"backend/internal/bootstrap"
	"backend/internal/writabledisk"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type Config struct {
	RootDirectory       string // ROOT_DIRECTORY on the worker host; all agent paths derive from it
	AssetsPath          string
	KernelPath          string
	RootfsPath          string
	InitrdPath          string
	FirecrackerBinary   string
	SocketDir           string
	ActiveDiskBackend   string
	ActiveDiskDir       string
	ActiveDiskCloneMode writabledisk.CloneMode
	SnapshotDir         string
	HostValidationMode  string
	FCRunUID            int // uid Firecracker VMMs drop to via setpriv; 0 = run as root (disabled)
	FCRunGID            int // gid Firecracker VMMs drop to via setpriv; 0 = run as root (disabled)
	Warnings            []string
}

// Load builds the worker host configuration. It does not read or require a
// database URL because persistence belongs to the control plane.
func Load() (*Config, error) {
	cfg := &Config{
		HostValidationMode: defaultString(strings.TrimSpace(os.Getenv("HOST_VALIDATION_MODE")), "strict"),
	}

	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get working directory: %w", err)
	}

	// ROOT_DIRECTORY is the agent's anchor on the worker host — every path below
	// derives from it. Establish the directory tree before resolving any path so
	// $ROOT/assets, $ROOT/sockets etc. exist to be found.
	cfg.RootDirectory, err = resolveRoot(os.Getenv("ROOT_DIRECTORY"))
	if err != nil {
		return nil, fmt.Errorf("resolve root directory: %w", err)
	}
	if err := bootstrap.EnsureLayout(cfg.RootDirectory); err != nil {
		return nil, err
	}

	cfg.AssetsPath, err = resolvePath(
		strings.TrimSpace(os.Getenv("ASSETS_PATH")),
		[]string{
			filepath.Join(cfg.RootDirectory, "assets"),
			"/app/assets",
			filepath.Join(cwd, "assets"),
			filepath.Join(cwd, "..", "assets"),
		},
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve assets path: %w", err)
	}

	// Firecracker binary + VM assets default to the pushed bundle under
	// $ROOT/assets. Existence is deliberately NOT checked here: the agent's
	// bootstrap (bootstrap.EnsureAssets) unpacks the bundle first, then calls
	// cfg.ValidateAssets(). This lets a freshly allocated VM start with an empty
	// assets dir and populate it before validating.
	cfg.FirecrackerBinary = defaultString(
		strings.TrimSpace(os.Getenv("FIRECRACKER_BINARY")),
		filepath.Join(cfg.AssetsPath, "firecracker"),
	)
	if abs, aerr := filepath.Abs(cfg.FirecrackerBinary); aerr == nil {
		cfg.FirecrackerBinary = abs
	}

	// Guest kernel: by default the activated version under assets
	// (ops/worker/activate-kernel.sh); KERNEL_PATH points at a specific file.
	cfg.KernelPath = defaultString(
		strings.TrimSpace(os.Getenv("KERNEL_PATH")),
		filepath.Join(cfg.AssetsPath, "kernel", "vmlinux"),
	)
	if abs, aerr := filepath.Abs(cfg.KernelPath); aerr == nil {
		cfg.KernelPath = abs
	}
	cfg.RootfsPath = filepath.Join(cfg.AssetsPath, "rootfs", "rootfs-alpine.ext4")
	cfg.InitrdPath = filepath.Join(cfg.AssetsPath, "initramfs.cpio.gz")

	cfg.SocketDir = defaultString(strings.TrimSpace(os.Getenv("SOCKET_DIR")), filepath.Join(cfg.RootDirectory, "sockets"))
	cfg.SocketDir, err = filepath.Abs(cfg.SocketDir)
	if err != nil {
		return nil, fmt.Errorf("resolve socket dir: %w", err)
	}
	if err := os.MkdirAll(cfg.SocketDir, 0755); err != nil {
		return nil, fmt.Errorf("create socket dir %q: %w", cfg.SocketDir, err)
	}

	cfg.ActiveDiskBackend = defaultString(strings.ToLower(strings.TrimSpace(os.Getenv("ACTIVE_DISK_BACKEND"))), writabledisk.BackendFilesystem)
	cfg.ActiveDiskDir = defaultString(strings.TrimSpace(os.Getenv("ACTIVE_DISK_DIR")), cfg.SocketDir)
	cfg.ActiveDiskDir, err = filepath.Abs(cfg.ActiveDiskDir)
	if err != nil {
		return nil, fmt.Errorf("resolve active disk dir: %w", err)
	}
	cfg.ActiveDiskCloneMode, err = writabledisk.ParseCloneMode(os.Getenv("ACTIVE_DISK_CLONE_MODE"))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.ActiveDiskDir, 0750); err != nil {
		return nil, fmt.Errorf("create active disk dir %q: %w", cfg.ActiveDiskDir, err)
	}

	cfg.SnapshotDir = defaultString(strings.TrimSpace(os.Getenv("SNAPSHOT_DIR")), "/dev/shm/fc-snapshots")
	cfg.SnapshotDir, err = filepath.Abs(cfg.SnapshotDir)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot dir: %w", err)
	}
	if err := os.MkdirAll(cfg.SnapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("create snapshot dir %q: %w", cfg.SnapshotDir, err)
	}

	cfg.FCRunUID = intEnv("FC_RUN_UID", 0)
	cfg.FCRunGID = intEnv("FC_RUN_GID", 0)

	warnings, err := validateHost(cfg.HostValidationMode)
	if err != nil {
		return nil, err
	}
	cfg.Warnings = warnings

	return cfg, nil
}

// ValidateAssets checks that the VM assets and Firecracker binary are usable.
// The worker calls this after bootstrap has unpacked the asset bundle.
func (c *Config) ValidateAssets() error {
	if err := requireFile(c.KernelPath); err != nil {
		return fmt.Errorf("kernel asset invalid: %w", err)
	}
	if err := requireFile(c.RootfsPath); err != nil {
		return fmt.Errorf("rootfs asset invalid: %w", err)
	}
	if err := requireFile(c.InitrdPath); err != nil {
		return fmt.Errorf("initramfs asset invalid: %w", err)
	}
	if err := requireExecutable(c.FirecrackerBinary); err != nil {
		return fmt.Errorf("firecracker binary invalid: %w", err)
	}
	return nil
}

func defaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// intEnv reads an integer environment variable, returning fallback if unset or invalid.
func intEnv(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// resolveRoot turns ROOT_DIRECTORY (default "~/aman") into an absolute path,
// expanding a leading ~ against the agent user's home. This is the one place a
// remote path is anchored — everything else derives from it, so nothing is
// hardcoded (plan.md invariant).
func resolveRoot(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "~/aman"
	}
	if raw == "~" || strings.HasPrefix(raw, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand ~ in ROOT_DIRECTORY: %w", err)
		}
		raw = filepath.Join(home, strings.TrimPrefix(raw, "~"))
	}
	return filepath.Abs(raw)
}

func resolvePath(explicit string, candidates []string, wantDir bool) (string, error) {
	if explicit != "" {
		abs, err := filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
		return abs, nil
	}

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil {
			continue
		}
		if wantDir && info.IsDir() {
			return filepath.Abs(candidate)
		}
		if !wantDir && !info.IsDir() {
			return filepath.Abs(candidate)
		}
	}

	kind := "file"
	if wantDir {
		kind = "directory"
	}
	return "", fmt.Errorf("no %s found; set the relevant environment variable explicitly", kind)
}

func requireFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("%q is a directory", path)
	}
	return nil
}

func requireExecutable(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("%q is a directory", path)
	}
	if info.Mode()&0111 == 0 {
		return fmt.Errorf("%q is not executable", path)
	}
	return nil
}

func validateHost(mode string) ([]string, error) {
	var warnings []string

	if runtime.GOOS != "linux" {
		msg := fmt.Sprintf("host OS %q is unsupported for Firecracker; run the API on Linux", runtime.GOOS)
		if mode == "warn" {
			return []string{msg}, nil
		}
		return nil, errors.New(msg)
	}

	if _, err := os.Stat("/dev/kvm"); err != nil {
		msg := "missing /dev/kvm; Firecracker requires KVM on the host"
		if mode == "warn" {
			warnings = append(warnings, msg)
		} else {
			return nil, errors.New(msg)
		}
	}

	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		warnings = append(warnings, "cgroup v2 controller file not found; CPU and memory limits may not be enforced")
	}

	return warnings, nil
}
