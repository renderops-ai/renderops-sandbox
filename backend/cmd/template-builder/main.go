// template-builder is a one-shot process that builds the platform's standard
// Firecracker templates (nano/small/medium, with numpy baked in) plus a bare
// "micro" benchmark template, and publishes them as immutable, versioned releases
// to durable object storage. Runtime workers never build templates — they only
// download and restore what this builds. It runs on a KVM host (it boots real
// Firecracker VMs) and is triggered by an operator or CI, not by an API call.
package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"backend/internal/template"
	"backend/internal/workerplane/host"
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

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func main() {
	setupLogger()
	buildAgents := envBool("BUILD_AGENTS", false)
	buildStandard := envBool("BUILD_STANDARD", !buildAgents)
	if buildAgents && buildStandard {
		slog.Error("agent build must not activate the standard release; set BUILD_STANDARD=false")
		os.Exit(1)
	}

	h, err := host.Init()
	if err != nil {
		slog.Error("template-builder host init failed", "err", err)
		os.Exit(1)
	}
	cfg := h.Config
	mgr := h.VMManager
	ctx := context.Background()

	snapRoot := env("TEMPLATE_BUILD_DIR", filepath.Join(cfg.SnapshotDir, "build"))

	// Durable store: Azure Blob when BLOB_STORAGE_ACCOUNT is set (unless overridden
	// with TEMPLATE_STORE=local), else a local directory. Credentials come from env.
	var baseStore template.ArtifactStore
	if acct := env("BLOB_STORAGE_ACCOUNT", ""); acct != "" && env("TEMPLATE_STORE", "blob") != "local" {
		bs, err := template.NewAzureBlobStore(acct, env("BLOB_SECRET_KEY", ""), env("BLOB_CONTAINER_NAME", ""))
		if err != nil {
			slog.Error("open azure blob store", "err", err)
			os.Exit(1)
		}
		baseStore = bs
		slog.Info("publishing to Azure Blob", "account", acct, "container", env("BLOB_CONTAINER_NAME", ""))
	} else {
		storeDir := env("TEMPLATE_STORE_DIR", filepath.Join(cfg.RootDirectory, "template-store"))
		ls, err := template.NewLocalStore(storeDir)
		if err != nil {
			slog.Error("open local artifact store", "dir", storeDir, "err", err)
			os.Exit(1)
		}
		baseStore = ls
		slog.Info("publishing to local store", "dir", storeDir)
	}
	// Manifest + active pointer stay on the RAW store (small, human-readable JSON);
	// large artifacts go through a gzip layer (mostly-zero seeds shrink hugely).
	repo := template.NewStoreManifestRepository(baseStore)
	artifactStore := template.NewCompressedStore(baseStore)

	f, err := template.GatherHostFacts(cfg.RootfsPath, cfg.KernelPath, cfg.FirecrackerBinary, cfg.AssetsPath)
	if err != nil {
		slog.Error("gather host facts", "err", err)
		os.Exit(1)
	}
	slog.Info("build host facts",
		"arch", f.Arch, "cpu_compat_class", f.CPUCompatClass,
		"firecracker_version", f.FirecrackerVersion,
		"rootfs_digest", short(f.RootfsDigest), "kernel_digest", short(f.KernelDigest))

	stamp := time.Now().UTC().Format("20060102-150405")
	// Baking a package (e.g. numpy) needs the build VM to reach the internet, which the
	// cold-boot build path does not provide yet. So standard templates are warmed-only by
	// default (matching the current worker); set STANDARD_PROVISION once build-VM egress lands.
	var stdProvision []string
	if p := env("STANDARD_PROVISION", ""); p != "" {
		stdProvision = []string{p}
	}

	// ---- standard release: nano/small/medium (warmed), activated for serving ----
	if buildStandard {
		var variants []template.BuiltVariant
		for _, s := range standardSpecs(stdProvision) {
			slog.Info("building standard variant", "size", s.Name, "vcpus", s.VCPUs, "memory_mb", s.MemoryMB, "disk_mb", s.DiskMB)
			bv, err := buildAndValidate(ctx, mgr, cfg, s, filepath.Join(snapRoot, "standard"))
			if err != nil {
				slog.Error("standard variant build failed", "size", s.Name, "err", err)
				os.Exit(1)
			}
			variants = append(variants, bv)
			time.Sleep(2 * time.Second) // let the previous build VM's kernel-side teardown settle
		}
		releaseID := "standard-" + stamp
		m, err := template.PublishRelease(ctx, artifactStore, repo, releaseInputs(releaseID, f, variants))
		if err != nil {
			slog.Error("publish standard release", "err", err)
			os.Exit(1)
		}
		if err := repo.Activate(ctx, releaseID); err != nil {
			slog.Error("activate standard release", "release_id", releaseID, "err", err)
			os.Exit(1)
		}
		slog.Info("standard release published + ACTIVATED", "release_id", m.ReleaseID, "variants", len(m.Variants))
	}

	// Agent release is deliberately opt-in and never changes the shared active
	// pointer. Its builder assets must contain the agent rootfs; validation checks
	// the marker and every CLI after restore before any release is published.
	if buildAgents {
		var variants []template.BuiltVariant
		for _, s := range agentSpecs() {
			bv, err := buildAndValidate(ctx, mgr, cfg, s, filepath.Join(snapRoot, "agents"))
			if err != nil {
				slog.Error("agent variant build failed", "size", s.Name, "err", err)
				os.Exit(1)
			}
			variants = append(variants, bv)
			time.Sleep(2 * time.Second)
		}
		releaseID := "agents-" + stamp
		m, err := template.PublishRelease(ctx, artifactStore, repo, releaseInputs(releaseID, f, variants))
		if err != nil {
			slog.Error("publish agent release", "err", err)
			os.Exit(1)
		}
		slog.Info("agent release published (not activated)", "release_id", m.ReleaseID)
	}

	// ---- micro release: bare benchmark, published but NOT activated ----
	if envBool("BUILD_MICRO", !buildAgents) {
		s := microSpec()
		slog.Info("building micro benchmark variant", "vcpus", s.VCPUs, "memory_mb", s.MemoryMB, "disk_mb", s.DiskMB)
		bv, err := buildAndValidate(ctx, mgr, cfg, s, filepath.Join(snapRoot, "micro"))
		if err != nil {
			slog.Error("micro variant build failed", "err", err)
			os.Exit(1)
		}
		releaseID := "micro-" + stamp
		m, err := template.PublishRelease(ctx, artifactStore, repo, releaseInputs(releaseID, f, []template.BuiltVariant{bv}))
		if err != nil {
			slog.Error("publish micro release", "err", err)
			os.Exit(1)
		}
		slog.Info("micro benchmark release published (not activated)", "release_id", m.ReleaseID)
	}

	slog.Info("template builder finished")
}

func releaseInputs(releaseID string, f template.HostFacts, variants []template.BuiltVariant) template.ReleaseInputs {
	return template.ReleaseInputs{
		ReleaseID:          releaseID,
		CreatedAt:          time.Now().UTC(),
		Arch:               f.Arch,
		CPUCompatClass:     f.CPUCompatClass,
		FirecrackerVersion: f.FirecrackerVersion,
		AssetBundleDigest:  f.AssetBundleDigest,
		RootfsDigest:       f.RootfsDigest,
		KernelDigest:       f.KernelDigest,
		Variants:           variants,
	}
}

func short(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}
