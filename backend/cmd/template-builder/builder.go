package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"backend/internal/executor/firecracker"
	"backend/internal/template"
	"backend/internal/vmsize"
	workerconfig "backend/internal/workerplane/config"
)

// buildSpec is one template variant to build: its shape, what to run inside the
// build VM (warm-up + provision commands), and how to validate the resulting
// snapshot after restoring it.
type buildSpec struct {
	Name      string
	VCPUs     int // integer vCPU count (overcommit is a runtime lever, not baked here)
	MemoryMB  int
	DiskMB    int
	Warmup    bool     // run Node/Python warm-up before snapshot
	Provision []string // commands run in the build VM; a failure aborts the build
	Validate  []string // commands run on the RESTORED snapshot; must exit 0 to publish
}

// standardSpecs derives the standard sizes from vmsize.Sizes (the single source of
// truth) and bakes numpy into each. Disk is converted GB->MB here; CPU stays an
// integer count.
func standardSpecs(provision []string) []buildSpec {
	// Validate the real artifact after restore. If we baked numpy, assert it imports;
	// otherwise just confirm the runtimes survived the restore (no internet needed).
	validate := []string{"python3 --version", "node --version"}
	for _, p := range provision {
		if strings.Contains(p, "numpy") {
			validate = []string{"python3 -c 'import numpy'"}
		}
	}
	specs := make([]buildSpec, 0, len(vmsize.Sizes))
	for _, sz := range vmsize.Sizes {
		specs = append(specs, buildSpec{
			Name:      sz.Name,
			VCPUs:     sz.VCPUs,
			MemoryMB:  sz.MemoryMB,
			DiskMB:    sz.DiskGB * 1024,
			Warmup:    true,
			Provision: provision,
			Validate:  validate,
		})
	}
	return specs
}

// agentSpecs installs nothing in the VM: binaries are baked into the rootfs.
// Validate all agents after restore, without API keys or model calls.
func agentSpecs() []buildSpec {
	specs := standardSpecs(nil)
	for i := range specs {
		specs[i].Validate = append(specs[i].Validate,
			"node -e 'if (+process.versions.node.split(\".\")[0] < 22) process.exit(1)'",
			"node -e 'const m=require(\"/etc/spawn/agents.json\"); if (m.image!==\"agents\") process.exit(1)'",
			"git --version", "codex --version", "claude --version", "opencode --version")
	}
	return specs
}

// microSpec is the bare benchmark variant: 1 vCPU / 128 MB / 512 MB, no warm-up
// or provisioned packages. CPU throughput under overcommit is workload-dependent.
func microSpec() buildSpec {
	return buildSpec{
		Name:     "micro",
		VCPUs:    1,
		MemoryMB: 128,
		DiskMB:   512,
		Warmup:   false,
		Validate: []string{"true"}, // prove it restores + the guest agent responds
	}
}

func (s buildSpec) vmConfig(cfg *workerconfig.Config) firecracker.VMConfig {
	return firecracker.VMConfig{
		VCPUCount:  s.VCPUs,
		MemSizeMiB: s.MemoryMB,
		DiskMB:     s.DiskMB,
		Timeout:    30 * time.Second,
		KernelPath: cfg.KernelPath,
		RootfsPath: cfg.RootfsPath,
		InitrdPath: cfg.InitrdPath,
		BootArgs:   "console=ttyS0 reboot=k panic=1 pci=off random.trust_cpu=on rng_core.default_quality=1024",
	}
}

// buildAndValidate builds one variant, then restores its snapshot and runs the
// validation commands (Codex's "validate the real artifact" gate). It returns the
// built variant ready to publish.
func buildAndValidate(ctx context.Context, mgr *firecracker.FireCrackerManager, cfg *workerconfig.Config, s buildSpec, snapRoot string) (template.BuiltVariant, error) {
	vmCfg := s.vmConfig(cfg)
	snapDir := filepath.Join(snapRoot, s.Name)

	tmpl, err := mgr.CreateTemplateWithProvision(ctx, vmCfg, snapDir, firecracker.TemplateBuildOptions{
		Warmup:    s.Warmup,
		Provision: s.Provision,
	})
	if err != nil {
		return template.BuiltVariant{}, fmt.Errorf("build %s: %w", s.Name, err)
	}

	if err := validateSnapshot(ctx, mgr, vmCfg, tmpl, s.Validate); err != nil {
		return template.BuiltVariant{}, fmt.Errorf("validate %s: %w", s.Name, err)
	}

	return template.BuiltVariant{
		Size:             s.Name,
		VCPUs:            s.VCPUs,
		MemoryMB:         s.MemoryMB,
		DiskMB:           s.DiskMB,
		SnapshotPath:     tmpl.SnapPath,
		MemoryPath:       tmpl.MemPath,
		WritableSeedPath: tmpl.WritableDiskPath,
		VsockPath:        tmpl.VsockPath,
		TapName:          tmpl.TapName,
	}, nil
}

// validateSnapshot restores a just-built snapshot into a throwaway VM and runs the
// validation commands over vsock. Any failure means the artifact must not ship.
func validateSnapshot(ctx context.Context, mgr *firecracker.FireCrackerManager, cfg firecracker.VMConfig, tmpl *firecracker.SnapshotTemplate, cmds []string) error {
	vm, err := mgr.LoadFromSnapshot(ctx, cfg, tmpl)
	if err != nil {
		return fmt.Errorf("restore for validation: %w", err)
	}
	defer mgr.Destroy(ctx, vm.ID)

	vsock := firecracker.NewVsockClient(vm.VsockPath)
	ready := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ok, _, _ := vsock.Ping(); ok {
			ready = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ready {
		return fmt.Errorf("restored VM vsock never became ready")
	}

	conn, err := vsock.Connect()
	if err != nil {
		return fmt.Errorf("vsock connect: %w", err)
	}
	defer conn.Close()
	for _, c := range cmds {
		resp, err := vsock.ExecOnConn(conn, c, 120)
		if err != nil {
			return fmt.Errorf("validation %q transport failed: %w", c, err)
		}
		if resp.ExitCode != 0 {
			return fmt.Errorf("validation %q failed (exit %d): %s", c, resp.ExitCode, strings.TrimSpace(resp.Stderr))
		}
	}
	return nil
}

// Host-facts gathering (arch, CPU class, FC version, rootfs/kernel digests) lives
// in internal/template.GatherHostFacts — shared with the worker's compatibility check.
