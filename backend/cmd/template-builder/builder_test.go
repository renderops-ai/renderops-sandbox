package main

import (
	"testing"

	"backend/internal/vmsize"
)

func TestStandardSpecsFollowVMSizes(t *testing.T) {
	t.Parallel()

	specs := standardSpecs(nil)
	if len(specs) != len(vmsize.Sizes) {
		t.Fatalf("got %d standard specs, want %d", len(specs), len(vmsize.Sizes))
	}
	for i, sz := range vmsize.Sizes {
		spec := specs[i]
		if spec.Name != sz.Name ||
			spec.VCPUs != sz.VCPUs ||
			spec.MemoryMB != sz.MemoryMB ||
			spec.DiskMB != sz.DiskGB*1024 {
			t.Fatalf("spec %d = %#v, does not match size %#v", i, spec, sz)
		}
		if !spec.Warmup {
			t.Fatalf("standard spec %q must enable runtime warmup", spec.Name)
		}
	}
}

func TestStandardSpecsValidateProvisionedNumpy(t *testing.T) {
	t.Parallel()

	specs := standardSpecs([]string{"pip install numpy"})
	for _, spec := range specs {
		if len(spec.Validate) != 1 || spec.Validate[0] != "python3 -c 'import numpy'" {
			t.Fatalf("spec %q validation = %v", spec.Name, spec.Validate)
		}
	}
}

func TestMicroSpecIsMinimalAndUnwarmed(t *testing.T) {
	t.Parallel()

	spec := microSpec()
	if spec.Name != "micro" || spec.VCPUs != 1 || spec.MemoryMB != 128 || spec.DiskMB != 512 {
		t.Fatalf("unexpected micro spec: %#v", spec)
	}
	if spec.Warmup || len(spec.Provision) != 0 {
		t.Fatalf("micro spec should be bare: %#v", spec)
	}
}

func TestAgentSpecsArePreinstalledAndValidated(t *testing.T) {
	specs := agentSpecs()
	if len(specs) != len(vmsize.Sizes) {
		t.Fatal("agent release must cover every supported VM size")
	}
	for i, spec := range specs {
		if spec.Name != vmsize.Sizes[i].Name || len(spec.Provision) != 0 {
			t.Fatalf("agent spec installs at boot or has wrong size: %+v", spec)
		}
		for _, command := range []string{"git --version", "codex --version", "claude --version", "opencode --version"} {
			found := false
			for _, actual := range spec.Validate {
				if actual == command {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing validation: %s", command)
			}
		}
	}
}
