// Package vmsize is the single source of truth for sandbox resource shapes ("sizes").
// Validation (handler), pool construction (main), and routing (session Manager) all read
// from here — add a size to Sizes and everything else follows (DRY).
package vmsize

import "fmt"

// Size is a sandbox resource shape.
type Size struct {
	Name     string
	VCPUs    int
	MemoryMB int
	DiskGB   int
}

// Sizes is the canonical menu. The first entry is the default (used when a request omits
// resources). The default size is served from a warm snapshot template; others cold-boot.
var Sizes = []Size{
	{Name: "nano", VCPUs: 1, MemoryMB: 256, DiskGB: 1},
	{Name: "small", VCPUs: 2, MemoryMB: 512, DiskGB: 10},
	{Name: "medium", VCPUs: 2, MemoryMB: 1024, DiskGB: 20},
}

// Large is for images that need more memory (the agents image). It is not in
// Sizes: standard pools, templates and prebuilt releases are built for Sizes
// only, and package sandboximage decides which image may run at which size.
var Large = Size{Name: "large", VCPUs: 2, MemoryMB: 4096, DiskGB: 20}

// All returns every size a sandbox can be created with.
func All() []Size { return append(append([]Size(nil), Sizes...), Large) }

// Default returns the default size (used when a create request omits resources).
func Default() Size { return Sizes[0] }

// cgroupMemOverheadMB is host-side headroom above the guest RAM for the Firecracker VMM
// process + page cache, so a VM isn't OOM-killed by its own host ceiling.
const cgroupMemOverheadMB = 768

// CgroupCPUPeriodUS is the cgroup cpu.max period; quota = VCPUs * period.
const CgroupCPUPeriodUS = 100_000

// CgroupMemMaxBytes is the host memory ceiling for a VM of this size (guest RAM + overhead).
func (s Size) CgroupMemMaxBytes() int64 {
	return int64(s.MemoryMB+cgroupMemOverheadMB) * 1024 * 1024
}

// CgroupCPUQuotaUS is the cgroup cpu.max quota for this size (VCPUs worth of CPU).
func (s Size) CgroupCPUQuotaUS() int64 { return int64(s.VCPUs) * CgroupCPUPeriodUS }

// Key uniquely identifies a size for pool routing.
func Key(vcpus, memoryMB, diskGB int) string {
	return fmt.Sprintf("%d-%d-%d", vcpus, memoryMB, diskGB)
}

// Key returns this size's routing key.
func (s Size) Key() string { return Key(s.VCPUs, s.MemoryMB, s.DiskGB) }

// IsDefault reports whether this is the default (warm-template) size.
func (s Size) IsDefault() bool { return s.Key() == Default().Key() }

// Resolve validates a requested shape against the menu. A nil/all-zero request returns the
// default; a partial or unknown shape is an error (so we never silently mis-size a VM).
func Resolve(vcpus, memoryMB, diskGB int) (Size, error) {
	if vcpus == 0 && memoryMB == 0 && diskGB == 0 {
		return Default(), nil
	}
	for _, s := range All() {
		if s.VCPUs == vcpus && s.MemoryMB == memoryMB && s.DiskGB == diskGB {
			return s, nil
		}
	}
	return Size{}, fmt.Errorf("unsupported size %dvcpu/%dMB/%dGB (allowed: %s)", vcpus, memoryMB, diskGB, menu())
}

// ByName resolves a size by its menu name ("nano", "small", ...).
func ByName(name string) (Size, error) {
	for _, s := range All() {
		if s.Name == name {
			return s, nil
		}
	}
	return Size{}, fmt.Errorf("unknown size %q (allowed: %s)", name, menu())
}

func menu() string {
	out := ""
	for i, s := range All() {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s(%dvcpu/%dMB/%dGB)", s.Name, s.VCPUs, s.MemoryMB, s.DiskGB)
	}
	return out
}
