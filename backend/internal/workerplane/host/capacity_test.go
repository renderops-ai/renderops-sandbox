package host

import "testing"

func TestResolveCapacityDefaultsToTwoTimesCPU(t *testing.T) {
	t.Setenv("WORKER_ALLOCATABLE_VCPUS", "8")
	t.Setenv("WORKER_CPU_OVERCOMMIT_RATIO", "")
	t.Setenv("WORKER_MAX_SESSIONS", "200")
	t.Setenv("SLOT_COUNT", "")
	got := resolveCapacity()
	if got.CPUOvercommitRatio != 2 || got.NetworkSlots != 16 {
		t.Fatalf("default capacity = %+v, want 2x CPU and 16 slots", got)
	}
}

func TestResolveCapacityDerivesNetworkSlots(t *testing.T) {
	t.Setenv("WORKER_ALLOCATABLE_VCPUS", "8")
	t.Setenv("WORKER_CPU_OVERCOMMIT_RATIO", "4")
	t.Setenv("WORKER_MAX_SESSIONS", "200")
	t.Setenv("SLOT_COUNT", "")

	got := resolveCapacity()
	if got.NetworkSlots != 32 || got.MaxSessions != 200 || got.NetworkSlotsExplicit {
		t.Fatalf("capacity = %+v", got)
	}
}

func TestResolveCapacityCapsNetworkSlotsAtMaxSessions(t *testing.T) {
	t.Setenv("WORKER_ALLOCATABLE_VCPUS", "64")
	t.Setenv("WORKER_CPU_OVERCOMMIT_RATIO", "4")
	t.Setenv("WORKER_MAX_SESSIONS", "200")
	t.Setenv("SLOT_COUNT", "")

	got := resolveCapacity()
	if got.NetworkSlots != 200 {
		t.Fatalf("network slots = %d, want 200", got.NetworkSlots)
	}
}

func TestResolveCapacityHonorsExplicitSlotOverride(t *testing.T) {
	t.Setenv("WORKER_ALLOCATABLE_VCPUS", "8")
	t.Setenv("WORKER_CPU_OVERCOMMIT_RATIO", "4")
	t.Setenv("WORKER_MAX_SESSIONS", "200")
	t.Setenv("SLOT_COUNT", "48")

	got := resolveCapacity()
	if got.NetworkSlots != 48 || !got.NetworkSlotsExplicit {
		t.Fatalf("capacity = %+v", got)
	}
}
