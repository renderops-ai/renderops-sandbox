package host

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
)

const defaultMaxSessions = 200

// CapacityConfig separates the number of retained sessions from the number of
// network slots needed by concurrently active microVMs.
type CapacityConfig struct {
	PhysicalVCPUs        int
	CPUOvercommitRatio   float64
	MaxSessions          int
	NetworkSlots         int
	NetworkSlotsExplicit bool
}

func resolveCapacity() CapacityConfig {
	physicalVCPUs := envInt("WORKER_ALLOCATABLE_VCPUS", runtime.NumCPU())
	cpuRatio := envFloat("WORKER_CPU_OVERCOMMIT_RATIO", 2)
	maxSessions := envInt("WORKER_MAX_SESSIONS", defaultMaxSessions)

	networkSlots := int(math.Floor(float64(physicalVCPUs) * cpuRatio))
	if networkSlots > maxSessions {
		networkSlots = maxSessions
	}
	if networkSlots < 1 {
		networkSlots = 1
	}

	explicit := false
	if raw := strings.TrimSpace(os.Getenv("SLOT_COUNT")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			networkSlots = value
			explicit = true
		}
	}

	return CapacityConfig{
		PhysicalVCPUs:        physicalVCPUs,
		CPUOvercommitRatio:   cpuRatio,
		MaxSessions:          maxSessions,
		NetworkSlots:         networkSlots,
		NetworkSlotsExplicit: explicit,
	}
}

func envFloat(key string, def float64) float64 {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed >= 1 {
			return parsed
		}
	}
	return def
}
