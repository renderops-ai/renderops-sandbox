package cgroup

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const cgroupRoot = "/sys/fs/cgroup/sandbox"

type Config struct {
	CPUQuotaUS  int64 //ms of cpu per needed, 0 means unlinimited  and 0.5 means 50% of the cpu core we eneed
	CPUPeriodUS int64
	MemMaxBytes int64
}

type Cgroup struct {
	path string
}

func Init() error {
	if err := os.MkdirAll(cgroupRoot, 0755); err != nil {
		return fmt.Errorf("cgroup: create root %s: %w", cgroupRoot, err)
	}

	if err := os.WriteFile(
		filepath.Join(cgroupRoot, "cgroup.subtree_control"),
		[]byte("+cpu +memory"),
		0644,
	); err != nil {
		return fmt.Errorf("cgroup: enable controllers: %w", err)
	}
	return nil
}

func New(tenantID, vmID string, cfg Config) (*Cgroup, error) {
	tenantPath := filepath.Join(cgroupRoot, "tenant-"+tenantID)
	vmPath := filepath.Join(tenantPath, "vm-"+vmID)

	if err := os.MkdirAll(tenantPath, 0755); err != nil {
		return nil, fmt.Errorf("cgroup: create tenant dir: %w", err)
	}
	if err := os.WriteFile(
		filepath.Join(tenantPath, "cgroup.subtree_control"),
		[]byte("+cpu +memory"),
		0644,
	); err != nil {
		return nil, fmt.Errorf("cgroup, enable tenant controllers: %w", err)
	}

	if err := os.MkdirAll(vmPath, 0755); err != nil {
		return nil, fmt.Errorf("cgroup: create vm dir: %w", err)
	}

	cg := &Cgroup{path: vmPath}

	if err := cg.setCPU(cfg); err != nil {
		os.Remove(vmPath)
		return nil, err
	}
	if err := cg.setMemory(cfg); err != nil {
		os.Remove(vmPath)
		return nil, err
	}

	return cg, nil
}

func (c *Cgroup) setCPU(cfg Config) error {
	period := cfg.CPUPeriodUS
	if period == 0 {
		period = 100_000
	}
	var val string
	if cfg.CPUQuotaUS == 0 {
		val = fmt.Sprintf("max %d", period)
	} else {
		val = fmt.Sprintf("%d %d", cfg.CPUQuotaUS, period)
	}
	if err := os.WriteFile(filepath.Join(c.path, "cpu.max"), []byte(val), 0644); err != nil {
		return fmt.Errorf("cgroup: set cpu.max: %w", err)
	}

	return nil
}

func (c *Cgroup) setMemory(cfg Config) error {
	var val string
	if cfg.MemMaxBytes == 0 {
		val = "max"
	} else {
		val = strconv.FormatInt(cfg.MemMaxBytes, 10)
	}
	if err := os.WriteFile(filepath.Join(c.path, "memory.max"), []byte(val), 0644); err != nil {
		return fmt.Errorf("cgroup: set memory.max: %w", err)
	}
	return nil
}

func (c *Cgroup) AddPID(pid int) error {
	if err := os.WriteFile(
		filepath.Join(c.path, "cgroup.procs"),
		[]byte(strconv.Itoa(pid)),
		0644,
	); err != nil {
		return fmt.Errorf("cgroup: add pid %d: %w", pid, err)
	}
	return nil
}

// OOMKills returns how many processes the kernel OOM killer has killed in this
// cgroup: the oom_kill counter of its memory.events.
func (c *Cgroup) OOMKills() (uint64, error) {
	return ReadOOMKills(filepath.Join(c.path, "memory.events"))
}

// ReadOOMKills reads the oom_kill counter from a cgroup v2 memory.events file.
func ReadOOMKills(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("cgroup: read %s: %w", path, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || key != "oom_kill" {
			continue
		}
		count, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cgroup: parse oom_kill in %s: %w", path, err)
		}
		return count, nil
	}
	return 0, fmt.Errorf("cgroup: no oom_kill counter in %s", path)
}

func (c *Cgroup) Destroy() error {
	// A cgroup dir can only be removed once the kernel has drained the killed VM's
	// threads, so a fresh os.Remove often fails with EBUSY. Retry briefly instead of
	// leaking the directory. Treat "already gone" as success.
	var err error
	for range 5 {
		err = os.Remove(c.path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("cgroup: destroy %s: %w", c.path, err)
}
