package plane

import (
	"errors"
	"time"
)

var ErrNoCapacity = errors.New("worker has insufficient capacity")

// ErrSandboxNotActive means the sandbox is not running (paused, resuming, ...)
// and the command asked not to resume it (ExecRequest.ActiveOnly).
var ErrSandboxNotActive = errors.New("sandbox is not active")

// ErrSandboxLost means the sandbox's VM is gone (it crashed, or its worker no
// longer has it) and the sandbox cannot be resumed; it can only be removed.
var ErrSandboxLost = errors.New("sandbox lost")

// LostError is ErrSandboxLost with the reason the VM is gone, e.g. "VM crashed
// (out of memory)".
type LostError struct {
	Reason string
}

func (e *LostError) Error() string {
	if e.Reason == "" {
		return ErrSandboxLost.Error()
	}
	return ErrSandboxLost.Error() + ": " + e.Reason
}

func (e *LostError) Is(target error) bool { return target == ErrSandboxLost }

// SessionInfo is the plane-neutral view of a sandbox session — everything the
// control plane may know about one. Host-side runtime state (VM handles,
// snapshots, vsock) lives in internal/session on top of this.
type SessionInfo struct {
	ID               string
	UserID           string
	Image            string
	BillingModel     string
	VCPUs            int
	MemoryMB         int
	DiskGB           int
	Env              map[string]string
	Internet         bool
	IdleTimeout      time.Duration
	MaxLifetime      time.Duration
	CreatedAt        time.Time
	LastUsed         time.Time
	PausedAt         time.Time
	State            SessionState
	RunCount         int
	TotalExecutionMs float64
	LastExitCode     *int
}

// CreateRequest is what the control plane sends to boot a sandbox on an agent.
type CreateRequest struct {
	SandboxID    string            `json:"sandbox_id"`
	UserID       string            `json:"user_id"`
	Image        string            `json:"image"`
	BillingModel string            `json:"billing_model"`
	Env          map[string]string `json:"env,omitempty"`
	VCPUs        int               `json:"vcpus"`
	MemoryMB     int               `json:"memory_mb"`
	DiskGB       int               `json:"disk_gb"`
	Internet     bool              `json:"internet"`
	IdleTimeoutS int               `json:"idle_timeout_s,omitempty"`
	MaxLifetimeS int               `json:"max_lifetime_s,omitempty"`
}

// CreateResponse is the agent's reply after booting a sandbox.
type CreateResponse struct {
	SandboxID string `json:"sandbox_id"`
	State     string `json:"state"`
	Image     string `json:"image"`
	VCPUs     int    `json:"vcpus"`
	MemoryMB  int    `json:"memory_mb"`
	DiskGB    int    `json:"disk_gb"`
}

// GitAccessRequest grants a sandbox read-only fetch access to one GitHub
// repository through the worker's git proxy. Token is a short-lived
// installation token narrowed to that repository with contents:read; the
// worker keeps it in memory only and never forwards it to the guest.
type GitAccessRequest struct {
	Repository string    `json:"repository"` // owner/name of the base repository
	Token      string    `json:"token"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// GitAccessResponse is the credential-free remote the guest fetches from.
type GitAccessResponse struct {
	RemoteURL string    `json:"remote_url"`
	ExpiresAt time.Time `json:"expires_at"`
}

// ModelAccessRequest lets a sandbox call one model through the worker's model
// proxy. Token is a random proxy token the guest presents; it is not a
// provider key, which stays on the control plane.
type ModelAccessRequest struct {
	Provider  string    `json:"provider"` // "anthropic"
	Model     string    `json:"model"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// ModelAccessResponse is the address the guest's agent calls.
type ModelAccessResponse struct {
	BaseURL   string    `json:"base_url"`
	ExpiresAt time.Time `json:"expires_at"`
}

// RunRequest runs code in a sandbox.
type RunRequest struct {
	Code     string `json:"code"`
	Language string `json:"language"`
	TimeoutS int    `json:"timeout_s,omitempty"`
}

// ExecRequest runs a shell command in a sandbox.
type ExecRequest struct {
	Command  string `json:"command"`
	TimeoutS int    `json:"timeout_s,omitempty"`
	// ActiveOnly refuses a sandbox that is not running (409
	// sandbox_not_active) instead of resuming a paused one first.
	ActiveOnly bool `json:"active_only,omitempty"`
	// Passive does not count the command as activity: the sandbox's idle
	// timer, run count and last exit code are left alone.
	Passive bool `json:"passive,omitempty"`
	// Quiet keeps the command's output out of the sandbox's log stream
	// (sandbox_logs); the output is still returned. For platform-internal
	// commands such as file reads and writes, agent polls and setup. A guest
	// agent built before this option ignores it and logs the output as before.
	Quiet bool `json:"quiet,omitempty"`
}

// ExecOptions are the per-command options a caller of the control plane's
// ExecIfActive picks; see ExecRequest for each.
type ExecOptions struct {
	Passive bool
	Quiet   bool
}

// ExecResult is the wire form of an execution outcome. Field tags stay in
// lockstep with executor.ExecutionResult, which the agent encodes directly.
type ExecResult struct {
	OutputLogsCaptured bool    `json:"output_logs_captured,omitempty"`
	Stdout             string  `json:"stdout"`
	Stderr             string  `json:"stderr"`
	Duration           float64 `json:"duration"`
	GuestDuration      float64 `json:"guest_duration"`
	ExitCode           int64   `json:"exit_code"`
	TerminationReason  string  `json:"termination_reason,omitempty"`
}

// Capacity is the worker-authoritative resource snapshot published to the
// orchestrator. Allocatable values include configured overcommit.
type Capacity struct {
	FreeSlots           int `json:"free_slots"`
	MaxSlots            int `json:"max_slots"`
	AllocatableVCPUs    int `json:"allocatable_vcpus"`
	AllocatableMemoryMB int `json:"allocatable_memory_mb"`
	AllocatableDiskGB   int `json:"allocatable_disk_gb"`
	ReservedVCPUs       int `json:"reserved_vcpus"`
	ReservedMemoryMB    int `json:"reserved_memory_mb"`
	ReservedDiskGB      int `json:"reserved_disk_gb"`
	ReservedSandboxes   int `json:"reserved_sandboxes"`
}

// ErrorResponse is a structured agent error.
type ErrorResponse struct {
	Code  string `json:"code"`
	Error string `json:"error"`
	// Reason is why a sandbox was lost (code "sandbox_lost").
	Reason string `json:"reason,omitempty"`
}
