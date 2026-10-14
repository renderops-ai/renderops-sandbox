package handler

// --- Shared ---

type Resources struct {
	VCPUs    int `json:"vcpus,omitempty"`     //guest vcpus
	MemoryMB int `json:"memory_mb,omitempty"` // guestRAM, in MB
	DiskGB   int `json:"disk_gb,omitempty"`
}

// NetworkConfig controls the sandbox's network policy.
// Internet is a pointer so an omitted field means "default" (on) rather than false.
// (allowed_domains / expose_ports are intentionally not here yet — they need
// DNS-filtering and the preview-URL proxy respectively.)
type NetworkConfig struct {
	Internet *bool `json:"internet,omitempty"` // nil = default (on); false = egress blocked
}

type APIError struct {
	Status    string `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type TenantContext struct {
	TenantID     string `json:"tenant_id"`
	BillingModel string `json:"billing_model"`
}

type ExecutionOutput struct {
	Stdout            string  `json:"stdout"`
	Stderr            string  `json:"stderr"`
	ExitCode          int     `json:"exit_code"`
	TerminationReason string  `json:"termination_reason"`
	DurationMs        float64 `json:"duration_ms"`
	GuestDurationMs   float64 `json:"guest_duration_ms"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type UsageInfo struct {
	ExecutionTimeMs float64 `json:"execution_time_ms"`
	QueueWaitMs     float64 `json:"queue_wait_ms"`
	TimeoutLimitMs  int     `json:"timeout_limit_ms"`
}

// --- POST /session ---

type CreateSessionRequest struct {
	Name         string            `json:"name,omitempty"`     //dashboard-facing label; default "sandbox"
	OS           string            `json:"os,omitempty"`       // base OS; default "alpine" (see GET /v1/catalog)
	Template     string            `json:"template,omitempty"` // software on top of the OS; default "base"
	Image        string            `json:"image,omitempty"`    // legacy: image id, used when os/template are unset; default "alpine"
	Metadata     map[string]any    `json:"metadata,omitempty"` //user labels (purpose/owner/etc.) for list + filter
	Env          map[string]string `json:"env,omitempty"`
	Size         string            `json:"size,omitempty"`           // named size from the menu ("nano"...); the sk dashboard sends this
	Resources    *Resources        `json:"resources,omitempty"`      //nil = default size; wins over Size when both are set
	Network      *NetworkConfig    `json:"network,omitempty"`        //nil = default (internet on)
	IdleTimeoutS *int              `json:"idle_timeout_s,omitempty"` // nil = policy default; capped at max lifetime
	MaxLifetimeS *int              `json:"max_lifetime_s,omitempty"` // nil = policy default/cap
}

// --- POST /session/:id/exec ---

type ExecInSessionRequest struct {
	Command string `json:"command"`
	Timeout int    `json:"timeout,omitempty"`
}

type CreateSessionResponse struct {
	Status    string         `json:"status"`
	RequestID string         `json:"request_id"`
	Session   *SessionDetail `json:"session"`
	Limits    *SessionLimits `json:"limits"`
	Tenant    *TenantContext `json:"tenant"`
}

type SessionDetail struct {
	SessionID    string `json:"session_id"`
	Image        string `json:"image"`
	OS           string `json:"os,omitempty"`
	Template     string `json:"template,omitempty"`
	State        string `json:"state"`
	BillingModel string `json:"billing_model"`
	CreatedAt    string `json:"created_at"`
	LastUsed     string `json:"last_used"`
	ExpiresAt    string `json:"expires_at"`
}

type SessionLimits struct {
	MaxSessions    int `json:"max_sessions"`
	ActiveSessions int `json:"active_sessions"`
	MaxExecutionMs int `json:"max_execution_ms"`
	IdleTimeoutMs  int `json:"idle_timeout_ms"`
}

// --- POST /session/:id/run ---

type RunInSessionRequest struct {
	Code     string `json:"code"`
	Language string `json:"language"`
	Timeout  *int   `json:"timeout,omitempty"` // per-run timeout seconds; nil = policy default, capped at policy max
}

type SessionExecuteResponse struct {
	Status    string           `json:"status"`
	RequestID string           `json:"request_id"`
	SessionID string           `json:"session_id"`
	Result    *ExecutionOutput `json:"result,omitempty"`
	Error     *ErrorDetail     `json:"error,omitempty"`
	Usage     *UsageInfo       `json:"usage"`
	Tenant    *TenantContext   `json:"tenant"`
	Session   *SessionState    `json:"session"`
}

type SessionState struct {
	State     string `json:"state"`
	LastUsed  string `json:"last_used"`
	ExpiresAt string `json:"expires_at"`
	RunCount  int    `json:"run_count"`
}

// --- GET /session/:id ---

type SessionInfoResponse struct {
	Status    string         `json:"status"`
	RequestID string         `json:"request_id"`
	Session   *SessionDetail `json:"session"`
	Limits    *SessionLimits `json:"limits"`
	Stats     *SessionStats  `json:"stats"`
	Tenant    *TenantContext `json:"tenant"`
}

type SessionStats struct {
	RunCount         int     `json:"run_count"`
	TotalExecutionMs float64 `json:"total_execution_ms"`
	LastExitCode     *int    `json:"last_exit_code"`
}
