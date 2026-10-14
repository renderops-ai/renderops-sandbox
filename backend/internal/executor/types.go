package executor

type ExecutionResult struct {
	OutputLogsCaptured bool    `json:"output_logs_captured,omitempty"`
	Stdout             string  `json:"stdout"`
	Stderr             string  `json:"stderr"`
	Duration           float64 `json:"duration"`
	GuestDuration      float64 `json:"guest_duration"` // guest-side execution time from vsock resp
	ExitCode           int64   `json:"exit_code"`
	TerminationReason  string  `json:"termination_reason,omitempty"`
}
