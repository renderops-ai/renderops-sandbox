package plane

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AgentStatePath is the file the agent's hooks in the guest (installed by the
// spawn CLI) keep up to date while a persistent agent session runs:
//
//	{"state":"working|waiting|done|idle","updated_at":"<RFC3339>"}
const AgentStatePath = "/var/lib/spawn/agent-state.json"

// AgentStateCommand reads the agent-state file in the guest. It prints the
// guest's clock (Unix seconds) on the first line, then at most 4 KiB of the
// file (the guest writes it, so its size is not trusted), and nothing when
// the file is missing. It always exits 0. The guest clock is read in the same
// command because a guest restored from a snapshot can run behind the host:
// updated_at is only compared with the clock that wrote it.
const AgentStateCommand = "date -u +%s; head -c 4096 " + AgentStatePath + " 2>/dev/null; true"

// The states the agent's hooks write. AgentStateUnknown is the platform's own:
// there is no file, or it is not valid.
const (
	AgentStateWorking = "working"
	AgentStateWaiting = "waiting"
	AgentStateDone    = "done"
	AgentStateIdle    = "idle"
	AgentStateUnknown = "unknown"
)

var (
	// ErrAgentStateMissing means the guest has no agent-state file.
	ErrAgentStateMissing = errors.New("agent state file is missing")
	// ErrAgentStateInvalid means the file is not a known state with an
	// RFC3339 updated_at.
	ErrAgentStateInvalid = errors.New("agent state file is invalid")
)

// AgentState is one reading of the guest's agent-state file.
type AgentState struct {
	State     string
	UpdatedAt time.Time
	// Age is how long before the read UpdatedAt was, on the guest's clock.
	// It is negative when UpdatedAt is ahead of that clock.
	Age time.Duration
}

// ParseAgentState parses AgentStateCommand's output. now stands in for the
// guest clock when the output does not start with one.
func ParseAgentState(stdout string, now time.Time) (AgentState, error) {
	guestNow, body := now, stdout
	clock, rest, _ := strings.Cut(stdout, "\n")
	if seconds, err := strconv.ParseInt(strings.TrimSpace(clock), 10, 64); err == nil {
		guestNow, body = time.Unix(seconds, 0), rest
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return AgentState{}, ErrAgentStateMissing
	}

	var file struct {
		State     string `json:"state"`
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal([]byte(body), &file); err != nil {
		return AgentState{}, fmt.Errorf("%w: %w", ErrAgentStateInvalid, err)
	}
	switch file.State {
	case AgentStateWorking, AgentStateWaiting, AgentStateDone, AgentStateIdle:
	default:
		return AgentState{}, fmt.Errorf("%w: unknown state", ErrAgentStateInvalid)
	}
	updatedAt, err := time.Parse(time.RFC3339, file.UpdatedAt)
	if err != nil {
		return AgentState{}, fmt.Errorf("%w: updated_at: %w", ErrAgentStateInvalid, err)
	}
	return AgentState{State: file.State, UpdatedAt: updatedAt, Age: guestNow.Sub(updatedAt)}, nil
}
