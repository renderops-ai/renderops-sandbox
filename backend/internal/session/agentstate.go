package session

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"backend/internal/plane"
)

const (
	// DefaultAgentBusyGrace is how recent a "working" agent state must be to
	// keep an idle sandbox awake. The agent's hooks rewrite the state on every
	// tool call, so an older one means the agent (or its hooks) stopped.
	DefaultAgentBusyGrace = 15 * time.Minute
	// agentBusyRecheck is how long a working agent defers the idle pause
	// before the reaper reads its state again.
	agentBusyRecheck = time.Minute
	// agentClockSkew is how far ahead of the guest clock updated_at may be
	// and still count as fresh.
	agentClockSkew = time.Minute
	// agentStateReadTimeoutSec bounds the guest command that reads the state.
	agentStateReadTimeoutSec = 5
)

// SetAgentBusyGrace sets how recent a "working" agent state must be to keep
// an idle sandbox awake (AGENT_BUSY_GRACE). Zero or less turns the check off:
// idle sandboxes pause regardless of their agent.
func (m *Manager) SetAgentBusyGrace(grace time.Duration) {
	m.agentBusyGrace.Store(int64(grace))
}

// AgentBusyGrace returns the grace set by SetAgentBusyGrace.
func (m *Manager) AgentBusyGrace() time.Duration {
	return time.Duration(m.agentBusyGrace.Load())
}

// agentKeepsAwake decides, from one read of the agent-state file, whether
// the sandbox's agent is working, so an idle pause would interrupt it. Only a
// "working" state written within grace (on the guest's clock) counts. A
// missing, unreadable or invalid file never keeps the sandbox awake. The
// returned reason is for logs.
func agentKeepsAwake(state plane.AgentState, readErr error, grace time.Duration) (bool, string) {
	switch {
	case grace <= 0:
		return false, "check_disabled"
	case errors.Is(readErr, plane.ErrAgentStateMissing):
		return false, "missing"
	case errors.Is(readErr, plane.ErrAgentStateInvalid):
		return false, "invalid"
	case readErr != nil:
		return false, "read_failed"
	case state.State != plane.AgentStateWorking:
		return false, state.State
	case state.Age > grace:
		return false, "stale"
	case state.Age < -agentClockSkew:
		return false, "future"
	}
	return true, plane.AgentStateWorking
}

// agentWorking is the reaper's check before an idle pause: it reports whether
// the sandbox's agent is working (agentKeepsAwake). The state file is read
// with a passive, quiet exec: it does not count as activity and stays out of
// the sandbox's logs. A working agent defers the pause by agentBusyRecheck;
// the reaper then reads the state again. This never extends the max
// lifetime: reapActionFor destroys an expired sandbox before any pause.
func (m *Manager) agentWorking(ctx context.Context, sessionID string, now time.Time) bool {
	grace := m.AgentBusyGrace()
	if grace <= 0 {
		return false
	}
	sess, ok := m.store.Get(sessionID)
	if !ok {
		return false
	}
	sess.mu.Lock()
	recheckAt := sess.agentRecheckAt
	sess.mu.Unlock()
	if now.Before(recheckAt) {
		return true
	}

	res, err := m.ExecWithOptions(ctx, sessionID, plane.AgentStateCommand, agentStateReadTimeoutSec,
		ExecOptions{ActiveOnly: true, Passive: true, Quiet: true})
	var state plane.AgentState
	if err == nil {
		state, err = plane.ParseAgentState(res.Stdout, now)
	}
	busy, reason := agentKeepsAwake(state, err, grace)
	log := slog.With("component", "reaper", "session_id", sessionID, "agent_state", reason)
	if !busy {
		switch {
		case errors.Is(err, plane.ErrAgentStateMissing), errors.Is(err, ErrNotActive):
			log.Debug("idle pause: no agent state")
		case errors.Is(err, plane.ErrAgentStateInvalid):
			log.Info("idle pause: agent state is invalid, pausing", "err", err)
		case err != nil:
			log.Warn("idle pause: agent state unreadable, pausing", "err", err)
		default:
			log.Info("idle pause: agent is not working, pausing", "updated_at", state.UpdatedAt,
				"age_s", int64(state.Age.Seconds()))
		}
		return false
	}

	sess.mu.Lock()
	sess.agentRecheckAt = now.Add(agentBusyRecheck)
	sess.mu.Unlock()
	log.Info("idle pause deferred: agent is working", "updated_at", state.UpdatedAt,
		"age_s", int64(state.Age.Seconds()), "grace", grace.String(), "recheck_in", agentBusyRecheck.String())
	return true
}
