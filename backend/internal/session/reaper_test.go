package session

import (
	"testing"
	"time"
)

func TestReapActionFor(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		sess Session
		want reapAction
	}{
		{
			name: "active session reaches idle timeout",
			sess: Session{State: StateActive, LastUsed: now.Add(-5 * time.Minute), IdleTimeout: 5 * time.Minute},
			want: reapPause,
		},
		{
			name: "recently active session remains active",
			sess: Session{State: StateActive, LastUsed: now.Add(-time.Minute), IdleTimeout: 5 * time.Minute},
			want: reapNone,
		},
		{
			name: "open terminal prevents idle pause",
			sess: Session{State: StateActive, LastUsed: now.Add(-10 * time.Minute), IdleTimeout: 5 * time.Minute, Terminals: map[string]struct{}{"terminal-1": {}}},
			want: reapNone,
		},
		{
			name: "maximum lifetime overrides open terminal",
			sess: Session{State: StateActive, CreatedAt: now.Add(-time.Hour), MaxLifetime: time.Hour, Terminals: map[string]struct{}{"terminal-1": {}}},
			want: reapDestroy,
		},
		{
			name: "working agent never extends the maximum lifetime",
			sess: Session{State: StateActive, CreatedAt: now.Add(-time.Hour), MaxLifetime: time.Hour, LastUsed: now.Add(-10 * time.Minute), IdleTimeout: 5 * time.Minute, agentRecheckAt: now.Add(time.Minute)},
			want: reapDestroy,
		},
		{
			name: "paused session reaches retention ttl",
			sess: Session{State: StatePaused, PausedAt: now.Add(-pauseTTL)},
			want: reapDestroy,
		},
		{
			name: "paused session remains retained",
			sess: Session{State: StatePaused, PausedAt: now.Add(-pauseTTL + time.Minute)},
			want: reapNone,
		},
	}

	for i := range tests {
		tt := &tests[i]
		t.Run(tt.name, func(t *testing.T) {
			if got := reapActionFor(&tt.sess, now); got != tt.want {
				t.Fatalf("reapActionFor() = %v, want %v", got, tt.want)
			}
		})
	}
}
