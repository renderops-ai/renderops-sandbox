package plane

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestParseAgentState(t *testing.T) {
	hostNow := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	// A guest restored from a snapshot runs behind the host.
	guestNow := hostNow.Add(-3 * time.Hour)
	clock := fmt.Sprintf("%d\n", guestNow.Unix())

	tests := []struct {
		name    string
		stdout  string
		want    AgentState
		wantErr error
	}{
		{
			name:   "age is measured on the guest clock",
			stdout: clock + `{"state":"working","updated_at":"` + guestNow.Add(-2*time.Minute).Format(time.RFC3339) + `"}` + "\n",
			want:   AgentState{State: AgentStateWorking, UpdatedAt: guestNow.Add(-2 * time.Minute), Age: 2 * time.Minute},
		},
		{
			name:   "fractional seconds, offset and extra fields",
			stdout: clock + `{"state":"waiting","updated_at":"` + guestNow.Add(-time.Second).In(time.FixedZone("x", 3600)).Format(time.RFC3339Nano) + `","session":"s1"}`,
			want:   AgentState{State: AgentStateWaiting, UpdatedAt: guestNow.Add(-time.Second), Age: time.Second},
		},
		{
			name:   "no guest clock falls back to now",
			stdout: `{"state":"done","updated_at":"` + hostNow.Add(-time.Minute).Format(time.RFC3339) + `"}`,
			want:   AgentState{State: AgentStateDone, UpdatedAt: hostNow.Add(-time.Minute), Age: time.Minute},
		},
		{name: "missing file", stdout: clock, wantErr: ErrAgentStateMissing},
		{name: "missing file, clock without newline", stdout: fmt.Sprint(guestNow.Unix()), wantErr: ErrAgentStateMissing},
		{name: "empty output", stdout: "", wantErr: ErrAgentStateMissing},
		{name: "malformed json", stdout: clock + `{"state":"working",`, wantErr: ErrAgentStateInvalid},
		{name: "not an object", stdout: clock + `"working"`, wantErr: ErrAgentStateInvalid},
		{name: "unknown state", stdout: clock + `{"state":"thinking","updated_at":"2026-10-17T09:00:00Z"}`, wantErr: ErrAgentStateInvalid},
		{name: "missing updated_at", stdout: clock + `{"state":"working"}`, wantErr: ErrAgentStateInvalid},
		{name: "updated_at not RFC3339", stdout: clock + `{"state":"working","updated_at":"1760695200"}`, wantErr: ErrAgentStateInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAgentState(tt.stdout, hostNow)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.State != tt.want.State || !got.UpdatedAt.Equal(tt.want.UpdatedAt) || got.Age != tt.want.Age {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
