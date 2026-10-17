package controlplane

import (
	"context"
	"testing"

	"backend/internal/platform"
)

type lostRows struct {
	ref    platform.SandboxRef
	found  bool
	reason string
}

func (r lostRows) GetSandbox(context.Context, string) (platform.SandboxRef, bool, error) {
	return r.ref, r.found, nil
}

func (r lostRows) SandboxLostReason(context.Context, string) (string, error) {
	return r.reason, nil
}

func TestLostSandboxReadsReason(t *testing.T) {
	rows := lostRows{
		ref:    platform.SandboxRef{ID: "sandbox-1", UserID: "user-1", State: "lost"},
		found:  true,
		reason: "VM crashed (out of memory)",
	}
	userID, reason, lost := lostSandbox(context.Background(), rows, "sandbox-1")
	if !lost || userID != "user-1" || reason != "VM crashed (out of memory)" {
		t.Fatalf("lostSandbox() = %q, %q, %v", userID, reason, lost)
	}

	rows.reason = ""
	if _, reason, _ := lostSandbox(context.Background(), rows, "sandbox-1"); reason != "VM is gone" {
		t.Fatalf("default reason = %q", reason)
	}

	for _, state := range []string{"active", "paused", "destroyed"} {
		rows.ref.State = state
		if _, _, lost := lostSandbox(context.Background(), rows, "sandbox-1"); lost {
			t.Fatalf("%s sandbox reported lost", state)
		}
	}
	if _, _, lost := lostSandbox(context.Background(), lostRows{}, "missing"); lost {
		t.Fatal("missing sandbox reported lost")
	}
}
