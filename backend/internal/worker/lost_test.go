package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"backend/internal/agent"
	"backend/internal/plane"
)

// lostSessionService is a session service whose sandbox's VM died.
type lostSessionService struct {
	fakeSessionService
	reason string
}

func (s *lostSessionService) Pause(_ context.Context, id string) error {
	return fmt.Errorf("snapshot for pause: %w", &plane.LostError{Reason: s.reason})
}

func (s *lostSessionService) LostReason(string) (string, bool) { return s.reason, true }

// A pause or resume of a lost sandbox reaches the control plane as a typed
// lost error with the reason, not "operation_failed ... not found".
func TestLostSandboxCrossesTheWireTyped(t *testing.T) {
	svc := &lostSessionService{reason: "VM crashed (out of memory)"}
	server := httptest.NewServer(NewServer(svc, "token", 4).Handler())
	defer server.Close()
	client := agent.NewClient(server.URL, "token")

	for name, call := range map[string]func() error{
		"pause":  func() error { return client.Pause(context.Background(), "sandbox-1") },
		"resume": func() error { return client.Resume(context.Background(), "sandbox-1") },
	} {
		err := call()
		var lost *plane.LostError
		if !errors.As(err, &lost) || lost.Reason != "VM crashed (out of memory)" {
			t.Fatalf("%s error = %v, want lost with the reason", name, err)
		}
	}
}
