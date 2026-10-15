package session

import (
	"errors"
	"testing"

	"backend/internal/executor/firecracker"
)

func TestBindGitAccessBindsOnlyTheLiveSlotOfAnActiveSandbox(t *testing.T) {
	m := &Manager{store: NewStore(10)}
	active := &Session{ID: "active", State: StateActive, VM: &firecracker.MicroVM{Slot: 5}}
	paused := &Session{ID: "paused", State: StatePaused}
	coldBoot := &Session{ID: "cold", State: StateActive, VM: &firecracker.MicroVM{Slot: -1}}
	for _, sess := range []*Session{active, paused, coldBoot} {
		if err := m.store.Add(sess); err != nil {
			t.Fatal(err)
		}
	}

	var boundSlot = -1
	if err := m.BindGitAccess("active", func(slot int) error { boundSlot = slot; return nil }); err != nil || boundSlot != 5 {
		t.Fatalf("active: slot=%d err=%v", boundSlot, err)
	}
	for _, id := range []string{"paused", "cold", "missing"} {
		called := false
		if err := m.BindGitAccess(id, func(int) error { called = true; return nil }); err == nil || called {
			t.Errorf("%s: err=%v called=%v, want refusal without binding", id, err, called)
		}
	}

	bindErr := errors.New("listener failed")
	if err := m.BindGitAccess("active", func(int) error { return bindErr }); !errors.Is(err, bindErr) {
		t.Fatalf("bind error not returned: %v", err)
	}
}

func TestBindGitAccessRefusesASessionRemovedWhileWaiting(t *testing.T) {
	m := &Manager{store: NewStore(10)}
	sess := &Session{ID: "sbx", State: StateActive, VM: &firecracker.MicroVM{Slot: 2}}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}
	// Hold the session lock (a running exec) while destroy removes the session.
	sess.mu.Lock()
	done := make(chan error, 1)
	called := false
	go func() { done <- m.BindGitAccess("sbx", func(int) error { called = true; return nil }) }()
	m.gitBindMu.Lock()
	m.store.Delete("sbx")
	m.gitBindMu.Unlock()
	sess.mu.Unlock()
	if err := <-done; err == nil || called {
		t.Fatalf("bound a removed session: err=%v called=%v", err, called)
	}
}

func TestRevokerHook(t *testing.T) {
	m := &Manager{}
	m.revokeGitAccess("sbx") // no revoker configured: no-op
	var revoked []string
	m.SetGitAccessRevoker(func(id string) { revoked = append(revoked, id) })
	m.revokeGitAccess("sbx")
	if len(revoked) != 1 || revoked[0] != "sbx" {
		t.Fatalf("revoked = %v", revoked)
	}
}
