package session

import (
	"errors"
	"testing"

	"backend/internal/executor/firecracker"
)

func TestBindSlotGrantBindsOnlyTheLiveSlotOfAnActiveSandbox(t *testing.T) {
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
	if err := m.BindSlotGrant("active", func(slot int) error { boundSlot = slot; return nil }); err != nil || boundSlot != 5 {
		t.Fatalf("active: slot=%d err=%v", boundSlot, err)
	}
	for _, id := range []string{"paused", "cold", "missing"} {
		called := false
		if err := m.BindSlotGrant(id, func(int) error { called = true; return nil }); err == nil || called {
			t.Errorf("%s: err=%v called=%v, want refusal without binding", id, err, called)
		}
	}

	bindErr := errors.New("listener failed")
	if err := m.BindSlotGrant("active", func(int) error { return bindErr }); !errors.Is(err, bindErr) {
		t.Fatalf("bind error not returned: %v", err)
	}
}

func TestBindSlotGrantRefusesASessionRemovedWhileWaiting(t *testing.T) {
	m := &Manager{store: NewStore(10)}
	sess := &Session{ID: "sbx", State: StateActive, VM: &firecracker.MicroVM{Slot: 2}}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}
	// Hold the session lock (a running exec) while destroy removes the session.
	sess.mu.Lock()
	done := make(chan error, 1)
	called := false
	go func() { done <- m.BindSlotGrant("sbx", func(int) error { called = true; return nil }) }()
	m.slotBindMu.Lock()
	m.store.Delete("sbx")
	m.slotBindMu.Unlock()
	sess.mu.Unlock()
	if err := <-done; err == nil || called {
		t.Fatalf("bound a removed session: err=%v called=%v", err, called)
	}
}

func TestEveryRegisteredProxyIsRevoked(t *testing.T) {
	m := &Manager{}
	m.revokeSlotGrants("sbx") // no revoker configured: no-op
	var revoked []string
	m.AddSlotRevoker(func(id string) { revoked = append(revoked, "git:"+id) })
	m.AddSlotRevoker(func(id string) { revoked = append(revoked, "model:"+id) })
	m.revokeSlotGrants("sbx")
	if len(revoked) != 2 || revoked[0] != "git:sbx" || revoked[1] != "model:sbx" {
		t.Fatalf("revoked = %v", revoked)
	}
}
