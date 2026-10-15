package session

import "fmt"

// Slot grants bind a worker-side proxy (git, model inference) to a sandbox's
// network slot so the guest can reach it on its gateway without holding a
// credential. A grant must never outlive the slot: slots are reused.

// AddSlotRevoker is configured before serving requests. revoke drops any
// grant a proxy has bound to the sandbox's slot. Every revoker runs before
// the slot is released on pause and destroy, so a reused slot never inherits
// another sandbox's grant.
func (m *Manager) AddSlotRevoker(revoke func(string)) {
	m.slotGrantMu.Lock()
	defer m.slotGrantMu.Unlock()
	m.slotRevokers = append(m.slotRevokers, revoke)
}

func (m *Manager) revokeSlotGrants(id string) {
	m.slotGrantMu.RLock()
	revokers := m.slotRevokers
	m.slotGrantMu.RUnlock()
	for _, revoke := range revokers {
		revoke(id)
	}
}

// BindSlotGrant calls bind with the sandbox's current network slot while it is
// active. It holds the session lock (serializing with pause, which revokes
// under that lock) and slotBindMu (serializing with destroy, which removes the
// session and revokes under it), so a grant can never be bound to a slot that
// is being released. It waits for a running exec to finish.
func (m *Manager) BindSlotGrant(id string, bind func(slot int) error) error {
	sess, ok := m.store.Get(id)
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	m.slotBindMu.Lock()
	defer m.slotBindMu.Unlock()
	if current, ok := m.store.Get(id); !ok || current != sess {
		return fmt.Errorf("session %s not found", id)
	}
	if sess.State != StateActive || sess.VM == nil || sess.VM.Slot < 0 {
		return fmt.Errorf("session %s is not active", id)
	}
	return bind(sess.VM.Slot)
}
