package session

import "fmt"

// SetGitAccessRevoker is configured before serving requests. revoke drops any
// read-only git grant bound to a sandbox's network slot. It runs before the
// slot is released on pause and destroy, so a reused slot never inherits
// another sandbox's grant.
func (m *Manager) SetGitAccessRevoker(revoke func(string)) {
	m.gitMu.Lock()
	defer m.gitMu.Unlock()
	m.gitRevoke = revoke
}

func (m *Manager) revokeGitAccess(id string) {
	m.gitMu.RLock()
	revoke := m.gitRevoke
	m.gitMu.RUnlock()
	if revoke != nil {
		revoke(id)
	}
}

// BindGitAccess calls bind with the sandbox's current network slot while it is
// active. It holds the session lock (serializing with pause, which revokes
// under that lock) and gitBindMu (serializing with destroy, which removes the
// session and revokes under it), so a grant can never be bound to a slot that
// is being released. It waits for a running exec to finish.
func (m *Manager) BindGitAccess(id string, bind func(slot int) error) error {
	sess, ok := m.store.Get(id)
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	m.gitBindMu.Lock()
	defer m.gitBindMu.Unlock()
	if current, ok := m.store.Get(id); !ok || current != sess {
		return fmt.Errorf("session %s not found", id)
	}
	if sess.State != StateActive || sess.VM == nil || sess.VM.Slot < 0 {
		return fmt.Errorf("session %s is not active", id)
	}
	return bind(sess.VM.Slot)
}
