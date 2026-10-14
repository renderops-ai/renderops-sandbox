package session

// SetLogCollector is configured before serving requests. Log subscriptions use
// a dedicated vsock connection and never take the execution lock or update TTL.
func (m *Manager) SetLogCollector(start func(string, string), stop func(string)) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	m.logStart, m.logStop = start, stop
}

func (m *Manager) logsEnabled() bool {
	m.logMu.RLock()
	defer m.logMu.RUnlock()
	return m.logStart != nil
}

func (m *Manager) startLogs(id, path string) {
	m.logMu.RLock()
	start := m.logStart
	m.logMu.RUnlock()
	if start != nil {
		start(id, path)
	}
}

func (m *Manager) stopLogs(id string) {
	m.logMu.RLock()
	stop := m.logStop
	m.logMu.RUnlock()
	if stop != nil {
		stop(id)
	}
}
