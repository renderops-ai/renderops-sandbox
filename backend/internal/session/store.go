package session

import (
	"backend/internal/cgroup"
	"backend/internal/executor/firecracker"
	"fmt"
	"net"
	"sync"
	"time"
)

// SessionState is the lifecycle state of a session.
type SessionState string

const (
	StateActive SessionState = "active" // live VM holding a slot + RAM
	StatePaused SessionState = "paused" // snapshotted to disk; no live VM, slot/RAM freed
	// StateLost: the VM died (or could not be paused before the worker shut
	// down). Nothing is left to resume; the session is out of the store and
	// only a lost record remains (see lost.go).
	StateLost SessionState = "lost"
)

type Session struct {
	ID               string
	UserID           string // owning tenant — used to sync state + timeline events to the DB
	Image            string // immutable template image id used to boot this sandbox
	VM               *firecracker.MicroVM
	Cgroup           *cgroup.Cgroup
	BillingModel     string
	VCPUs            int // allocated compute shape — the billing basis
	MemoryMB         int
	DiskGB           int
	Env              map[string]string // env vars, re-injected into the fresh shell on resume
	Internet         bool              // egress allowed (false = network-isolated)
	IdleTimeout      time.Duration     // reaper pauses after this idle time (per-session)
	MaxLifetime      time.Duration     // hard ceiling regardless of activity (0 = no limit)
	CreatedAt        time.Time
	LastUsed         time.Time
	RunCount         int
	TotalExecutionMs float64
	LastExitCode     *int
	mu               sync.Mutex            // serializes concurrent runs + pause/resume on same session
	PooledVM         *firecracker.PooledVM // non-nil when VM came from a pool (active only)
	Pool             *firecracker.VMPool   // which pool the VM came from (active only)
	VsockConn        net.Conn              // persistent vsock connection, reused across all calls
	Terminals        map[string]struct{}   // active guest PTYs, guarded by mu
	agentRecheckAt   time.Time             // a working agent defers the idle pause until then (see agentWorking), guarded by mu

	// Auto-pause state. When State == StatePaused the VM is gone and these describe how
	// to resume it: restore from SnapPath/MemPath, reattach WritableDiskPath in place,
	// re-bake VsockPathAtPause + TapNameAtPause. All persisted to the recovery manifest.
	State             SessionState
	PausedAt          time.Time
	SnapPath          string // per-session VM-state snapshot file (on disk, not /dev/shm)
	MemPath           string // per-session guest-RAM snapshot file
	WritableDiskPath  string // the session's writable upper disk — survives across pause
	RootfsPathAtPause string // immutable lower rootfs used by this snapshot
	VsockPathAtPause  string // vsock UDS path baked into the session snapshot
	TapNameAtPause    string // host TAP name baked into the session snapshot
	CheckpointRef     string // durable checkpoint manifest key; local paths remain the phase-1 resume source
	BootIDAtPause     string // host boot the local pause files were written in (see readManifest)
}

type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	max      int // hard cap for each session = one live vm = ram cost
}

func NewStore(max int) *Store {
	return &Store{
		sessions: make(map[string]*Session),
		max:      max,
	}
}

func (s *Store) Add(sess *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.sessions[sess.ID]; exists {
		return fmt.Errorf("session %s already exists", sess.ID)
	}
	if len(s.sessions) >= s.max {
		return fmt.Errorf("session limit reached (%d)", s.max)
	}

	s.sessions[sess.ID] = sess
	return nil
}

func (s *Store) Get(id string) (*Session, bool) {
	s.mu.RLock()

	defer s.mu.RUnlock()

	sess, ok := s.sessions[id]

	return sess, ok
}

func (s *Store) Delete(id string) (*Session, bool) {
	s.mu.Lock()

	defer s.mu.Unlock()

	sess, ok := s.sessions[id]

	if ok {
		delete(s.sessions, id)
	}

	return sess, ok
}

// EvictIdle returns sessions idle longer than their own IdleTimeout OR alive longer than
// their own MaxLifetime, and removes them from the store. A zero value on a session
// disables that check for that session.
func (s *Store) EvictIdle() []*Session {
	s.mu.Lock()
	defer s.mu.Unlock()

	var evicted []*Session
	for id, sess := range s.sessions {
		idleExpired := sess.IdleTimeout > 0 && time.Since(sess.LastUsed) > sess.IdleTimeout
		lifetimeExpired := sess.MaxLifetime > 0 && time.Since(sess.CreatedAt) > sess.MaxLifetime
		if idleExpired || lifetimeExpired {
			evicted = append(evicted, sess)
			delete(s.sessions, id)
		}
	}
	return evicted
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

func (s *Store) All() []*Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		out = append(out, sess)
	}
	return out
}
