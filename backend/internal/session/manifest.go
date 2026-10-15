package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// manifest persists the set of PAUSED sessions to disk so they survive a backend
// process restart (deploy, reboot) — the snapshot + writable disk are already on disk;
// the manifest is the index that lets a fresh process find and resume them. When
// configured, CheckpointRef also identifies the durable Blob generation.
//
// Local artifacts are preferred. If they are missing but CheckpointRef is present, the
// durable checkpoint reader hydrates them before Firecracker restore.

type pausedRecord struct {
	ID                string            `json:"id"`
	UserID            string            `json:"user_id"`
	Image             string            `json:"image,omitempty"`
	BillingModel      string            `json:"billing_model"`
	LegacyTier        string            `json:"tier,omitempty"` // read-only compatibility for pre-PAYG manifests
	VCPUs             int               `json:"vcpus"`
	MemoryMB          int               `json:"memory_mb"`
	DiskGB            int               `json:"disk_gb"`
	Env               map[string]string `json:"env,omitempty"`
	Internet          bool              `json:"internet"`
	IdleTimeoutNs     int64             `json:"idle_timeout_ns"`
	MaxLifetimeNs     int64             `json:"max_lifetime_ns"`
	CreatedAt         time.Time         `json:"created_at"`
	PausedAt          time.Time         `json:"paused_at"`
	SnapPath          string            `json:"snap_path"`
	MemPath           string            `json:"mem_path"`
	WritableDiskPath  string            `json:"writable_disk_path"`
	RootfsPathAtPause string            `json:"rootfs_path_at_pause,omitempty"`
	VsockPathAtPause  string            `json:"vsock_path_at_pause"`
	TapNameAtPause    string            `json:"tap_name_at_pause"`
	CheckpointRef     string            `json:"checkpoint_ref,omitempty"`
	BootID            string            `json:"boot_id,omitempty"`
}

func recordFromSession(s *Session) pausedRecord {
	return pausedRecord{
		ID:                s.ID,
		UserID:            s.UserID,
		Image:             s.Image,
		BillingModel:      s.BillingModel,
		VCPUs:             s.VCPUs,
		MemoryMB:          s.MemoryMB,
		DiskGB:            s.DiskGB,
		Env:               s.Env,
		Internet:          s.Internet,
		IdleTimeoutNs:     int64(s.IdleTimeout),
		MaxLifetimeNs:     int64(s.MaxLifetime),
		CreatedAt:         s.CreatedAt,
		PausedAt:          s.PausedAt,
		SnapPath:          s.SnapPath,
		MemPath:           s.MemPath,
		WritableDiskPath:  s.WritableDiskPath,
		RootfsPathAtPause: s.RootfsPathAtPause,
		VsockPathAtPause:  s.VsockPathAtPause,
		TapNameAtPause:    s.TapNameAtPause,
		CheckpointRef:     s.CheckpointRef,
		BootID:            s.BootIDAtPause,
	}
}

func (r pausedRecord) toSession() *Session {
	billingModel := r.BillingModel
	image := r.Image
	if image == "" {
		image = "alpine"
	}
	if billingModel == "" {
		billingModel = r.LegacyTier
	}
	if billingModel == "" {
		billingModel = "payg"
	}
	return &Session{
		ID:                r.ID,
		UserID:            r.UserID,
		Image:             image,
		BillingModel:      billingModel,
		VCPUs:             r.VCPUs,
		MemoryMB:          r.MemoryMB,
		DiskGB:            r.DiskGB,
		Env:               r.Env,
		Internet:          r.Internet,
		IdleTimeout:       time.Duration(r.IdleTimeoutNs),
		MaxLifetime:       time.Duration(r.MaxLifetimeNs),
		CreatedAt:         r.CreatedAt,
		LastUsed:          r.PausedAt,
		State:             StatePaused,
		PausedAt:          r.PausedAt,
		SnapPath:          r.SnapPath,
		MemPath:           r.MemPath,
		WritableDiskPath:  r.WritableDiskPath,
		RootfsPathAtPause: r.RootfsPathAtPause,
		VsockPathAtPause:  r.VsockPathAtPause,
		TapNameAtPause:    r.TapNameAtPause,
		CheckpointRef:     r.CheckpointRef,
		BootIDAtPause:     r.BootID,
	}
}

// writeManifest atomically rewrites the manifest with the given paused sessions.
func writeManifest(path string, paused []*Session) error {
	records := make([]pausedRecord, 0, len(paused))
	for _, s := range paused {
		records = append(records, recordFromSession(s))
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir manifest dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write manifest tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}

// readManifest loads paused-session records. A missing file is not an error. Records with
// missing local artifacts remain recoverable when they carry a durable CheckpointRef.
func readManifest(path string) ([]*Session, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var records []pausedRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	sessions := make([]*Session, 0, len(records))
	currentBoot := bootID()
	for _, r := range records {
		// Pause memory files are not fsynced (the Blob checkpoint is the durable copy),
		// so after a host reboot they may be incomplete: restore from Blob instead.
		if r.BootID != "" && currentBoot != "" && r.BootID != currentBoot && r.CheckpointRef != "" {
			r.SnapPath, r.MemPath = "", ""
		}
		if (!fileExists(r.SnapPath) || !fileExists(r.MemPath) || !fileExists(r.WritableDiskPath)) && r.CheckpointRef == "" {
			continue
		}
		sessions = append(sessions, r.toSession())
	}
	return sessions, nil
}

// bootID identifies the current host boot, or is empty if unknown.
func bootID() string {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}
