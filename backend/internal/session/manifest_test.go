package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPausedRecordPreservesImmutableRootfs(t *testing.T) {
	want := filepath.Join(t.TempDir(), "rootfs-version.ext4")
	session := &Session{ID: "sandbox-1", RootfsPathAtPause: want, CheckpointRef: "sandbox-checkpoints/sandbox-1/generations/generation-1/manifest.json"}
	recovered := recordFromSession(session).toSession()
	if recovered.RootfsPathAtPause != want {
		t.Fatalf("rootfs path = %q, want %q", recovered.RootfsPathAtPause, want)
	}
	if recovered.CheckpointRef != session.CheckpointRef {
		t.Fatalf("checkpoint ref = %q, want %q", recovered.CheckpointRef, session.CheckpointRef)
	}
}

func TestReadManifestRetainsDurableCheckpointWhenLocalFilesAreMissing(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "paused-sessions.json")
	record := pausedRecord{
		ID:               "sandbox-1",
		CheckpointRef:    "sandbox-checkpoints/sandbox-1/generations/generation-1/manifest.json",
		SnapPath:         "/missing/snap",
		MemPath:          "/missing/mem",
		WritableDiskPath: "/missing/disk",
	}
	data, err := json.Marshal([]pausedRecord{record})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sessions, err := readManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].CheckpointRef != record.CheckpointRef {
		t.Fatalf("durable paused session was dropped: %#v", sessions)
	}
}

func TestReadManifestRestoresFromBlobAfterHostReboot(t *testing.T) {
	if bootID() == "" {
		t.Skip("no boot id on this platform")
	}
	dir := t.TempDir()
	snap, mem := filepath.Join(dir, "snap"), filepath.Join(dir, "mem")
	for _, name := range []string{snap, mem} {
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifestPath := filepath.Join(dir, "paused-sessions.json")
	records := []pausedRecord{
		{ID: "rebooted", CheckpointRef: "ref", SnapPath: snap, MemPath: mem, BootID: "an-earlier-boot"},
		{ID: "same-boot", CheckpointRef: "ref", SnapPath: snap, MemPath: mem, BootID: bootID()},
	}
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sessions, err := readManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 {
		t.Fatalf("got %d sessions, want 2", len(sessions))
	}
	if sessions[0].SnapPath != "" || sessions[0].MemPath != "" {
		t.Fatalf("pause files from an earlier boot were kept: %q %q", sessions[0].SnapPath, sessions[0].MemPath)
	}
	if sessions[1].SnapPath != snap || sessions[1].MemPath != mem {
		t.Fatalf("pause files from this boot were dropped: %q %q", sessions[1].SnapPath, sessions[1].MemPath)
	}
}
