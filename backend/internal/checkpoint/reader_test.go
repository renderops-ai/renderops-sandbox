package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"backend/internal/template"

	"github.com/google/uuid"
)

func TestWriterReaderRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, err := template.NewLocalStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	sandboxID := uuid.NewString()
	sourceDir := t.TempDir()
	snapshotPath := writeTestFile(t, sourceDir, "snap", "vm-state")
	memoryPath := writeTestFile(t, sourceDir, "mem", "guest-memory")
	diskPath := filepath.Join(sourceDir, "writable.ext4")
	disk, err := os.Create(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.Truncate(12 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.WriteAt([]byte("first"), 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.WriteAt([]byte("last"), 9*1024*1024); err != nil {
		t.Fatal(err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	manifestKey, err := writer.Save(ctx, Input{
		SandboxID: sandboxID, VCPUs: 1, MemoryMB: 128, DiskGB: 1,
		RootfsPath: "/assets/rootfs.ext4", VsockPath: "/run/template.vsock", TapName: "fctap0",
		SnapshotPath: snapshotPath, MemoryPath: memoryPath, WritableDiskPath: diskPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	destinationDir := t.TempDir()
	dst := RestorePaths{
		Snapshot:     filepath.Join(destinationDir, "snap"),
		Memory:       filepath.Join(destinationDir, "mem"),
		WritableDisk: filepath.Join(destinationDir, "writable.ext4"),
	}
	reader, err := NewReader(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Restore(ctx, sandboxID, "", dst)
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestKey != manifestKey {
		t.Fatalf("manifest key = %q, want %q", result.ManifestKey, manifestKey)
	}
	assertFilesEqual(t, diskPath, dst.WritableDisk)
	assertFilesEqual(t, snapshotPath, dst.Snapshot)
	assertFilesEqual(t, memoryPath, dst.Memory)
}

func TestReaderRestoresActiveChunkedCheckpointSparsely(t *testing.T) {
	ctx := context.Background()
	store := newRecordingStore()
	sandboxID := uuid.NewString()
	prefix := "sandbox-checkpoints"
	generation := uuid.NewString()
	base := prefix + "/" + sandboxID + "/generations/" + generation

	snapshot := putTestArtifact(t, store, base+"/vmstate.snap", []byte("snapshot"))
	memory := putTestArtifact(t, store, base+"/memory.mem", []byte("memory"))
	chunk0 := putTestChunk(t, store, prefix+"/"+sandboxID+"/disk-chunks/first", 0, []byte("ABCD"))
	chunk2 := putTestChunk(t, store, prefix+"/"+sandboxID+"/disk-chunks/last", 2, []byte("XY"))
	manifest := Manifest{
		Version:      manifestVersion,
		SandboxID:    sandboxID,
		Generation:   generation,
		Resources:    Resources{VCPUs: 2, MemoryMB: 512, DiskGB: 1},
		Resume:       ResumeMetadata{RootfsPath: "/assets/rootfs.ext4", VsockPath: "/run/template.vsock", TapName: "fctap0"},
		Snapshot:     snapshot,
		Memory:       memory,
		WritableDisk: WritableDisk{LogicalSizeBytes: 10, ChunkSizeBytes: 4, Chunks: []DiskChunk{chunk2, chunk0}},
	}
	manifestKey := base + "/manifest.json"
	putTestJSON(t, store, manifestKey, manifest)
	putTestJSON(t, store, prefix+"/"+sandboxID+"/latest.json", activePointer{SandboxID: sandboxID, Generation: generation, ManifestKey: manifestKey})

	dir := t.TempDir()
	dst := RestorePaths{
		Snapshot:     filepath.Join(dir, "pause", "snap"),
		Memory:       filepath.Join(dir, "pause", "mem"),
		WritableDisk: filepath.Join(dir, "active", "writable.ext4"),
	}
	reader, err := NewReader(store, prefix)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Restore(ctx, sandboxID, "", dst)
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestKey != manifestKey || result.Resources.VCPUs != 2 || result.Resume.TapName != "fctap0" {
		t.Fatalf("unexpected restore result: %#v", result)
	}
	assertFileContent(t, dst.Snapshot, []byte("snapshot"))
	assertFileContent(t, dst.Memory, []byte("memory"))
	assertFileContent(t, dst.WritableDisk, []byte{'A', 'B', 'C', 'D', 0, 0, 0, 0, 'X', 'Y'})
}

func TestReaderRestoresLegacyFullDisk(t *testing.T) {
	store := newRecordingStore()
	sandboxID := uuid.NewString()
	base := "sandbox-checkpoints/" + sandboxID + "/generations/legacy"
	manifest := legacyManifest{
		Version:      1,
		SandboxID:    sandboxID,
		Generation:   "legacy",
		Snapshot:     putTestArtifact(t, store, base+"/vmstate.snap", []byte("snap")),
		Memory:       putTestArtifact(t, store, base+"/memory.mem", []byte("mem")),
		WritableDisk: putTestArtifact(t, store, base+"/writable.ext4", []byte("full-disk")),
	}
	manifestKey := base + "/manifest.json"
	putTestJSON(t, store, manifestKey, manifest)
	dir := t.TempDir()
	dst := RestorePaths{filepath.Join(dir, "snap"), filepath.Join(dir, "mem"), filepath.Join(dir, "disk")}
	reader, _ := NewReader(store, "sandbox-checkpoints")
	if _, err := reader.Restore(context.Background(), sandboxID, manifestKey, dst); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, dst.WritableDisk, []byte("full-disk"))
}

func TestReaderRejectsCorruptChunkWithoutPublishingFiles(t *testing.T) {
	store := newRecordingStore()
	sandboxID := uuid.NewString()
	base := "sandbox-checkpoints/" + sandboxID + "/generations/bad"
	chunk := putTestChunk(t, store, "sandbox-checkpoints/"+sandboxID+"/disk-chunks/bad", 0, []byte("data"))
	chunk.SHA256 = hex.EncodeToString(make([]byte, sha256.Size))
	manifest := Manifest{
		Version:      manifestVersion,
		SandboxID:    sandboxID,
		Generation:   "bad",
		Snapshot:     putTestArtifact(t, store, base+"/vmstate.snap", []byte("snap")),
		Memory:       putTestArtifact(t, store, base+"/memory.mem", []byte("mem")),
		WritableDisk: WritableDisk{LogicalSizeBytes: 4, ChunkSizeBytes: 4, Chunks: []DiskChunk{chunk}},
	}
	manifestKey := base + "/manifest.json"
	putTestJSON(t, store, manifestKey, manifest)
	dir := t.TempDir()
	dst := RestorePaths{filepath.Join(dir, "snap"), filepath.Join(dir, "mem"), filepath.Join(dir, "disk")}
	reader, _ := NewReader(store, "sandbox-checkpoints")
	if _, err := reader.Restore(context.Background(), sandboxID, manifestKey, dst); err == nil {
		t.Fatal("expected corrupt chunk restore to fail")
	}
	for _, name := range []string{dst.Snapshot, dst.Memory, dst.WritableDisk} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("destination %s was published after failed verification", name)
		}
	}
}

func TestReaderRejectsDuplicateChunkIndex(t *testing.T) {
	store := newRecordingStore()
	sandboxID := uuid.NewString()
	base := "sandbox-checkpoints/" + sandboxID + "/generations/duplicate"
	chunk := putTestChunk(t, store, "sandbox-checkpoints/"+sandboxID+"/disk-chunks/chunk", 0, []byte("data"))
	manifest := Manifest{
		Version:      manifestVersion,
		SandboxID:    sandboxID,
		Snapshot:     putTestArtifact(t, store, base+"/snap", []byte("snap")),
		Memory:       putTestArtifact(t, store, base+"/mem", []byte("mem")),
		WritableDisk: WritableDisk{LogicalSizeBytes: 4, ChunkSizeBytes: 4, Chunks: []DiskChunk{chunk, chunk}},
	}
	manifestKey := base + "/manifest.json"
	putTestJSON(t, store, manifestKey, manifest)
	reader, _ := NewReader(store, "sandbox-checkpoints")
	dir := t.TempDir()
	_, err := reader.Restore(context.Background(), sandboxID, manifestKey, RestorePaths{filepath.Join(dir, "snap"), filepath.Join(dir, "mem"), filepath.Join(dir, "disk")})
	if err == nil {
		t.Fatal("expected duplicate chunk index to fail")
	}
}

func putTestArtifact(t *testing.T, store *recordingStore, key string, data []byte) Artifact {
	t.Helper()
	if err := store.Put(context.Background(), key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	return Artifact{Key: key, SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
}

func putTestChunk(t *testing.T, store *recordingStore, key string, index int64, data []byte) DiskChunk {
	t.Helper()
	artifact := putTestArtifact(t, store, key, data)
	return DiskChunk{Index: index, Key: key, SizeBytes: artifact.SizeBytes, SHA256: artifact.SHA256}
}

func putTestJSON(t *testing.T, store *recordingStore, key string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, name string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertFilesEqual(t *testing.T, left, right string) {
	t.Helper()
	leftData, err := os.ReadFile(left)
	if err != nil {
		t.Fatal(err)
	}
	rightData, err := os.ReadFile(right)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftData, rightData) {
		t.Fatalf("restored file %s does not match %s", right, left)
	}
}

func TestWriterReaderRoundTripRestoresEmptyMemoryChunks(t *testing.T) {
	ctx := context.Background()
	store, err := template.NewLocalStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := t.TempDir()
	// Guest memory: data, an all-zero chunk the writer leaves out, then data again.
	memoryPath := filepath.Join(sourceDir, "mem")
	memory := append(append(bytes.Repeat([]byte{7}, defaultChunkSize), make([]byte, defaultChunkSize)...), []byte("tail")...)
	if err := os.WriteFile(memoryPath, memory, 0o600); err != nil {
		t.Fatal(err)
	}
	sandboxID := uuid.NewString()
	if _, err := writer.Save(ctx, Input{
		SandboxID: sandboxID, SnapshotPath: writeTestFile(t, sourceDir, "snap", "vm-state"),
		MemoryPath: memoryPath, WritableDiskPath: writeTestFile(t, sourceDir, "disk", "disk"),
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	destinationDir := t.TempDir()
	dst := RestorePaths{
		Snapshot:     filepath.Join(destinationDir, "snap"),
		Memory:       filepath.Join(destinationDir, "mem"),
		WritableDisk: filepath.Join(destinationDir, "disk"),
	}
	if _, err := reader.Restore(ctx, sandboxID, "", dst); err != nil {
		t.Fatal(err)
	}
	assertFilesEqual(t, memoryPath, dst.Memory)
}
