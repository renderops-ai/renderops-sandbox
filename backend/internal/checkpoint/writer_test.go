package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"backend/internal/template"
)

func TestWriterCommitsPointerLast(t *testing.T) {
	store := newRecordingStore()
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	writer.now = func() time.Time { return time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC) }

	dir := t.TempDir()
	snap := writeTestFile(t, dir, "snap", "vm-state")
	mem := writeTestFile(t, dir, "mem", "guest-memory")
	disk := writeTestFile(t, dir, "disk", "user-data")
	sandboxID := "a6846a48-db37-46bc-907a-3a9e15093603"

	manifestKey, err := writer.Save(context.Background(), Input{
		SandboxID: sandboxID, SnapshotPath: snap, MemoryPath: mem, WritableDiskPath: disk,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.order) != 5 {
		t.Fatalf("writes=%d, want 5: %v", len(store.order), store.order)
	}
	if store.order[3] != manifestKey {
		t.Fatalf("manifest write=%q, want %q", store.order[3], manifestKey)
	}
	wantPointer := "sandbox-checkpoints/" + sandboxID + "/latest.json"
	if store.order[4] != wantPointer {
		t.Fatalf("last write=%q, want %q", store.order[4], wantPointer)
	}

	var manifest Manifest
	if err := json.Unmarshal(store.objects[manifestKey], &manifest); err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256([]byte("user-data"))
	if manifest.Version != 2 || manifest.WritableDisk.LogicalSizeBytes != int64(len("user-data")) || len(manifest.WritableDisk.Chunks) != 1 || manifest.WritableDisk.Chunks[0].SHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("unexpected writable artifact: %+v", manifest.WritableDisk)
	}
}

func TestWriterReusesUnchangedDiskChunks(t *testing.T) {
	store := newRecordingStore()
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	writer.chunkSize = 4
	dir := t.TempDir()
	input := Input{
		SandboxID:        "a6846a48-db37-46bc-907a-3a9e15093603",
		SnapshotPath:     writeTestFile(t, dir, "snap", "snap"),
		MemoryPath:       writeTestFile(t, dir, "mem", "mem"),
		WritableDiskPath: writeTestFile(t, dir, "disk", "abcdefgh"),
	}
	if _, err := writer.Save(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	firstChunkWrites := store.chunkWrites()
	store.order = nil
	if _, err := writer.Save(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if got := store.chunkWrites(); got != 0 {
		t.Fatalf("unchanged disk uploaded %d chunks, want 0 (first generation uploaded %d)", got, firstChunkWrites)
	}
}

func TestWriterUploadsOnlyChangedDiskChunk(t *testing.T) {
	store := newRecordingStore()
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	writer.chunkSize = 4
	dir := t.TempDir()
	disk := writeTestFile(t, dir, "disk", "abcdefgh")
	input := Input{SandboxID: "a6846a48-db37-46bc-907a-3a9e15093603", SnapshotPath: writeTestFile(t, dir, "snap", "snap"), MemoryPath: writeTestFile(t, dir, "mem", "mem"), WritableDiskPath: disk}
	if _, err := writer.Save(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(disk, []byte("abcdWXYZ"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.order = nil
	if _, err := writer.Save(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if got := store.chunkWrites(); got != 1 {
		t.Fatalf("changed disk uploaded %d chunks, want 1", got)
	}
}

func TestWriterOmitsSparseHoles(t *testing.T) {
	store := newRecordingStore()
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	writer.chunkSize = 4096
	dir := t.TempDir()
	disk := filepath.Join(dir, "disk")
	f, err := os.Create(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 * writer.chunkSize); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("changed"), 2*writer.chunkSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	manifestKey, err := writer.Save(context.Background(), Input{
		SandboxID:        "a6846a48-db37-46bc-907a-3a9e15093603",
		SnapshotPath:     writeTestFile(t, dir, "snap", "snap"),
		MemoryPath:       writeTestFile(t, dir, "mem", "mem"),
		WritableDiskPath: disk,
	})
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(store.objects[manifestKey], &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.WritableDisk.Chunks) != 1 || manifest.WritableDisk.Chunks[0].Index != 2 {
		t.Fatalf("chunks=%+v, want only index 2", manifest.WritableDisk.Chunks)
	}
}

func TestWriterDoesNotCommitManifestAfterArtifactFailure(t *testing.T) {
	store := newRecordingStore()
	store.failAt = 2
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	_, err = writer.Save(context.Background(), Input{
		SandboxID:        "a6846a48-db37-46bc-907a-3a9e15093603",
		SnapshotPath:     writeTestFile(t, dir, "snap", "snap"),
		MemoryPath:       writeTestFile(t, dir, "mem", "mem"),
		WritableDiskPath: writeTestFile(t, dir, "disk", "disk"),
	})
	if err == nil {
		t.Fatal("expected upload failure")
	}
	for _, key := range store.order {
		if strings.HasSuffix(key, "manifest.json") || strings.HasSuffix(key, "latest.json") {
			t.Fatalf("committed %q after partial upload", key)
		}
	}
}

func writeTestFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// recordingStore is safe for concurrent use: the writer uploads disk chunks in parallel.
type recordingStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	order   []string
	failAt  int
}

func newRecordingStore() *recordingStore { return &recordingStore{objects: make(map[string][]byte)} }

func (s *recordingStore) Put(_ context.Context, key string, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.order = append(s.order, key)
	if s.failAt > 0 && len(s.order) == s.failAt {
		return fmt.Errorf("injected failure")
	}
	b, err := io.ReadAll(r)
	if err == nil {
		s.objects[key] = b
	}
	return err
}

func (s *recordingStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	if !ok {
		return nil, template.ErrNotFound
	}
	return io.NopCloser(strings.NewReader(string(b))), nil
}
func (s *recordingStore) Stat(_ context.Context, key string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	return int64(len(b)), ok, nil
}
func (s *recordingStore) List(context.Context, string) ([]string, error) { return nil, nil }

func (s *recordingStore) chunkWrites() int {
	count := 0
	for _, key := range s.order {
		if strings.Contains(key, "/disk-chunks/") {
			count++
		}
	}
	return count
}
