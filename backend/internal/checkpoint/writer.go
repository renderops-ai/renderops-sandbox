package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"backend/internal/template"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const (
	manifestVersion  = 3 // 3: memory in chunks (MemoryChunks); 2: one memory object
	defaultChunkSize = 4 * 1024 * 1024
)

// Input is the complete local state required to resume a paused sandbox.
type Input struct {
	SandboxID        string
	VCPUs            int
	MemoryMB         int
	DiskGB           int
	RootfsPath       string
	VsockPath        string
	TapName          string
	SnapshotPath     string
	MemoryPath       string
	WritableDiskPath string
}

type Resources struct {
	VCPUs    int `json:"vcpus"`
	MemoryMB int `json:"memory_mb"`
	DiskGB   int `json:"disk_gb"`
}

type ResumeMetadata struct {
	RootfsPath string `json:"rootfs_path"`
	VsockPath  string `json:"vsock_path"`
	TapName    string `json:"tap_name"`
}

// Artifact identifies one immutable object in a checkpoint generation.
type Artifact struct {
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type DiskChunk struct {
	Index     int64  `json:"index"`
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type WritableDisk struct {
	LogicalSizeBytes int64       `json:"logical_size_bytes"`
	ChunkSizeBytes   int64       `json:"chunk_size_bytes"`
	Chunks           []DiskChunk `json:"chunks"`
}

// Manifest is committed only after every checkpoint artifact is durable.
type Manifest struct {
	Version      int            `json:"version"`
	SandboxID    string         `json:"sandbox_id"`
	Generation   string         `json:"generation"`
	CreatedAt    time.Time      `json:"created_at"`
	Resources    Resources      `json:"resources"`
	Resume       ResumeMetadata `json:"resume"`
	Snapshot     Artifact       `json:"snapshot"`
	Memory       Artifact       `json:"memory"` // version 2: the whole memory file
	WritableDisk WritableDisk   `json:"writable_disk"`
	// Version 3: guest memory in chunks, same layout as the writable disk. Empty
	// chunks are left out and unchanged ones reuse the previous generation's objects.
	MemoryChunks *WritableDisk `json:"memory_chunks,omitempty"`
}

type activePointer struct {
	Version     int       `json:"version"`
	SandboxID   string    `json:"sandbox_id"`
	Generation  string    `json:"generation"`
	ManifestKey string    `json:"manifest_key"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Writer publishes immutable checkpoint generations to an object store.
type Writer struct {
	store     template.ArtifactStore
	prefix    string
	chunkSize int64
	now       func() time.Time
}

func NewWriter(store template.ArtifactStore, prefix string) (*Writer, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint store is required")
	}
	prefix = strings.Trim(path.Clean(prefix), "/")
	if prefix == "" || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") {
		return nil, fmt.Errorf("invalid checkpoint prefix %q", prefix)
	}
	return &Writer{store: store, prefix: prefix, chunkSize: defaultChunkSize, now: time.Now}, nil
}

// objectDeleter is the part of a store DeleteSandbox needs.
type objectDeleter interface {
	Delete(ctx context.Context, key string) error
}

// deleteConcurrency bounds parallel deletes; a sandbox has one object per disk chunk.
const deleteConcurrency = 8

// DeleteSandbox removes every checkpoint object of a sandbox: all generations,
// disk chunks and latest.json. latest.json goes first so a reader never follows
// a pointer into a half-deleted generation. Objects already gone are fine.
// Returns how many objects were deleted.
func (w *Writer) DeleteSandbox(ctx context.Context, sandboxID string) (int, error) {
	// A UUID can never be empty or contain "/", so this cannot widen to the prefix.
	if _, err := uuid.Parse(sandboxID); err != nil {
		return 0, fmt.Errorf("invalid sandbox id %q: %w", sandboxID, err)
	}
	store, ok := w.store.(objectDeleter)
	if !ok {
		return 0, fmt.Errorf("checkpoint store cannot delete objects")
	}
	base := path.Join(w.prefix, sandboxID) + "/"
	keys, err := w.store.List(ctx, base)
	if err != nil {
		return 0, fmt.Errorf("list checkpoints: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	latest := path.Join(w.prefix, sandboxID, "latest.json")
	if err := store.Delete(ctx, latest); err != nil {
		return 0, fmt.Errorf("delete latest pointer: %w", err)
	}

	var (
		mu      sync.Mutex
		deleted = 0
		errs    []error
		wg      sync.WaitGroup
		slots   = make(chan struct{}, deleteConcurrency)
	)
	for _, key := range keys {
		if key == latest {
			mu.Lock()
			deleted++
			mu.Unlock()
			continue
		}
		wg.Add(1)
		slots <- struct{}{}
		go func(key string) {
			defer wg.Done()
			defer func() { <-slots }()
			err := store.Delete(ctx, key)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			deleted++
		}(key)
	}
	wg.Wait()
	return deleted, errors.Join(errs...)
}

// Save uploads VM state and only the new memory and writable-disk chunks, then
// commits the manifest and active pointer last. Partial generations are never selected.
func (w *Writer) Save(ctx context.Context, in Input) (string, error) {
	if _, err := uuid.Parse(in.SandboxID); err != nil {
		return "", fmt.Errorf("invalid sandbox id %q: %w", in.SandboxID, err)
	}
	if in.SnapshotPath == "" || in.MemoryPath == "" || in.WritableDiskPath == "" {
		return "", fmt.Errorf("checkpoint requires snapshot, memory and writable disk paths")
	}

	generation := uuid.NewString()
	base := path.Join(w.prefix, in.SandboxID, "generations", generation)
	createdAt := w.now().UTC()
	start := time.Now()

	snapshot, err := w.putFile(ctx, path.Join(base, "vmstate.snap"), in.SnapshotPath)
	if err != nil {
		return "", fmt.Errorf("upload VM state: %w", err)
	}
	previous := w.previousChunks(ctx, in.SandboxID)
	memoryStart := time.Now()
	memory, memoryStats, err := w.putChunks(ctx, in.MemoryPath, path.Join(w.prefix, in.SandboxID, "memory-chunks"), previous.memory)
	if err != nil {
		return "", fmt.Errorf("upload memory: %w", err)
	}
	memoryTime := time.Since(memoryStart)
	diskStart := time.Now()
	writable, diskStats, err := w.putChunks(ctx, in.WritableDiskPath, path.Join(w.prefix, in.SandboxID, "disk-chunks"), previous.disk)
	if err != nil {
		return "", fmt.Errorf("upload writable disk: %w", err)
	}
	diskTime := time.Since(diskStart)

	manifest := Manifest{
		Version:      manifestVersion,
		SandboxID:    in.SandboxID,
		Generation:   generation,
		CreatedAt:    createdAt,
		Resources:    Resources{VCPUs: in.VCPUs, MemoryMB: in.MemoryMB, DiskGB: in.DiskGB},
		Resume:       ResumeMetadata{RootfsPath: in.RootfsPath, VsockPath: in.VsockPath, TapName: in.TapName},
		Snapshot:     snapshot,
		WritableDisk: writable,
		MemoryChunks: &memory,
	}
	manifestKey := path.Join(base, "manifest.json")
	if err := w.putJSON(ctx, manifestKey, manifest); err != nil {
		return "", fmt.Errorf("commit checkpoint manifest: %w", err)
	}
	pointer := activePointer{
		Version:     manifestVersion,
		SandboxID:   in.SandboxID,
		Generation:  generation,
		ManifestKey: manifestKey,
		UpdatedAt:   createdAt,
	}
	if err := w.putJSON(ctx, path.Join(w.prefix, in.SandboxID, "latest.json"), pointer); err != nil {
		return "", fmt.Errorf("activate checkpoint: %w", err)
	}
	slog.Info("checkpoint saved", "sandbox_id", in.SandboxID, "generation", generation,
		"memory_chunks", len(memory.Chunks), "memory_chunks_uploaded", memoryStats.uploaded.Load(),
		"memory_chunks_reused", memoryStats.reused.Load(), "memory_ms", memoryTime.Milliseconds(),
		"disk_chunks", len(writable.Chunks), "disk_chunks_uploaded", diskStats.uploaded.Load(),
		"disk_chunks_reused", diskStats.reused.Load(), "disk_ms", diskTime.Milliseconds(),
		"ms", time.Since(start).Milliseconds())
	return manifestKey, nil
}

// chunkUploadConcurrency bounds parallel chunk uploads. Each in-flight chunk holds
// one chunk of data in memory.
const chunkUploadConcurrency = 8

type chunkStats struct {
	uploaded atomic.Int64 // new chunks sent to the store
	reused   atomic.Int64 // chunks already in the store
}

// putChunks uploads filePath as chunks under chunkDir: all-zero chunks are left out,
// and a chunk matching the previous generation's at the same index reuses its object.
func (w *Writer) putChunks(ctx context.Context, filePath, chunkDir string, previous map[int64]DiskChunk) (WritableDisk, *chunkStats, error) {
	stats := &chunkStats{}
	f, err := os.Open(filePath)
	if err != nil {
		return WritableDisk{}, nil, fmt.Errorf("open %s: %w", filePath, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return WritableDisk{}, nil, fmt.Errorf("stat %s: %w", filePath, err)
	}
	if info.Size() <= 0 {
		return WritableDisk{}, nil, fmt.Errorf("%s is empty", filePath)
	}

	indices, err := allocatedChunkIndices(f, info.Size(), w.chunkSize)
	if err != nil {
		return WritableDisk{}, nil, fmt.Errorf("scan sparse file: %w", err)
	}

	// Chunks are read and hashed in order; new ones upload in the background. The
	// first upload error cancels the rest.
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		slots    = make(chan struct{}, chunkUploadConcurrency)
		failOnce sync.Once
		firstErr error
	)
	fail := func(err error) {
		failOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}
	chunks := make([]DiskChunk, 0, len(indices))
	for _, index := range indices {
		if uploadCtx.Err() != nil {
			break
		}
		offset := index * w.chunkSize
		size := min(w.chunkSize, info.Size()-offset)
		data := make([]byte, size)
		if _, err := f.ReadAt(data, offset); err != nil && !errors.Is(err, io.EOF) {
			fail(fmt.Errorf("read chunk %d: %w", index, err))
			break
		}
		if isZero(data) {
			continue
		}
		hash := sha256.Sum256(data)
		digest := hex.EncodeToString(hash[:])
		key := path.Join(chunkDir, digest)
		if old, ok := previous[index]; ok && old.SHA256 == digest {
			key = old.Key
			stats.reused.Add(1)
		} else {
			wg.Add(1)
			slots <- struct{}{}
			go func(index int64, key string, data []byte) {
				defer wg.Done()
				defer func() { <-slots }()
				_, exists, err := w.store.Stat(uploadCtx, key)
				if err != nil {
					fail(fmt.Errorf("stat chunk %d: %w", index, err))
					return
				}
				if exists {
					stats.reused.Add(1)
					return
				}
				if err := w.store.Put(uploadCtx, key, bytes.NewReader(data)); err != nil {
					fail(fmt.Errorf("upload chunk %d: %w", index, err))
					return
				}
				stats.uploaded.Add(1)
			}(index, key, data)
		}
		chunks = append(chunks, DiskChunk{Index: index, Key: key, SizeBytes: size, SHA256: digest})
	}
	wg.Wait()
	if firstErr != nil {
		return WritableDisk{}, nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return WritableDisk{}, nil, err
	}
	return WritableDisk{LogicalSizeBytes: info.Size(), ChunkSizeBytes: w.chunkSize, Chunks: chunks}, stats, nil
}

// previousGeneration is the latest generation's chunks by index, for reuse.
type previousGeneration struct {
	disk, memory map[int64]DiskChunk
}

func (w *Writer) previousChunks(ctx context.Context, sandboxID string) previousGeneration {
	result := previousGeneration{disk: make(map[int64]DiskChunk), memory: make(map[int64]DiskChunk)}
	pointerReader, err := w.store.Get(ctx, path.Join(w.prefix, sandboxID, "latest.json"))
	if err != nil {
		return result
	}
	defer pointerReader.Close()
	var pointer activePointer
	if json.NewDecoder(pointerReader).Decode(&pointer) != nil || pointer.ManifestKey == "" {
		return result
	}
	manifestReader, err := w.store.Get(ctx, pointer.ManifestKey)
	if err != nil {
		return result
	}
	defer manifestReader.Close()
	var manifest Manifest
	if json.NewDecoder(manifestReader).Decode(&manifest) != nil || manifest.Version < 2 {
		return result
	}
	for _, chunk := range manifest.WritableDisk.Chunks {
		result.disk[chunk.Index] = chunk
	}
	if manifest.MemoryChunks != nil {
		for _, chunk := range manifest.MemoryChunks.Chunks {
			result.memory[chunk.Index] = chunk
		}
	}
	return result
}

func allocatedChunkIndices(f *os.File, logicalSize, chunkSize int64) ([]int64, error) {
	indices := make(map[int64]struct{})
	for offset := int64(0); offset < logicalSize; {
		data, err := unix.Seek(int(f.Fd()), offset, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
			for index := int64(0); index*chunkSize < logicalSize; index++ {
				indices[index] = struct{}{}
			}
			break
		}
		if err != nil {
			return nil, err
		}
		hole, err := unix.Seek(int(f.Fd()), data, unix.SEEK_HOLE)
		if err != nil {
			return nil, err
		}
		if hole > logicalSize {
			hole = logicalSize
		}
		for index := data / chunkSize; index*chunkSize < hole; index++ {
			indices[index] = struct{}{}
		}
		offset = hole
	}
	result := make([]int64, 0, len(indices))
	for index := range indices {
		result = append(result, index)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

// zeroChunk lets isZero compare with bytes.Equal, which is much faster than a byte loop
// over the hundreds of MiB a memory file has.
var zeroChunk = make([]byte, defaultChunkSize)

func isZero(data []byte) bool {
	for len(data) > 0 {
		n := min(len(data), len(zeroChunk))
		if !bytes.Equal(data[:n], zeroChunk[:n]) {
			return false
		}
		data = data[n:]
	}
	return true
}

func (w *Writer) putFile(ctx context.Context, key, filePath string) (Artifact, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return Artifact{}, fmt.Errorf("open %s: %w", filePath, err)
	}
	defer f.Close()

	hash := sha256.New()
	counter := &byteCounter{}
	if err := w.store.Put(ctx, key, io.TeeReader(f, io.MultiWriter(hash, counter))); err != nil {
		return Artifact{}, err
	}
	return Artifact{Key: key, SizeBytes: counter.n, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (w *Writer) putJSON(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return w.store.Put(ctx, key, strings.NewReader(string(data)))
}

type byteCounter struct{ n int64 }

func (c *byteCounter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}
