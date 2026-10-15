package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"backend/internal/template"

	"github.com/google/uuid"
)

type RestorePaths struct {
	Snapshot     string
	Memory       string
	WritableDisk string
}

type RestoreResult struct {
	ManifestKey string
	Resources   Resources
	Resume      ResumeMetadata
	Paths       RestorePaths
}

type legacyManifest struct {
	Version      int            `json:"version"`
	SandboxID    string         `json:"sandbox_id"`
	Generation   string         `json:"generation"`
	Resources    Resources      `json:"resources"`
	Resume       ResumeMetadata `json:"resume"`
	Snapshot     Artifact       `json:"snapshot"`
	Memory       Artifact       `json:"memory"`
	WritableDisk Artifact       `json:"writable_disk"`
}

// Reader restores a committed checkpoint generation from durable object storage.
type Reader struct {
	store  template.ArtifactStore
	prefix string
}

func NewReader(store template.ArtifactStore, prefix string) (*Reader, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint store is required")
	}
	prefix = strings.Trim(path.Clean(prefix), "/")
	if prefix == "" || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") {
		return nil, fmt.Errorf("invalid checkpoint prefix %q", prefix)
	}
	return &Reader{store: store, prefix: prefix}, nil
}

// Restore downloads either manifestKey or the sandbox's active generation when the key
// is empty. Artifacts are verified in temporary files before replacing destination files.
func (r *Reader) Restore(ctx context.Context, sandboxID, manifestKey string, dst RestorePaths) (RestoreResult, error) {
	if _, err := uuid.Parse(sandboxID); err != nil {
		return RestoreResult{}, fmt.Errorf("invalid sandbox id %q: %w", sandboxID, err)
	}
	if dst.Snapshot == "" || dst.Memory == "" || dst.WritableDisk == "" {
		return RestoreResult{}, fmt.Errorf("checkpoint restore requires snapshot, memory and writable disk destinations")
	}
	var err error
	if manifestKey == "" {
		manifestKey, err = r.activeManifestKey(ctx, sandboxID)
		if err != nil {
			return RestoreResult{}, err
		}
	}
	if err := r.validateObjectKey(sandboxID, manifestKey); err != nil {
		return RestoreResult{}, fmt.Errorf("invalid checkpoint manifest key: %w", err)
	}

	data, err := r.readObject(ctx, manifestKey)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("read checkpoint manifest: %w", err)
	}
	var header struct {
		Version   int    `json:"version"`
		SandboxID string `json:"sandbox_id"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return RestoreResult{}, fmt.Errorf("decode checkpoint manifest header: %w", err)
	}
	if header.SandboxID != sandboxID {
		return RestoreResult{}, fmt.Errorf("checkpoint belongs to sandbox %q, not %q", header.SandboxID, sandboxID)
	}

	result := RestoreResult{ManifestKey: manifestKey, Paths: dst}
	switch header.Version {
	case 1:
		var manifest legacyManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return RestoreResult{}, fmt.Errorf("decode legacy checkpoint manifest: %w", err)
		}
		result.Resources, result.Resume = manifest.Resources, manifest.Resume
		if err := r.restoreArtifacts(ctx, sandboxID, dst, manifest.Snapshot, manifest.Memory, nil, &manifest.WritableDisk, nil); err != nil {
			return RestoreResult{}, err
		}
	case 2, manifestVersion:
		var manifest Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return RestoreResult{}, fmt.Errorf("decode checkpoint manifest: %w", err)
		}
		result.Resources, result.Resume = manifest.Resources, manifest.Resume
		if err := r.restoreArtifacts(ctx, sandboxID, dst, manifest.Snapshot, manifest.Memory, manifest.MemoryChunks, nil, &manifest.WritableDisk); err != nil {
			return RestoreResult{}, err
		}
	default:
		return RestoreResult{}, fmt.Errorf("unsupported checkpoint manifest version %d", header.Version)
	}
	return result, nil
}

func (r *Reader) activeManifestKey(ctx context.Context, sandboxID string) (string, error) {
	data, err := r.readObject(ctx, path.Join(r.prefix, sandboxID, "latest.json"))
	if err != nil {
		return "", fmt.Errorf("read active checkpoint: %w", err)
	}
	var pointer activePointer
	if err := json.Unmarshal(data, &pointer); err != nil {
		return "", fmt.Errorf("decode active checkpoint: %w", err)
	}
	if pointer.SandboxID != sandboxID || pointer.ManifestKey == "" {
		return "", fmt.Errorf("active checkpoint pointer is invalid for sandbox %q", sandboxID)
	}
	return pointer.ManifestKey, nil
}

// restoreArtifacts restores memory from memoryChunks when set, else from the single
// memory object (version 1 and 2 checkpoints).
func (r *Reader) restoreArtifacts(ctx context.Context, sandboxID string, dst RestorePaths, snapshot, memory Artifact, memoryChunks *WritableDisk, legacyDisk *Artifact, disk *WritableDisk) error {
	temps := make([]string, 0, 3)
	cleanup := func() {
		for _, name := range temps {
			_ = os.Remove(name)
		}
	}
	defer cleanup()

	snapTemp, err := r.downloadArtifact(ctx, sandboxID, snapshot, dst.Snapshot)
	if err != nil {
		return fmt.Errorf("restore VM state: %w", err)
	}
	temps = append(temps, snapTemp)
	var memTemp string
	if memoryChunks != nil {
		memTemp, err = r.reconstructDisk(ctx, sandboxID, *memoryChunks, dst.Memory)
	} else {
		memTemp, err = r.downloadArtifact(ctx, sandboxID, memory, dst.Memory)
	}
	if err != nil {
		return fmt.Errorf("restore memory: %w", err)
	}
	temps = append(temps, memTemp)

	var diskTemp string
	if legacyDisk != nil {
		diskTemp, err = r.downloadArtifact(ctx, sandboxID, *legacyDisk, dst.WritableDisk)
	} else {
		diskTemp, err = r.reconstructDisk(ctx, sandboxID, *disk, dst.WritableDisk)
	}
	if err != nil {
		return fmt.Errorf("restore writable disk: %w", err)
	}
	temps = append(temps, diskTemp)

	for _, pair := range [][2]string{{snapTemp, dst.Snapshot}, {memTemp, dst.Memory}, {diskTemp, dst.WritableDisk}} {
		if err := os.Rename(pair[0], pair[1]); err != nil {
			for _, name := range []string{dst.Snapshot, dst.Memory, dst.WritableDisk} {
				_ = os.Remove(name)
			}
			return fmt.Errorf("commit restored artifact %s: %w", pair[1], err)
		}
	}
	return nil
}

func (r *Reader) downloadArtifact(ctx context.Context, sandboxID string, artifact Artifact, destination string) (string, error) {
	if err := r.validateArtifact(sandboxID, artifact); err != nil {
		return "", err
	}
	tmp, err := createRestoreTemp(destination)
	if err != nil {
		return "", err
	}
	if err := r.copyVerified(ctx, artifact, tmp); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

func (r *Reader) reconstructDisk(ctx context.Context, sandboxID string, disk WritableDisk, destination string) (string, error) {
	if disk.LogicalSizeBytes <= 0 || disk.ChunkSizeBytes <= 0 {
		return "", fmt.Errorf("invalid disk geometry: logical=%d chunk=%d", disk.LogicalSizeBytes, disk.ChunkSizeBytes)
	}
	chunks := append([]DiskChunk(nil), disk.Chunks...)
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Index < chunks[j].Index })
	var previous int64 = -1
	for _, chunk := range chunks {
		if chunk.Index < 0 || chunk.Index == previous {
			return "", fmt.Errorf("invalid or duplicate disk chunk index %d", chunk.Index)
		}
		previous = chunk.Index
		offset := chunk.Index * disk.ChunkSizeBytes
		if offset < 0 || offset >= disk.LogicalSizeBytes {
			return "", fmt.Errorf("disk chunk %d is outside logical disk", chunk.Index)
		}
		wantSize := min(disk.ChunkSizeBytes, disk.LogicalSizeBytes-offset)
		if chunk.SizeBytes != wantSize {
			return "", fmt.Errorf("disk chunk %d size is %d, want %d", chunk.Index, chunk.SizeBytes, wantSize)
		}
		if err := r.validateObjectKey(sandboxID, chunk.Key); err != nil {
			return "", fmt.Errorf("disk chunk %d: %w", chunk.Index, err)
		}
		if !validSHA256(chunk.SHA256) {
			return "", fmt.Errorf("disk chunk %d has invalid SHA-256", chunk.Index)
		}
	}

	tmp, err := createRestoreTemp(destination)
	if err != nil {
		return "", err
	}
	fail := func(err error) (string, error) {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Truncate(disk.LogicalSizeBytes); err != nil {
		return fail(fmt.Errorf("size sparse disk: %w", err))
	}
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		artifact := Artifact{Key: chunk.Key, SizeBytes: chunk.SizeBytes, SHA256: chunk.SHA256}
		reader, err := r.store.Get(ctx, artifact.Key)
		if err != nil {
			return fail(fmt.Errorf("read chunk %d: %w", chunk.Index, err))
		}
		hash := sha256.New()
		written, copyErr := io.Copy(io.NewOffsetWriter(tmp, chunk.Index*disk.ChunkSizeBytes), io.TeeReader(io.LimitReader(reader, chunk.SizeBytes+1), hash))
		closeErr := reader.Close()
		if copyErr != nil {
			return fail(fmt.Errorf("write chunk %d: %w", chunk.Index, copyErr))
		}
		if closeErr != nil {
			return fail(fmt.Errorf("close chunk %d: %w", chunk.Index, closeErr))
		}
		if written != chunk.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != chunk.SHA256 {
			return fail(fmt.Errorf("chunk %d failed size or SHA-256 verification", chunk.Index))
		}
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

func (r *Reader) copyVerified(ctx context.Context, artifact Artifact, dst *os.File) error {
	reader, err := r.store.Get(ctx, artifact.Key)
	if err != nil {
		return err
	}
	defer reader.Close()
	hash := sha256.New()
	written, err := io.Copy(dst, io.TeeReader(io.LimitReader(reader, artifact.SizeBytes+1), hash))
	if err != nil {
		return err
	}
	if written != artifact.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
		return fmt.Errorf("artifact %q failed size or SHA-256 verification", artifact.Key)
	}
	return dst.Sync()
}

func (r *Reader) validateArtifact(sandboxID string, artifact Artifact) error {
	if artifact.SizeBytes < 0 || !validSHA256(artifact.SHA256) {
		return fmt.Errorf("artifact %q has invalid metadata", artifact.Key)
	}
	return r.validateObjectKey(sandboxID, artifact.Key)
}

func (r *Reader) validateObjectKey(sandboxID, key string) error {
	clean := path.Clean(key)
	wantPrefix := path.Join(r.prefix, sandboxID) + "/"
	if key == "" || strings.HasPrefix(key, "/") || clean != key || !strings.HasPrefix(clean, wantPrefix) {
		return fmt.Errorf("object key %q is outside sandbox checkpoint prefix", key)
	}
	return nil
}

func (r *Reader) readObject(ctx context.Context, key string) ([]byte, error) {
	reader, err := r.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func createRestoreTemp(destination string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return nil, err
	}
	return os.CreateTemp(filepath.Dir(destination), ".restore-*")
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
