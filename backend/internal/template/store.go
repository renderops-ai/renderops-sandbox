package template

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// ErrNotFound is returned by an ArtifactStore/ManifestRepository when a key or
// manifest does not exist, so callers can distinguish "missing" from "failed".
var ErrNotFound = errors.New("template: not found")

// ArtifactStore is the durable object store for release artifacts. Phase 1 ships
// only LocalStore; an Azure Blob / R2 / S3 implementation slots in behind this
// same interface later, reading its endpoint + credentials from the environment
// (never hardcoded). Keys are forward-slash object keys (see keys.go).
type ArtifactStore interface {
	// Put writes the full contents of r under key, overwriting any existing
	// object. Implementations must make the write atomic (a reader never sees a
	// half-written object) since release artifacts are treated as immutable.
	Put(ctx context.Context, key string, r io.Reader) error
	// Get opens key for reading. Returns ErrNotFound if the object is absent.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Stat reports an object's size and whether it exists.
	Stat(ctx context.Context, key string) (size int64, exists bool, err error)
	// List returns all keys under prefix (recursively), as forward-slash keys.
	List(ctx context.Context, prefix string) ([]string, error)
}

// LocalStore is a filesystem-backed ArtifactStore rooted at a directory. It
// exists for development and tests — the builder can publish to a local folder
// and a worker can sync from it with no cloud dependency. Object keys map to
// paths under root; "/" in a key becomes a subdirectory.
type LocalStore struct {
	root string
}

// NewLocalStore returns a LocalStore rooted at dir, creating it if needed.
func NewLocalStore(dir string) (*LocalStore, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve local store dir: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("create local store dir %s: %w", abs, err)
	}
	return &LocalStore{root: abs}, nil
}

var _ ArtifactStore = (*LocalStore)(nil)

// resolve maps an object key to an absolute path under root, rejecting any key
// that would escape root (defense against a malformed key doing path traversal).
func (s *LocalStore) resolve(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	clean := path.Clean(key)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("key %q escapes store root", key)
	}
	full := filepath.Join(s.root, filepath.FromSlash(clean))
	if full != s.root && !strings.HasPrefix(full, s.root+string(os.PathSeparator)) {
		return "", fmt.Errorf("key %q escapes store root", key)
	}
	return full, nil
}

func (s *LocalStore) Put(_ context.Context, key string, r io.Reader) error {
	full, err := s.resolve(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return fmt.Errorf("create dir for %s: %w", key, err)
	}
	// Write to a temp file in the same directory, then rename — so a reader never
	// observes a partially written object.
	tmp, err := os.CreateTemp(filepath.Dir(full), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", key, err)
	}
	tmpName := tmp.Name()
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp for %s: %w", key, err)
	}
	if err := os.Rename(tmpName, full); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("commit %s: %w", key, err)
	}
	return nil
}

// Delete removes key. A key that does not exist is not an error.
func (s *LocalStore) Delete(_ context.Context, key string) error {
	full, err := s.resolve(key)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *LocalStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	full, err := s.resolve(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	return f, nil
}

func (s *LocalStore) Stat(_ context.Context, key string) (int64, bool, error) {
	full, err := s.resolve(key)
	if err != nil {
		return 0, false, err
	}
	info, err := os.Stat(full)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("stat %s: %w", key, err)
	}
	return info.Size(), true, nil
}

func (s *LocalStore) List(_ context.Context, prefix string) ([]string, error) {
	base, err := s.resolve(prefix)
	if err != nil {
		return nil, err
	}
	var keys []string
	err = filepath.WalkDir(base, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil // prefix simply has no objects yet
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", prefix, err)
	}
	return keys, nil
}
