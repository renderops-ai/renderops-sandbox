package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"backend/internal/executor/firecracker"
	"backend/internal/sandboximage"
	"backend/internal/template"
	"backend/internal/vmsize"
	workerconfig "backend/internal/workerplane/config"
)

// Images served next to the default Alpine image. Each is a rootfs built by the
// "Agents image" workflow and stored in Blob as
//
//	rootfs-release/<image>/<version>/rootfs.ext4   (gzip, read via CompressedStore)
//
// and pinned per worker in WORKER_IMAGES ("agents=<version>@<sha256>,..."), which
// the deploy writes from ops/worker/images.lock.

type extraImage struct {
	Name    string
	Version string
	SHA256  string
}

var extraImagePattern = regexp.MustCompile(`^([a-z0-9][a-z0-9._-]{0,63})=([A-Za-z0-9][A-Za-z0-9._-]{0,127})@([0-9a-f]{64})$`)

func parseWorkerImages(value string) ([]extraImage, error) {
	var images []extraImage
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		m := extraImagePattern.FindStringSubmatch(entry)
		if m == nil {
			return nil, fmt.Errorf("invalid WORKER_IMAGES entry %q (want image=version@sha256)", entry)
		}
		if m[1] == sandboximage.Default {
			return nil, fmt.Errorf("WORKER_IMAGES must not list the default image %q", m[1])
		}
		images = append(images, extraImage{Name: m[1], Version: m[2], SHA256: m[3]})
	}
	return images, nil
}

// imageSizes returns the sizes an image is served at on this worker.
func imageSizes(image string) []vmsize.Size {
	if image == sandboximage.Default {
		return vmsize.Sizes
	}
	var sizes []vmsize.Size
	for _, name := range sandboximage.Sizes(image) {
		if sz, err := vmsize.ByName(name); err == nil {
			sizes = append(sizes, sz)
		}
	}
	return sizes
}

// addExtraImages fetches each pinned image's rootfs, builds its templates and adds
// it to imageConfigs. An image that cannot be fetched or templated is left out,
// so the worker keeps serving the others and never advertises a broken image.
// Returns the extra images' templates keyed by pool key.
func addExtraImages(ctx context.Context, cfg *workerconfig.Config, vmManager *firecracker.FireCrackerManager, baseCfg firecracker.VMConfig, imageConfigs map[string]firecracker.VMConfig) map[string]*firecracker.SnapshotTemplate {
	templates := map[string]*firecracker.SnapshotTemplate{}
	images, err := parseWorkerImages(env("WORKER_IMAGES", ""))
	if err != nil {
		slog.Error("extra images disabled", "err", err)
		return templates
	}
	if len(images) == 0 {
		return templates
	}
	if env("TEMPLATE_SOURCE", "build") != "build" {
		slog.Warn("extra images need TEMPLATE_SOURCE=build; not serving them", "count", len(images))
		return templates
	}
	baseStore, err := blobStoreFromEnv()
	if err != nil {
		slog.Error("extra images disabled", "err", err)
		return templates
	}
	store := template.NewCompressedStore(baseStore)

	for _, img := range images {
		logger := slog.With("image", img.Name, "version", img.Version)
		sizes := imageSizes(img.Name)
		if len(sizes) == 0 {
			logger.Error("image has no sizes in sandboximage; not serving it")
			continue
		}
		rootfs, err := fetchImageRootfs(ctx, store, filepath.Join(cfg.RootDirectory, "images"), img)
		if err != nil {
			logger.Error("fetch image rootfs failed; not serving it", "err", err)
			continue
		}
		imageCfg := baseCfg
		imageCfg.RootfsPath = rootfs

		imageTemplates := map[string]*firecracker.SnapshotTemplate{}
		for _, sz := range sizes {
			szCfg := imageCfg
			szCfg.VCPUCount, szCfg.MemSizeMiB, szCfg.DiskGB = sz.VCPUs, sz.MemoryMB, sz.DiskGB
			// On disk, not /dev/shm: these templates are large (4 GB of guest memory).
			snapDir := filepath.Join(cfg.RootDirectory, "snapshots", img.Name+"-"+sz.Name)
			if err := os.MkdirAll(snapDir, 0o755); err != nil {
				logger.Error("create image snapshot dir failed", "dir", snapDir, "err", err)
				break
			}
			if cfg.FCRunUID > 0 {
				if err := os.Chown(snapDir, cfg.FCRunUID, cfg.FCRunGID); err != nil {
					logger.Error("chown image snapshot dir failed", "dir", snapDir, "err", err)
					break
				}
			}
			tmpl, err := createTemplateWithRetry(vmManager, szCfg, snapDir, 3)
			if err != nil {
				logger.Error("image template failed; not serving it", "size", sz.Name, "err", err)
				break
			}
			imageTemplates[sandboximage.PoolKey(img.Name, sz.Key())] = tmpl
		}
		if len(imageTemplates) != len(sizes) {
			continue
		}
		imageConfigs[img.Name] = imageCfg
		for key, tmpl := range imageTemplates {
			templates[key] = tmpl
		}
		logger.Info("image ready", "rootfs", rootfs, "sizes", len(sizes))
	}
	return templates
}

// fetchImageRootfs returns the local path of a pinned image rootfs, downloading and
// verifying it on first use. Versions are cached side by side and never modified.
func fetchImageRootfs(ctx context.Context, store template.ArtifactStore, dir string, img extraImage) (string, error) {
	path := filepath.Join(dir, img.Name, img.Version, "rootfs.ext4")
	if sum, err := template.SHA256File(path); err == nil {
		if sum == img.SHA256 {
			return path, nil
		}
		return "", fmt.Errorf("cached %s has sha256 %s, pinned %s", path, sum, img.SHA256)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create image dir: %w", err)
	}
	key := fmt.Sprintf("rootfs-release/%s/%s/rootfs.ext4", img.Name, img.Version)
	slog.Info("downloading image rootfs", "image", img.Name, "key", key)
	rc, err := store.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", key, err)
	}
	defer rc.Close()

	tmp, err := os.CreateTemp(filepath.Dir(path), ".rootfs-*.ext4")
	if err != nil {
		return "", fmt.Errorf("create temp rootfs: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after the rename below succeeds
	hash := sha256.New()
	if err := writeSparse(tmp, io.TeeReader(rc, hash)); err != nil {
		tmp.Close()
		return "", fmt.Errorf("write %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp rootfs: %w", err)
	}
	if sum := hex.EncodeToString(hash.Sum(nil)); sum != img.SHA256 {
		return "", fmt.Errorf("%s has sha256 %s, pinned %s", key, sum, img.SHA256)
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return "", fmt.Errorf("chmod rootfs: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", fmt.Errorf("install rootfs: %w", err)
	}
	return path, nil
}

// writeSparse copies r into f, seeking over all-zero blocks so a mostly empty
// ext4 image does not occupy its full size on disk.
func writeSparse(f *os.File, r io.Reader) error {
	const block = 64 << 10
	buf := make([]byte, block)
	zero := make([]byte, block)
	var size int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if bytes.Equal(buf[:n], zero[:n]) {
				if _, serr := f.Seek(int64(n), io.SeekCurrent); serr != nil {
					return serr
				}
			} else if _, werr := f.Write(buf[:n]); werr != nil {
				return werr
			}
			size += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return f.Truncate(size)
		}
		if err != nil {
			return err
		}
	}
}
