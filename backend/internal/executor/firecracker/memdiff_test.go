package firecracker

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

const testPage = 4096

func pageOf(b byte) []byte { return bytes.Repeat([]byte{b}, testPage) }

func TestBuildMemoryWritesChangedPagesOverBase(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.mem")
	if err := os.WriteFile(base, bytes.Join([][]byte{pageOf('a'), pageOf('b'), pageOf('c'), pageOf('d')}, nil), 0o600); err != nil {
		t.Fatal(err)
	}
	// A Firecracker diff: full-size sparse file with only the dirty pages written.
	// Page 3 was changed to zeros, which must still overwrite the base.
	diff := filepath.Join(dir, "diff")
	f, err := os.Create(diff)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(4 * testPage); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(pageOf('X'), 1*testPage); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(pageOf(0), 3*testPage); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(diff); err != nil {
		t.Fatal(err)
	} else if allocatedBytes(info) >= info.Size() {
		// e.g. APFS stores these small holes as zeros. Firecracker's scratch
		// filesystems (tmpfs, XFS) and Linux CI keep them as holes.
		t.Skip("temp filesystem does not keep holes in small sparse files")
	}

	dst := filepath.Join(dir, "mem")
	build, err := (&FireCrackerManager{}).buildMemory(base, false, diff, dst)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join([][]byte{pageOf('a'), pageOf('X'), pageOf('c'), pageOf(0)}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("built memory differs from base with diff applied")
	}
	if build.changedBytes < 2*testPage {
		t.Fatalf("changed bytes = %d, want at least the 2 dirty pages", build.changedBytes)
	}
	if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}
}

func TestBuildMemoryRejectsDiffOfOtherSize(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.mem")
	diff := filepath.Join(dir, "diff")
	if err := os.WriteFile(base, pageOf('a'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(diff, bytes.Repeat(pageOf('b'), 2), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (&FireCrackerManager{}).buildMemory(base, false, diff, filepath.Join(dir, "mem")); err == nil {
		t.Fatal("expected size mismatch error")
	}
}

func TestSparseCopyKeepsContent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	data := append(bytes.Repeat([]byte{0}, memoryCopyChunk), pageOf('z')...) // an empty chunk, then data
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "dst")
	if err := sparseCopy(src, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("sparse copy differs from source")
	}
}
