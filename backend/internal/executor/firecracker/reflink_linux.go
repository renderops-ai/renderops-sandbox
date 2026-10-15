package firecracker

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile makes dst share src's extents (FICLONE). It fails with EXDEV across
// filesystems and EOPNOTSUPP where the filesystem has no reflink support.
func cloneFile(dst, src *os.File) error {
	return unix.IoctlFileClone(int(dst.Fd()), int(src.Fd()))
}
