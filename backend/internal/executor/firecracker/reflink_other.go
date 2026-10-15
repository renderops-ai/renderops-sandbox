//go:build !linux

package firecracker

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile is Linux-only (FICLONE); elsewhere callers fall back to copying.
func cloneFile(_, _ *os.File) error {
	return unix.EOPNOTSUPP
}
