//go:build !linux

package gitproxy

import (
	"errors"
	"net"
)

func listenInSlot(int, string) (net.Listener, error) {
	return nil, errors.New("git proxy requires Linux network namespaces")
}
