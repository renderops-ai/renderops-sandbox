//go:build !linux

package slotnet

import (
	"errors"
	"net"
)

// GatewayAddr is the address every slot's TAP holds: the guest's gateway.
const GatewayAddr = "172.16.0.1"

func Listen(int, string) (net.Listener, error) {
	return nil, errors.New("slot listeners require Linux network namespaces")
}
