//go:build linux

// Package slotnet opens worker-side listeners inside a sandbox slot's network
// namespace (fc-ns-<slot>). A guest reaches them on its gateway address
// (172.16.0.1); that traffic is delivered locally in the namespace, so it is
// unaffected by the host egress firewall and by the sandbox's internet switch.
// Worker proxies that must never hand a credential to the guest (git, model
// inference) listen this way.
package slotnet

import (
	"fmt"
	"net"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// GatewayAddr is the address every slot's TAP holds: the guest's gateway.
const GatewayAddr = "172.16.0.1"

// Listen opens a TCP listener on address inside fc-ns-<slot>. A socket's network
// namespace is fixed when it is created, so the listener and every connection
// it accepts stay in the sandbox namespace, while upstream dials made by the
// serving goroutines use the worker's own namespace.
func Listen(slot int, address string) (net.Listener, error) {
	type result struct {
		listener net.Listener
		err      error
	}
	results := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		unlockThread := true
		defer func() {
			if unlockThread {
				runtime.UnlockOSThread()
			}
		}()

		current, err := os.Open("/proc/self/ns/net")
		if err != nil {
			results <- result{err: fmt.Errorf("open current network namespace: %w", err)}
			return
		}
		defer current.Close()
		target, err := os.Open(fmt.Sprintf("/var/run/netns/fc-ns-%d", slot))
		if err != nil {
			results <- result{err: fmt.Errorf("open sandbox network namespace: %w", err)}
			return
		}
		defer target.Close()
		if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
			results <- result{err: fmt.Errorf("enter sandbox network namespace: %w", err)}
			return
		}
		listener, listenErr := net.Listen("tcp", address)
		if restoreErr := unix.Setns(int(current.Fd()), unix.CLONE_NEWNET); restoreErr != nil {
			// Exiting while still locked makes the runtime discard this thread
			// instead of scheduling other Go work inside the sandbox namespace.
			unlockThread = false
			if listener != nil {
				_ = listener.Close()
			}
			results <- result{err: fmt.Errorf("restore worker network namespace: %w", restoreErr)}
			return
		}
		if listenErr != nil {
			results <- result{err: fmt.Errorf("listen on %s in slot %d: %w", address, slot, listenErr)}
			return
		}
		results <- result{listener: listener}
	}()
	r := <-results
	return r.listener, r.err
}
