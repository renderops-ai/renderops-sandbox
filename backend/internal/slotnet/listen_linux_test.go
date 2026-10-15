//go:build linux

package slotnet

import (
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestListenServesOnlyInsideTheNamespace needs root and a prepared
// fc-ns-0 namespace holding 172.16.0.1, e.g.:
//
//	ip netns add fc-ns-0
//	ip netns exec fc-ns-0 ip link add dummy0 type dummy
//	ip netns exec fc-ns-0 ip addr add 172.16.0.1/30 dev dummy0
//	ip netns exec fc-ns-0 ip link set dummy0 up
//	SLOTNET_NETNS_TEST=1 go test -run TestListen ./internal/slotnet
func TestListenServesOnlyInsideTheNamespace(t *testing.T) {
	if os.Getenv("SLOTNET_NETNS_TEST") == "" || os.Geteuid() != 0 {
		t.Skip("needs root and a prepared fc-ns-0 namespace (SLOTNET_NETNS_TEST=1)")
	}
	before, err := os.Readlink("/proc/thread-self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := Listen(0, "172.16.0.1:7080")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		_ = http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "from-slot")
		}))
	}()

	out, err := exec.Command("ip", "netns", "exec", "fc-ns-0", "wget", "-qO-", "http://172.16.0.1:7080/").CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "from-slot" {
		t.Fatalf("guest-side request: err=%v out=%q", err, out)
	}
	if conn, err := net.DialTimeout("tcp", "172.16.0.1:7080", time.Second); err == nil {
		conn.Close()
		t.Fatal("listener reachable from the worker namespace; it must exist only inside the sandbox namespace")
	}
	after, err := os.Readlink("/proc/thread-self/ns/net")
	if err != nil || after != before {
		t.Fatalf("calling thread left in namespace %q (was %q): %v", after, before, err)
	}
}
