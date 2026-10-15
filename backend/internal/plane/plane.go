// Package plane defines the wire contract between the control plane and host
// agents: routes, auth header, lifecycle states, and request/response types.
// It must import nothing else from the backend so that neither plane pulls in
// the other's runtime (Firecracker/cgroups on one side, Postgres/auth on the
// other).
package plane

// The agent binds loopback only; the control plane reaches it through an SSH
// tunnel and authenticates every call with the AuthHeader shared secret.
const (
	DefaultAgentAddr = "127.0.0.1:9876"

	AuthHeader = "X-Worker-Token"

	RouteHealth        = "/worker/health"
	RouteCapacity      = "/worker/capacity"
	RouteSandbox       = "/worker/sandbox"  // POST — create
	RouteSandboxPrefix = "/worker/sandbox/" // {id}[/run|exec|pause|resume]; PUT|DELETE /{id}/git-access; DELETE /{id}
	RouteSandboxIDE    = "/worker/sandbox/{sandboxID}/ide"
)

type IDEInstance struct {
	Port  uint16 `json:"port"`
	State string `json:"state"`
}

// SessionState is a sandbox lifecycle state as it crosses the wire.
type SessionState string

const (
	StateActive SessionState = "active" // live VM holding a slot + RAM
	StatePaused SessionState = "paused" // snapshotted to disk; no live VM, slot/RAM freed
)
