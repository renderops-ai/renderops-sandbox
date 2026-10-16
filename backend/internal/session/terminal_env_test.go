package session

import (
	"context"
	"encoding/json"
	"maps"
	"net"
	"testing"

	"backend/internal/executor/firecracker"
)

// TestTerminalEnvAddsToTheSandboxEnv: a terminal's extra env (a terminal
// command's startup) reaches the guest over the sandbox's own env, which is
// left unchanged for later terminals.
func TestTerminalEnvAddsToTheSandboxEnv(t *testing.T) {
	host, guest := net.Pipe()
	defer host.Close()
	defer guest.Close()
	received := make(chan map[string]string, 2)
	go func() {
		for {
			var req struct {
				Type string            `json:"type"`
				Env  map[string]string `json:"env"`
			}
			if err := json.NewDecoder(guest).Decode(&req); err != nil {
				return
			}
			received <- req.Env
			if err := json.NewEncoder(guest).Encode(map[string]bool{"success": true}); err != nil {
				return
			}
		}
	}()

	sandboxEnv := map[string]string{"A": "1", "PROMPT_COMMAND": "sandbox's own"}
	m := &Manager{store: NewStore(10), maxTerminals: 4}
	sess := &Session{ID: "s", State: StateActive, VM: &firecracker.MicroVM{}, VsockConn: host, Env: sandboxEnv}
	if err := m.store.Add(sess); err != nil {
		t.Fatal(err)
	}

	extra := map[string]string{"PROMPT_COMMAND": "startup"}
	if err := m.OpenTerminalWithEnv(context.Background(), "s", "term_1", "/bin/bash", 120, 32, extra); err != nil {
		t.Fatal(err)
	}
	if got := <-received; !maps.Equal(got, map[string]string{"A": "1", "PROMPT_COMMAND": "startup"}) {
		t.Fatalf("guest env = %v", got)
	}
	if err := m.OpenTerminal(context.Background(), "s", "term_2", "/bin/bash", 120, 32); err != nil {
		t.Fatal(err)
	}
	if got := <-received; !maps.Equal(got, sandboxEnv) {
		t.Fatalf("plain terminal env = %v", got)
	}
	if sandboxEnv["PROMPT_COMMAND"] != "sandbox's own" || len(sess.Terminals) != 2 {
		t.Fatalf("sandbox env changed: %v (terminals %d)", sandboxEnv, len(sess.Terminals))
	}
}
