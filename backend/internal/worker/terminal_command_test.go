package worker

import (
	"context"
	"maps"
	"testing"

	workerv1 "backend/internal/rpc/worker/v1"
	"backend/internal/terminal"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// envSessionService is a session service that can open terminals with extra
// environment (session.Manager can).
type envSessionService struct {
	fakeSessionService
	openedEnv  map[string]string
	envOpens   int
	plainOpens int
}

func (f *envSessionService) OpenTerminal(ctx context.Context, sandboxID, terminalID, shell string, columns, rows uint16) error {
	f.plainOpens++
	return f.fakeSessionService.OpenTerminal(ctx, sandboxID, terminalID, shell, columns, rows)
}

func (f *envSessionService) OpenTerminalWithEnv(_ context.Context, sandboxID, terminalID, _ string, _, _ uint16, env map[string]string) error {
	f.envOpens++
	f.openedSandbox, f.openedTerminal, f.openedEnv = sandboxID, terminalID, env
	return nil
}

func commandRequest(start terminal.Command) *workerv1.OpenTerminalRequest {
	return &workerv1.OpenTerminalRequest{
		TerminalId: "term_cmd", SandboxId: "sandbox-1", Shell: "/bin/bash", Columns: 120, Rows: 32,
		Command: start.Argv, Cwd: start.Cwd, EnvFile: start.EnvFile,
	}
}

func TestTerminalGRPCStartsACommand(t *testing.T) {
	service := &envSessionService{}
	server := NewTerminalGRPCServer(service)
	start := terminal.Command{Argv: []string{"claude", "--continue"}, Cwd: "/workspace/repo", EnvFile: "/var/lib/spawn/cli/env"}

	opened, err := server.OpenTerminal(context.Background(), commandRequest(start))
	if err != nil {
		t.Fatalf("OpenTerminal: %v", err)
	}
	if !opened.GetCommandApplied() || opened.GetState() != "ready" || service.envOpens != 1 || service.plainOpens != 0 {
		t.Fatalf("response %+v, env opens %d, plain opens %d", opened, service.envOpens, service.plainOpens)
	}
	if !maps.Equal(service.openedEnv, start.StartupEnv()) || service.openedTerminal != "term_cmd" {
		t.Fatalf("guest env = %v", service.openedEnv)
	}

	// A plain terminal is opened as before, and says nothing was applied.
	plain, err := server.OpenTerminal(context.Background(), commandRequest(terminal.Command{}))
	if err != nil || plain.GetCommandApplied() || service.plainOpens != 1 || service.envOpens != 1 {
		t.Fatalf("plain open = %+v, %v", plain, err)
	}
}

func TestTerminalGRPCValidatesCommands(t *testing.T) {
	service := &envSessionService{}
	server := NewTerminalGRPCServer(service)
	for _, start := range []terminal.Command{
		{Argv: []string{"-c", "x"}},
		{Cwd: "/etc"},
		{EnvFile: "/workspace/.env"},
		{Argv: []string{"claude"}, Cwd: "/workspace/../etc"},
	} {
		_, err := server.OpenTerminal(context.Background(), commandRequest(start))
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%+v: code %s, want InvalidArgument", start, status.Code(err))
		}
	}
	if service.envOpens+service.plainOpens != 0 {
		t.Fatal("an invalid command reached the guest")
	}
}

func TestTerminalGRPCCommandNeedsEnvSupport(t *testing.T) {
	server := NewTerminalGRPCServer(&fakeSessionService{})
	_, err := server.OpenTerminal(context.Background(), commandRequest(terminal.Command{Argv: []string{"claude"}}))
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code %s, want Unimplemented", status.Code(err))
	}
}
