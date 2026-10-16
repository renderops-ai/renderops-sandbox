package agent

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	workerv1 "backend/internal/rpc/worker/v1"
	"backend/internal/terminal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeTerminalWorker is a worker's terminal service: it records requests and
// can answer like a worker that predates terminal commands.
type fakeTerminalWorker struct {
	workerv1.UnimplementedWorkerTerminalServiceServer
	ignoreCommands bool

	mu     sync.Mutex
	opened []*workerv1.OpenTerminalRequest
	closed []string
	tokens [][]string
}

func (f *fakeTerminalWorker) OpenTerminal(ctx context.Context, request *workerv1.OpenTerminalRequest) (*workerv1.OpenTerminalResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, request)
	f.tokens = append(f.tokens, metadata.ValueFromIncomingContext(ctx, workerTokenMetadata))
	applied := !f.ignoreCommands && (len(request.GetCommand()) > 0 || request.GetCwd() != "" || request.GetEnvFile() != "")
	return &workerv1.OpenTerminalResponse{TerminalId: request.GetTerminalId(), State: "ready", CommandApplied: applied}, nil
}

func (f *fakeTerminalWorker) CloseTerminal(ctx context.Context, request *workerv1.CloseTerminalRequest) (*workerv1.CloseTerminalResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = append(f.closed, request.GetTerminalId())
	f.tokens = append(f.tokens, metadata.ValueFromIncomingContext(ctx, workerTokenMetadata))
	return &workerv1.CloseTerminalResponse{TerminalId: request.GetTerminalId(), State: "closed"}, nil
}

func startTerminalWorker(t *testing.T, worker *fakeTerminalWorker) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	workerv1.RegisterWorkerTerminalServiceServer(server, worker)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return "http://" + listener.Addr().String()
}

func TestTerminalPoolSendsTheCommand(t *testing.T) {
	worker := &fakeTerminalWorker{}
	endpoint := startTerminalWorker(t, worker)
	pool := NewTerminalPool("worker-secret")
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := terminal.Command{Argv: []string{"claude", "--continue"}, Cwd: "/workspace/repo", EnvFile: "/var/lib/spawn/cli/env"}
	if err := pool.OpenTerminal(ctx, endpoint, "sandbox-1", "term_1", "/bin/bash", 120, 32, start); err != nil {
		t.Fatalf("OpenTerminal: %v", err)
	}
	if err := pool.OpenTerminal(ctx, endpoint, "sandbox-1", "term_2", "/bin/bash", 120, 32, terminal.Command{}); err != nil {
		t.Fatalf("plain OpenTerminal: %v", err)
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	first := worker.opened[0]
	if !slices.Equal(first.GetCommand(), start.Argv) || first.GetCwd() != start.Cwd || first.GetEnvFile() != start.EnvFile {
		t.Fatalf("worker received %+v", first)
	}
	if second := worker.opened[1]; len(second.GetCommand()) != 0 || second.GetCwd() != "" || second.GetEnvFile() != "" {
		t.Fatalf("plain terminal sent %+v", second)
	}
	if len(worker.closed) != 0 {
		t.Fatalf("terminals closed: %v", worker.closed)
	}
}

func TestTerminalPoolClosesAPlainShellFromAnOldWorker(t *testing.T) {
	worker := &fakeTerminalWorker{ignoreCommands: true}
	endpoint := startTerminalWorker(t, worker)
	pool := NewTerminalPool("worker-secret")
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pool.OpenTerminal(ctx, endpoint, "sandbox-1", "term_old", "/bin/bash", 120, 32, terminal.Command{Argv: []string{"claude"}})
	if !errors.Is(err, terminal.ErrCommandUnsupported) {
		t.Fatalf("err = %v, want ErrCommandUnsupported", err)
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if !slices.Equal(worker.closed, []string{"term_old"}) {
		t.Fatalf("closed %v, want the plain shell closed", worker.closed)
	}
	// Each call carries the worker token exactly once (the worker's auth
	// refuses repeated values).
	for i, tokens := range worker.tokens {
		if !slices.Equal(tokens, []string{"worker-secret"}) {
			t.Fatalf("call %d tokens = %v", i, tokens)
		}
	}
}
