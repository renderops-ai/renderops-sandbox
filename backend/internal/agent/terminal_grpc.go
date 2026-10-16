package agent

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	workerv1 "backend/internal/rpc/worker/v1"
	"backend/internal/terminal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const workerTokenMetadata = "x-worker-token"

type TerminalPool struct {
	mu          sync.Mutex
	workerToken string
	connections map[string]*grpc.ClientConn
}

func NewTerminalPool(workerToken string) *TerminalPool {
	return &TerminalPool{workerToken: workerToken, connections: make(map[string]*grpc.ClientConn)}
}

// OpenTerminal has the worker open a guest PTY. A non-zero start runs a
// command in it (terminal.Command); a worker that cannot apply start would
// open a plain shell instead, so that terminal is closed again and the call
// fails with terminal.ErrCommandUnsupported.
func (p *TerminalPool) OpenTerminal(ctx context.Context, endpoint, sandboxID, terminalID, shell string, columns, rows uint32, start terminal.Command) error {
	client, err := p.client(endpoint)
	if err != nil {
		return err
	}
	response, err := client.OpenTerminal(metadata.AppendToOutgoingContext(ctx, workerTokenMetadata, p.workerToken), &workerv1.OpenTerminalRequest{
		TerminalId: terminalID,
		SandboxId:  sandboxID,
		Shell:      shell,
		Columns:    columns,
		Rows:       rows,
		Command:    start.Argv,
		Cwd:        start.Cwd,
		EnvFile:    start.EnvFile,
	})
	if err != nil {
		return fmt.Errorf("worker open terminal: %w", err)
	}
	if response.GetTerminalId() != terminalID || response.GetState() != "ready" {
		return fmt.Errorf("worker returned invalid terminal readiness")
	}
	if !start.IsZero() && !response.GetCommandApplied() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if closeErr := p.CloseTerminal(closeCtx, endpoint, sandboxID, terminalID); closeErr != nil {
			return fmt.Errorf("%w (closing its plain shell failed: %v)", terminal.ErrCommandUnsupported, closeErr)
		}
		return terminal.ErrCommandUnsupported
	}
	return nil
}

func (p *TerminalPool) CloseTerminal(ctx context.Context, endpoint, sandboxID, terminalID string) error {
	client, err := p.client(endpoint)
	if err != nil {
		return err
	}
	ctx = metadata.AppendToOutgoingContext(ctx, workerTokenMetadata, p.workerToken)
	_, err = client.CloseTerminal(ctx, &workerv1.CloseTerminalRequest{TerminalId: terminalID, SandboxId: sandboxID})
	if err != nil {
		return fmt.Errorf("worker close terminal: %w", err)
	}
	return nil
}

func (p *TerminalPool) AttachTerminal(ctx context.Context, endpoint, sandboxID, terminalID string) (terminal.Stream, error) {
	client, err := p.client(endpoint)
	if err != nil {
		return nil, err
	}
	ctx = metadata.AppendToOutgoingContext(ctx, workerTokenMetadata, p.workerToken)
	stream, err := client.AttachTerminal(ctx)
	if err != nil {
		return nil, fmt.Errorf("worker attach terminal: %w", err)
	}
	if err := stream.Send(&workerv1.TerminalFrame{
		Type:       workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_ATTACH,
		TerminalId: terminalID,
		SandboxId:  sandboxID,
	}); err != nil {
		return nil, fmt.Errorf("send worker terminal attachment: %w", err)
	}
	attachment := &terminalClientStream{stream: stream}
	frame, err := attachment.Receive()
	if err != nil {
		return nil, fmt.Errorf("wait for worker terminal readiness: %w", err)
	}
	if frame.Type != terminal.FrameReady {
		return nil, fmt.Errorf("worker terminal did not become ready")
	}
	return attachment, nil
}

func (p *TerminalPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var first error
	for endpoint, connection := range p.connections {
		if err := connection.Close(); err != nil && first == nil {
			first = fmt.Errorf("close terminal connection %s: %w", endpoint, err)
		}
		delete(p.connections, endpoint)
	}
	return first
}

func (p *TerminalPool) client(endpoint string) (workerv1.WorkerTerminalServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if connection := p.connections[endpoint]; connection != nil {
		return workerv1.NewWorkerTerminalServiceClient(connection), nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid private worker endpoint %q", endpoint)
	}
	connection, err := grpc.NewClient(parsed.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create worker gRPC client: %w", err)
	}
	p.connections[endpoint] = connection
	return workerv1.NewWorkerTerminalServiceClient(connection), nil
}

type terminalClientStream struct {
	stream grpc.BidiStreamingClient[workerv1.TerminalFrame, workerv1.TerminalFrame]
}

func (s *terminalClientStream) Send(frame terminal.Frame) error {
	request := &workerv1.TerminalFrame{Data: frame.Data, Columns: frame.Columns, Rows: frame.Rows}
	switch frame.Type {
	case terminal.FrameInput:
		request.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_INPUT
	case terminal.FrameResize:
		request.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_RESIZE
	case terminal.FrameClose:
		request.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_CLOSE
	default:
		return fmt.Errorf("unsupported terminal frame %q", frame.Type)
	}
	return s.stream.Send(request)
}

func (s *terminalClientStream) Receive() (terminal.Frame, error) {
	response, err := s.stream.Recv()
	if err != nil {
		return terminal.Frame{}, err
	}
	frame := terminal.Frame{
		Data: response.GetData(), ExitCode: response.GetExitCode(), Message: response.GetMessage(),
	}
	switch response.GetType() {
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_READY:
		frame.Type = terminal.FrameReady
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_OUTPUT:
		frame.Type = terminal.FrameOutput
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_EXIT:
		frame.Type = terminal.FrameExit
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_ERROR:
		frame.Type = terminal.FrameError
	default:
		return terminal.Frame{}, fmt.Errorf("unsupported worker terminal frame %q", response.GetType())
	}
	return frame, nil
}
