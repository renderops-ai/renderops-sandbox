package worker

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"strings"

	workerv1 "backend/internal/rpc/worker/v1"
	"backend/internal/session"
	"backend/internal/terminal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const workerTokenMetadata = "x-worker-token"

type TerminalGRPCServer struct {
	workerv1.UnimplementedWorkerTerminalServiceServer
	svc session.Service
}

func NewTerminalGRPCServer(svc session.Service) *TerminalGRPCServer {
	return &TerminalGRPCServer{svc: svc}
}

func (s *TerminalGRPCServer) OpenTerminal(ctx context.Context, request *workerv1.OpenTerminalRequest) (*workerv1.OpenTerminalResponse, error) {
	if request.GetTerminalId() == "" || request.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "terminal_id and sandbox_id are required")
	}
	if request.GetShell() != "/bin/bash" {
		return nil, status.Error(codes.InvalidArgument, "unsupported terminal shell")
	}
	if request.GetColumns() < 20 || request.GetColumns() > 500 || request.GetRows() < 5 || request.GetRows() > 200 {
		return nil, status.Error(codes.InvalidArgument, "terminal size is outside the supported range")
	}
	start := terminal.Command{Argv: request.GetCommand(), Cwd: request.GetCwd(), EnvFile: request.GetEnvFile()}
	var err error
	if start.IsZero() {
		err = s.svc.OpenTerminal(ctx, request.GetSandboxId(), request.GetTerminalId(), request.GetShell(),
			uint16(request.GetColumns()), uint16(request.GetRows()))
	} else {
		if validateErr := start.Validate(); validateErr != nil {
			return nil, status.Error(codes.InvalidArgument, validateErr.Error())
		}
		opener, ok := s.svc.(envTerminalOpener)
		if !ok {
			return nil, status.Error(codes.Unimplemented, "this worker cannot start terminal commands")
		}
		err = opener.OpenTerminalWithEnv(ctx, request.GetSandboxId(), request.GetTerminalId(), request.GetShell(),
			uint16(request.GetColumns()), uint16(request.GetRows()), start.StartupEnv())
	}
	if err != nil {
		slog.Error("guest terminal creation failed",
			"sandbox_id", request.GetSandboxId(),
			"terminal_id", request.GetTerminalId(),
			"err", err,
		)
		return nil, terminalStatus(err)
	}
	return &workerv1.OpenTerminalResponse{
		TerminalId:     request.GetTerminalId(),
		State:          "ready",
		CommandApplied: !start.IsZero(),
	}, nil
}

// envTerminalOpener opens a terminal whose shell gets extra environment
// (session.Manager); terminal commands need it.
type envTerminalOpener interface {
	OpenTerminalWithEnv(ctx context.Context, sessionID, terminalID, shell string, columns, rows uint16, env map[string]string) error
}

func (s *TerminalGRPCServer) CloseTerminal(ctx context.Context, request *workerv1.CloseTerminalRequest) (*workerv1.CloseTerminalResponse, error) {
	if request.GetTerminalId() == "" || request.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "terminal_id and sandbox_id are required")
	}
	if err := s.svc.CloseTerminal(ctx, request.GetSandboxId(), request.GetTerminalId()); err != nil {
		return nil, terminalStatus(err)
	}
	return &workerv1.CloseTerminalResponse{TerminalId: request.GetTerminalId(), State: "closed"}, nil
}

func (s *TerminalGRPCServer) AttachTerminal(stream grpc.BidiStreamingServer[workerv1.TerminalFrame, workerv1.TerminalFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetType() != workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_ATTACH || first.GetTerminalId() == "" || first.GetSandboxId() == "" {
		return status.Error(codes.InvalidArgument, "first terminal frame must identify the terminal attachment")
	}
	adapter := &terminalGRPCStream{stream: stream}
	if err := s.svc.AttachTerminal(stream.Context(), first.GetSandboxId(), first.GetTerminalId(), adapter); err != nil {
		return terminalStatus(err)
	}
	return nil
}

func UnaryTokenAuth(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return handler(ctx, request)
		}
		values := metadata.ValueFromIncomingContext(ctx, workerTokenMetadata)
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid or missing worker token")
		}
		return handler(ctx, request)
	}
}

func StreamTokenAuth(token string) grpc.StreamServerInterceptor {
	return func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if token != "" && !validWorkerToken(stream.Context(), token) {
			return status.Error(codes.Unauthenticated, "invalid or missing worker token")
		}
		return handler(service, stream)
	}
}

func validWorkerToken(ctx context.Context, token string) bool {
	values := metadata.ValueFromIncomingContext(ctx, workerTokenMetadata)
	return len(values) == 1 && subtle.ConstantTimeCompare([]byte(values[0]), []byte(token)) == 1
}

type terminalGRPCStream struct {
	stream grpc.BidiStreamingServer[workerv1.TerminalFrame, workerv1.TerminalFrame]
}

func (s *terminalGRPCStream) Send(frame terminal.Frame) error {
	return s.stream.Send(terminalFrameToProto(frame))
}

func (s *terminalGRPCStream) Receive() (terminal.Frame, error) {
	frame, err := s.stream.Recv()
	if err != nil {
		return terminal.Frame{}, err
	}
	return terminalFrameFromProto(frame)
}

func terminalFrameToProto(frame terminal.Frame) *workerv1.TerminalFrame {
	result := &workerv1.TerminalFrame{
		Data: frame.Data, Columns: frame.Columns, Rows: frame.Rows,
		ExitCode: frame.ExitCode, Message: frame.Message,
	}
	switch frame.Type {
	case terminal.FrameReady:
		result.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_READY
	case terminal.FrameOutput:
		result.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_OUTPUT
	case terminal.FrameExit:
		result.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_EXIT
	case terminal.FrameError:
		result.Type = workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_ERROR
	}
	return result
}

func terminalFrameFromProto(frame *workerv1.TerminalFrame) (terminal.Frame, error) {
	result := terminal.Frame{Data: frame.GetData(), Columns: frame.GetColumns(), Rows: frame.GetRows()}
	switch frame.GetType() {
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_INPUT:
		result.Type = terminal.FrameInput
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_RESIZE:
		result.Type = terminal.FrameResize
	case workerv1.TerminalFrameType_TERMINAL_FRAME_TYPE_CLOSE:
		result.Type = terminal.FrameClose
	default:
		return terminal.Frame{}, status.Error(codes.InvalidArgument, "unsupported terminal client frame")
	}
	return result, nil
}

func terminalStatus(err error) error {
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "not found"):
		return status.Error(codes.NotFound, err.Error())
	case strings.Contains(message, "not active"):
		return status.Error(codes.FailedPrecondition, err.Error())
	case strings.Contains(message, "already exists"):
		return status.Error(codes.AlreadyExists, err.Error())
	case strings.Contains(message, "limit reached"):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
