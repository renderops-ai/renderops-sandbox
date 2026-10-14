package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

const (
	maxGuestTerminals  = 256
	terminalBufferSize = 256 * 1024
	terminalFrameLimit = 1 << 20

	terminalWireInput  byte = 1
	terminalWireOutput byte = 2
	terminalWireResize byte = 3
	terminalWireExit   byte = 4
	terminalWireError  byte = 5
	terminalWireClose  byte = 6
)

type terminalOpenRequest struct {
	ID       string
	Hostname string
	Shell    string
	Columns  uint16
	Rows     uint16
	Env      map[string]string
}

type terminalSession struct {
	id       string
	master   *os.File
	cmd      *exec.Cmd
	output   *byteRing
	done     chan struct{}
	mu       sync.Mutex
	nextSeq  uint64
	attached *terminalSubscription
	exitCode int
}

type terminalSubscription struct {
	afterSeq uint64
	output   chan []byte
	done     chan struct{}
	once     sync.Once
}

type terminalManager struct {
	mu       sync.Mutex
	sessions map[string]*terminalSession
	max      int
	workDir  string
}

func newTerminalManager(max int) *terminalManager {
	return &terminalManager{sessions: make(map[string]*terminalSession), max: max, workDir: "/workspace"}
}

func (m *terminalManager) Open(request terminalOpenRequest) error {
	if err := validateTerminalOpen(request); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.sessions[request.ID]; exists {
		return fmt.Errorf("terminal %s already exists", request.ID)
	}
	if len(m.sessions) >= m.max {
		return fmt.Errorf("terminal limit reached (%d)", m.max)
	}
	if request.Hostname != "" {
		if err := syscall.Sethostname([]byte(request.Hostname)); err != nil {
			return fmt.Errorf("set sandbox hostname: %w", err)
		}
	}

	cmd := exec.Command(request.Shell, "-l")
	cmd.Dir = m.workDir
	cmd.Env = mergeEnvironment(os.Environ(), request.Env)
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: request.Columns, Rows: request.Rows})
	if err != nil {
		return fmt.Errorf("start terminal shell: %w", err)
	}
	session := &terminalSession{
		id:     request.ID,
		master: master,
		cmd:    cmd,
		output: newByteRing(terminalBufferSize),
		done:   make(chan struct{}),
	}
	m.sessions[request.ID] = session
	go m.capture(session)
	return nil
}

func (m *terminalManager) Close(id string) error {
	m.mu.Lock()
	session, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("terminal %s not found", id)
	}
	stopTerminal(session)
	return nil
}

func (m *terminalManager) CloseAll() {
	m.mu.Lock()
	sessions := make([]*terminalSession, 0, len(m.sessions))
	for id, session := range m.sessions {
		sessions = append(sessions, session)
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	for _, session := range sessions {
		stopTerminal(session)
	}
}

func (m *terminalManager) Attach(conn io.ReadWriter, id string) error {
	m.mu.Lock()
	session, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		err := fmt.Errorf("terminal %s not found", id)
		_ = writeTerminalAttachResponse(conn, err)
		return err
	}

	subscription, backlog, err := session.subscribe()
	if err != nil {
		_ = writeTerminalAttachResponse(conn, err)
		return err
	}
	defer session.detach(subscription)
	if err := writeTerminalAttachResponse(conn, nil); err != nil {
		return err
	}
	if len(backlog) > 0 {
		if err := writeTerminalFrame(conn, terminalWireOutput, backlog); err != nil {
			return err
		}
	}

	inputs := make(chan terminalWireFrame, 1)
	go readTerminalInputs(conn, inputs)
	for {
		select {
		case output, open := <-subscription.output:
			if !open {
				exitCode := session.processExitCode()
				payload := make([]byte, 4)
				binary.BigEndian.PutUint32(payload, uint32(int32(exitCode)))
				return writeTerminalFrame(conn, terminalWireExit, payload)
			}
			if err := writeTerminalFrame(conn, terminalWireOutput, output); err != nil {
				return err
			}
		case frame := <-inputs:
			if frame.err != nil {
				return frame.err
			}
			switch frame.frameType {
			case terminalWireInput:
				if _, err := session.master.Write(frame.payload); err != nil {
					return err
				}
			case terminalWireResize:
				if len(frame.payload) != 4 {
					return errors.New("invalid terminal resize frame")
				}
				columns := binary.BigEndian.Uint16(frame.payload[0:2])
				rows := binary.BigEndian.Uint16(frame.payload[2:4])
				if columns < 20 || columns > 500 || rows < 5 || rows > 200 {
					return errors.New("terminal resize is outside the supported range")
				}
				if err := pty.Setsize(session.master, &pty.Winsize{Cols: columns, Rows: rows}); err != nil {
					return err
				}
			case terminalWireClose:
				return nil
			default:
				return fmt.Errorf("unsupported terminal frame %d", frame.frameType)
			}
		}
	}
}

func writeTerminalAttachResponse(writer io.Writer, attachErr error) error {
	response := struct {
		Success bool   `json:"success"`
		Error   string `json:"error,omitempty"`
	}{Success: attachErr == nil, Error: errorString(attachErr)}
	return json.NewEncoder(writer).Encode(response)
}

func (m *terminalManager) capture(session *terminalSession) {
	buffer := make([]byte, 32*1024)
	for {
		n, err := session.master.Read(buffer)
		if n > 0 {
			outputLogs.write("stdout", buffer[:n])
			session.publish(buffer[:n])
		}
		if err != nil {
			break
		}
	}
	_ = session.cmd.Wait()
	_ = session.master.Close()
	session.finish()
	m.mu.Lock()
	if current, ok := m.sessions[session.id]; ok && current == session {
		delete(m.sessions, session.id)
	}
	m.mu.Unlock()
}

func (s *terminalSession) subscribe() (*terminalSubscription, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return nil, nil, errors.New("terminal process has exited")
	default:
	}
	if s.attached != nil {
		return nil, nil, errors.New("terminal already has an attached client")
	}
	subscription := &terminalSubscription{
		afterSeq: s.nextSeq,
		output:   make(chan []byte, 128),
		done:     make(chan struct{}),
	}
	s.attached = subscription
	return subscription, s.output.Snapshot(), nil
}

func (s *terminalSession) detach(subscription *terminalSubscription) {
	s.mu.Lock()
	if s.attached == subscription {
		s.attached = nil
	}
	s.mu.Unlock()
	subscription.once.Do(func() { close(subscription.done) })
}

func (s *terminalSession) publish(data []byte) {
	chunk := append([]byte(nil), data...)
	s.mu.Lock()
	_, _ = s.output.Write(chunk)
	s.nextSeq++
	sequence := s.nextSeq
	subscription := s.attached
	s.mu.Unlock()
	if subscription != nil && sequence > subscription.afterSeq {
		select {
		case subscription.output <- chunk:
		case <-subscription.done:
		}
	}
}

func (s *terminalSession) finish() {
	s.mu.Lock()
	if s.cmd.ProcessState != nil {
		s.exitCode = s.cmd.ProcessState.ExitCode()
	}
	subscription := s.attached
	close(s.done)
	if subscription != nil {
		close(subscription.output)
	}
	s.mu.Unlock()
}

func (s *terminalSession) processExitCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exitCode
}

func stopTerminal(session *terminalSession) {
	if session.cmd.Process != nil {
		_ = session.cmd.Process.Signal(syscall.SIGHUP)
	}
	_ = session.master.Close()
	select {
	case <-session.done:
	case <-time.After(500 * time.Millisecond):
		if session.cmd.Process != nil {
			_ = session.cmd.Process.Kill()
		}
		<-session.done
	}
}

func validateTerminalOpen(request terminalOpenRequest) error {
	if request.ID == "" || len(request.ID) > 128 {
		return errors.New("terminal id is required and must not exceed 128 characters")
	}
	for _, r := range request.ID {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return errors.New("terminal id contains invalid characters")
		}
	}
	if request.Shell != "/bin/bash" {
		return errors.New("unsupported terminal shell")
	}
	if request.Hostname != "" && !validHostname(request.Hostname) {
		return errors.New("invalid sandbox hostname")
	}
	if request.Columns < 20 || request.Columns > 500 || request.Rows < 5 || request.Rows > 200 {
		return errors.New("terminal size is outside the supported range")
	}
	return nil
}

func validHostname(hostname string) bool {
	if len(hostname) == 0 || len(hostname) > 63 || hostname[0] == '-' || hostname[len(hostname)-1] == '-' {
		return false
	}
	for _, r := range hostname {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-') {
			return false
		}
	}
	return true
}

func mergeEnvironment(base []string, overrides map[string]string) []string {
	values := make(map[string]string, len(base)+len(overrides)+1)
	for _, item := range base {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	for key, value := range overrides {
		if key != "" && !strings.ContainsRune(key, '=') && !strings.ContainsRune(key, '\x00') && !strings.ContainsRune(value, '\x00') {
			values[key] = value
		}
	}
	values["TERM"] = "xterm-256color"
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}

type byteRing struct {
	mu   sync.Mutex
	data []byte
	max  int
}

func newByteRing(max int) *byteRing {
	return &byteRing{max: max, data: make([]byte, 0, max)}
}

func (r *byteRing) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	written := len(p)
	if len(p) >= r.max {
		r.data = append(r.data[:0], p[len(p)-r.max:]...)
		return written, nil
	}
	overflow := len(r.data) + len(p) - r.max
	if overflow > 0 {
		copy(r.data, r.data[overflow:])
		r.data = r.data[:len(r.data)-overflow]
	}
	r.data = append(r.data, p...)
	return written, nil
}

func (r *byteRing) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.data...)
}

type terminalWireFrame struct {
	frameType byte
	payload   []byte
	err       error
}

func readTerminalInputs(reader io.Reader, frames chan<- terminalWireFrame) {
	for {
		frameType, payload, err := readTerminalFrame(reader)
		frames <- terminalWireFrame{frameType: frameType, payload: payload, err: err}
		if err != nil {
			return
		}
	}
}

func writeTerminalFrame(writer io.Writer, frameType byte, payload []byte) error {
	if len(payload) > terminalFrameLimit {
		return errors.New("terminal frame exceeds maximum payload")
	}
	header := [5]byte{frameType}
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	_, err := writer.Write(payload)
	return err
}

func readTerminalFrame(reader io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size > terminalFrameLimit {
		return 0, nil, errors.New("terminal frame exceeds maximum payload")
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}
