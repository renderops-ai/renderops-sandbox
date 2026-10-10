package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Xvnc (TigerVNC) provides the virtual X display Chromium draws on and serves
// that display over VNC. It accepts VNC connections on loopback only: users
// reach it through the bridge, which sits behind the preview gateway.

func (s *stack) startDisplay(stop <-chan struct{}) (*process, error) {
	return s.launch(stop, &s.display, procSpec{
		name:  "xvnc",
		path:  s.cfg.xvncPath,
		args:  xvncArgs(s.cfg),
		ready: displayReady(s.cfg),
	})
}

func xvncArgs(cfg config) []string {
	return []string{
		cfg.display,
		"-geometry", fmt.Sprintf("%dx%d", cfg.width, cfg.height),
		"-depth", "24",
		"-rfbport", strconv.Itoa(cfg.vncPort),
		"-localhost",
		// No VNC password: the port is loopback-only and user access is
		// authorized by the preview gateway before it reaches the bridge.
		"-SecurityTypes", "None",
		// Viewers share one desktop instead of disconnecting each other.
		"-AlwaysShared",
		"-nolisten", "tcp",
	}
}

// displayReady reports ready once the X socket exists and the VNC port accepts
// connections.
func displayReady(cfg config) func(context.Context) error {
	socket := "/tmp/.X11-unix/X" + strings.TrimPrefix(cfg.display, ":")
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.vncPort))
	return func(ctx context.Context) error {
		if _, err := os.Stat(socket); err != nil {
			return fmt.Errorf("checking X socket: %w", err)
		}
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("dialing vnc port: %w", err)
		}
		if err := conn.Close(); err != nil {
			return fmt.Errorf("closing vnc probe: %w", err)
		}
		return nil
	}
}
