package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
)

// The viewer is the user's door. Port 3002 serves the noVNC client and, on
// /vnc, relays a websocket to Xvnc's VNC port. The preview gateway authorizes
// users before requests reach this port.

// noVNCLaunchPath opens the noVNC client connected to /vnc, scaled to the
// user's window and reconnecting after drops (e.g. sandbox pause/resume).
const noVNCLaunchPath = "/vnc.html?autoconnect=true&resize=scale&reconnect=true&path=vnc"

// starter is what the viewer needs from the browser stack: a way to make sure
// it is running before relaying to it.
type starter interface {
	Start(context.Context) error
}

type viewer struct {
	starter      starter
	vncAddr      string
	startTimeout time.Duration
	logger       *slog.Logger
}

func newViewer(browser starter, cfg config, logger *slog.Logger) http.Handler {
	v := &viewer{
		starter:      browser,
		vncAddr:      net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.vncPort)),
		startTimeout: 2 * cfg.readyTimeout,
		logger:       logger.With("door", "viewer"),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vnc", v.serveVNC)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, noVNCLaunchPath, http.StatusFound)
	})
	mux.Handle("GET /", http.FileServer(http.Dir(cfg.noVNCDir)))
	return mux
}

// serveVNC accepts the user's websocket, starts the browser if needed, and
// copies bytes between the websocket and Xvnc until either side closes.
func (v *viewer) serveVNC(w http.ResponseWriter, r *http.Request) {
	logger := v.logger.With("remote_addr", r.RemoteAddr, "forwarded_for", r.Header.Get("X-Forwarded-For"))

	// Accept rejects cross-origin requests: the Origin host must match Host,
	// which the worker preserves as 3002-<sandbox>.<domain>.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
	if err != nil {
		logger.Warn("accepting viewer websocket", "err", err)
		return
	}

	startCtx, cancel := context.WithTimeout(r.Context(), v.startTimeout)
	err = v.starter.Start(startCtx)
	cancel()
	if err != nil {
		logger.Warn("browser not ready for viewer", "err", err)
		closeWebsocket(conn, websocket.StatusTryAgainLater, "browser is not ready", logger)
		return
	}

	var dialer net.Dialer
	display, err := dialer.DialContext(r.Context(), "tcp", v.vncAddr)
	if err != nil {
		logger.Warn("dialing display", "addr", v.vncAddr, "err", err)
		closeWebsocket(conn, websocket.StatusInternalError, "display unavailable", logger)
		return
	}

	logger.Info("viewer connected")
	started := time.Now()
	err = relay(websocket.NetConn(r.Context(), conn, websocket.MessageBinary), display)
	logger.Info("viewer disconnected", "duration", time.Since(started).String(), "err", err)
}

// relay copies in both directions until one side ends, then closes both.
// It returns the error that ended the first copy, if any.
func relay(a, b net.Conn) error {
	errs := make(chan error, 2)
	go func() {
		_, err := io.Copy(a, b)
		errs <- err
	}()
	go func() {
		_, err := io.Copy(b, a)
		errs <- err
	}()
	first := <-errs
	// Closing both ends the other copy. Close errors here are expected (one
	// peer has already gone) and carry no information.
	_ = a.Close()
	_ = b.Close()
	<-errs
	return first
}

func closeWebsocket(conn *websocket.Conn, code websocket.StatusCode, reason string, logger *slog.Logger) {
	if err := conn.Close(code, reason); err != nil {
		logger.Debug("closing viewer websocket", "err", err)
	}
}
