package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"backend/internal/plane"
	"backend/internal/session"
)

// Server serves the worker HTTP API backed by a session.Service (the execution
// engine). The control plane calls it; users never reach it directly.
type Server struct {
	svc       session.Service
	token     string
	maxSlots  int
	admission *Admission
	draining  atomic.Bool
	dialer    namespaceDialer
	git       gitAccessProxy // nil until SetGitProxy
}

// NewServer builds a worker HTTP server. token is the internal shared secret the
// control plane must present (empty disables the check — dev only).
func NewServer(svc session.Service, token string, maxSlots int) *Server {
	return &Server{svc: svc, token: token, maxSlots: maxSlots, dialer: systemNamespaceDialer{}}
}

func NewServerWithAdmission(svc session.Service, token string, admission *Admission) *Server {
	return &Server{
		svc: svc, token: token, maxSlots: admission.Capacity().MaxSlots, admission: admission,
		dialer: systemNamespaceDialer{},
	}
}

// BeginDrain immediately rejects new sandbox creates while allowing lifecycle
// operations for existing sandboxes to finish during graceful shutdown.
func (s *Server) BeginDrain() {
	s.draining.Store(true)
}

// Handler returns the worker API mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(plane.RouteHealth, s.health)
	mux.HandleFunc(plane.RouteCapacity, s.authed(s.capacity))
	mux.HandleFunc(plane.RouteSandbox, s.authed(s.create))          // POST
	mux.HandleFunc(plane.RouteSandboxPrefix, s.authed(s.sandboxOp)) // /{id}/run|exec|pause|resume, DELETE /{id}
	return mux
}

// authed enforces the internal shared token before dispatching.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" && r.Header.Get(plane.AuthHeader) != s.token {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "invalid or missing worker token")
			return
		}
		next(w, r)
	}
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	status := "ok"
	if s.draining.Load() {
		status = "draining"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status, "role": "worker"})
}

func (s *Server) capacity(w http.ResponseWriter, _ *http.Request) {
	if s.admission != nil {
		capacity := s.admission.Capacity()
		if s.draining.Load() {
			capacity.FreeSlots = 0
		}
		writeJSON(w, http.StatusOK, capacity)
		return
	}
	if s.draining.Load() {
		writeJSON(w, http.StatusOK, plane.Capacity{FreeSlots: 0, MaxSlots: s.maxSlots})
		return
	}
	used := s.svc.Stats()["total_sessions"]
	free := s.maxSlots - used
	if free < 0 {
		free = 0
	}
	writeJSON(w, http.StatusOK, plane.Capacity{FreeSlots: free, MaxSlots: s.maxSlots})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	if s.draining.Load() {
		writeErr(w, http.StatusServiceUnavailable, "worker_draining", "worker is draining")
		return
	}
	var req plane.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.SandboxID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "sandbox_id is required")
		return
	}
	if s.admission != nil {
		if err := s.admission.ReserveCreate(req); err != nil {
			if errors.Is(err, plane.ErrNoCapacity) {
				writeErr(w, http.StatusTooManyRequests, "no_capacity", err.Error())
			} else {
				writeErr(w, http.StatusConflict, "reservation_conflict", err.Error())
			}
			return
		}
	}
	sess, err := s.svc.CreateWithID(r.Context(), req.SandboxID, req.UserID, req.BillingModel, req.Image,
		req.Env,
		req.VCPUs, req.MemoryMB, req.DiskGB, req.Internet,
		secs(req.IdleTimeoutS), secs(req.MaxLifetimeS))
	if err != nil {
		if s.admission != nil {
			s.admission.Release(req.SandboxID)
		}
		writeErr(w, http.StatusInternalServerError, "create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, plane.CreateResponse{
		SandboxID: sess.ID, State: string(sess.State),
		Image: sess.Image,
		VCPUs: sess.VCPUs, MemoryMB: sess.MemoryMB, DiskGB: sess.DiskGB,
	})
}

// sandboxOp routes /worker/sandbox/{id}[/op].
func (s *Server) sandboxOp(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, plane.RouteSandboxPrefix)
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	op := ""
	if len(parts) == 2 {
		op = parts[1]
	}
	if id == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "missing sandbox id")
		return
	}

	switch {
	case op == "run" && r.Method == http.MethodPost:
		var req plane.RunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		res, err := s.svc.Execute(r.Context(), id, req.Code, req.Language, req.TimeoutS)
		s.writeResult(w, res, err)

	case op == "exec" && r.Method == http.MethodPost:
		var req plane.ExecRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		res, err := s.svc.Exec(r.Context(), id, req.Command, req.TimeoutS)
		s.writeResult(w, res, err)

	case strings.HasPrefix(op, "ports/"):
		s.portProxy(w, r, id, strings.TrimPrefix(op, "ports/"))

	case op == "pause" && r.Method == http.MethodPost:
		// Finish even if the caller disconnects: a pause cancelled halfway would
		// leave the sandbox neither running nor checkpointed.
		err := s.svc.Pause(context.WithoutCancel(r.Context()), id)
		if err == nil && s.admission != nil {
			s.admission.MarkPaused(id)
		}
		s.writeStatus(w, err)

	case op == "resume" && r.Method == http.MethodPost:
		if s.admission != nil {
			if err := s.admission.ReserveResume(id); err != nil {
				if errors.Is(err, plane.ErrNoCapacity) {
					writeErr(w, http.StatusTooManyRequests, "no_capacity", err.Error())
				} else {
					writeErr(w, http.StatusConflict, "reservation_conflict", err.Error())
				}
				return
			}
		}
		// Like pause: a resume cancelled halfway leaves a half-restored VM.
		err := s.svc.Resume(context.WithoutCancel(r.Context()), id)
		if err != nil && s.admission != nil {
			s.admission.MarkPaused(id)
		}
		s.writeStatus(w, err)

	case op == "git-access" && r.Method == http.MethodPut:
		s.grantGitAccess(w, r, id)

	case op == "git-access" && r.Method == http.MethodDelete:
		s.revokeGitAccess(w, id)

	case op == "" && r.Method == http.MethodDelete:
		if err := s.svc.Destroy(r.Context(), id); err != nil {
			if s.admission != nil && strings.Contains(strings.ToLower(err.Error()), "not found") {
				s.admission.Release(id)
			}
			s.writeStatus(w, err)
			return
		}
		if s.admission != nil {
			s.admission.Release(id)
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		writeErr(w, http.StatusNotFound, "not_found", "unknown worker operation")
	}
}

type portTargetResolver interface {
	ResolvePortTarget(string) (int, error)
}

func (s *Server) portProxy(w http.ResponseWriter, r *http.Request, sandboxID, rest string) {
	portText, upstreamPath, _ := strings.Cut(rest, "/")
	parsedPort, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || parsedPort == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_preview_port", "port must be between 1 and 65535")
		return
	}
	resolver, ok := s.svc.(portTargetResolver)
	if !ok {
		writeErr(w, http.StatusNotImplemented, "preview_unavailable", "worker preview proxy is unavailable")
		return
	}
	slot, err := resolver.ResolvePortTarget(sandboxID)
	if err != nil {
		writeErr(w, http.StatusNotFound, "sandbox_not_found", "sandbox is not active")
		return
	}
	if upstreamPath == "" {
		upstreamPath = "/"
	} else {
		upstreamPath = "/" + upstreamPath
	}
	address := net.JoinHostPort("172.16.0.2", portText)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return s.dialer.DialContext(ctx, slot, address)
	}
	originalHost := r.Header.Get("X-Forwarded-Host")
	proxy := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL = &url.URL{
				Scheme: "http", Host: address, Path: upstreamPath,
				RawQuery: request.In.URL.RawQuery,
			}
			request.Out.Host = originalHost
			request.Out.Header.Del(plane.AuthHeader)
			request.SetXForwarded()
			request.Out.Header.Set("X-Forwarded-Host", originalHost)
			request.Out.Header.Set("X-Forwarded-Proto", "https")
		},
		Transport: transport, FlushInterval: -1,
		ErrorHandler: func(response http.ResponseWriter, _ *http.Request, proxyErr error) {
			writeErr(response, http.StatusBadGateway, "preview_upstream_unavailable", fmt.Sprintf("sandbox port %d is unavailable", parsedPort))
		},
	}
	proxy.ServeHTTP(w, r)
}

func (s *Server) writeResult(w http.ResponseWriter, res any, err error) {
	if err != nil {
		s.writeStatus(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// writeStatus maps a lifecycle error to an HTTP status (404 for missing session).
func (s *Server) writeStatus(w http.ResponseWriter, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	code := http.StatusInternalServerError
	if strings.Contains(err.Error(), "not found") {
		code = http.StatusNotFound
	}
	writeErr(w, code, "operation_failed", err.Error())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, plane.ErrorResponse{Code: code, Error: msg})
}

func secs(n int) time.Duration { return time.Duration(n) * time.Second }
