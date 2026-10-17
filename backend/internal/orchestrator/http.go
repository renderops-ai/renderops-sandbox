package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"backend/internal/plane"

	"github.com/google/uuid"
)

const (
	AuthHeader      = "X-Orchestrator-Token"
	RequestIDHeader = "X-Request-ID"
)

type requestIDContextKey struct{}

type HTTPServer struct {
	service      *Service
	controlToken string
	workerToken  string
}

func NewHTTPServer(service *Service, controlToken, workerToken string) *HTTPServer {
	return &HTTPServer{
		service:      service,
		controlToken: controlToken,
		workerToken:  workerToken,
	}
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("PUT /internal/workers/{workerID}", s.authed(s.workerToken, s.registerWorker))
	mux.HandleFunc("POST /internal/workers/{workerID}/heartbeat", s.authed(s.workerToken, s.heartbeat))
	mux.HandleFunc("POST /internal/workers/{workerID}/draining", s.authed(s.workerToken, s.setWorkerDraining))
	mux.HandleFunc("POST /internal/workers/{workerID}/sandboxes/{sandboxID}/state", s.authed(s.workerToken, s.workerState))
	mux.HandleFunc("POST /internal/workers/{workerID}/inventory", s.authed(s.workerToken, s.workerInventory))
	mux.HandleFunc("POST /internal/placements", s.authed(s.controlToken, s.place))
	mux.HandleFunc("GET /internal/placements/{sandboxID}", s.authed(s.controlToken, s.getPlacement))
	mux.HandleFunc("DELETE /internal/placements/{sandboxID}", s.authed(s.controlToken, s.releasePlacement))
	mux.HandleFunc("POST /internal/sandboxes", s.authed(s.controlToken, s.provision))
	mux.HandleFunc("POST /internal/sandboxes/{sandboxID}/pause", s.authed(s.controlToken, s.pause))
	mux.HandleFunc("POST /internal/sandboxes/{sandboxID}/resume", s.authed(s.controlToken, s.resume))
	mux.HandleFunc("DELETE /internal/sandboxes/{sandboxID}", s.authed(s.controlToken, s.destroy))
	return withRequestID(mux)
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get(RequestIDHeader))
		if requestID == "" || len(requestID) > 128 {
			requestID = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func (s *HTTPServer) authed(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token == "" || r.Header.Get(AuthHeader) != token {
			writeHTTPError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing orchestrator token")
			return
		}
		next(w, r)
	}
}

func (s *HTTPServer) health(w http.ResponseWriter, _ *http.Request) {
	writeHTTPJSON(w, http.StatusOK, map[string]string{"status": "ok", "role": "orchestrator"})
}

func (s *HTTPServer) registerWorker(w http.ResponseWriter, r *http.Request) {
	var registration WorkerRegistration
	if err := decodeHTTPJSON(w, r, &registration); err != nil {
		return
	}
	pathID := r.PathValue("workerID")
	if registration.ID != "" && registration.ID != pathID {
		writeHTTPError(w, http.StatusBadRequest, "worker_id_mismatch", "body worker id does not match request path")
		return
	}
	registration.ID = pathID
	if err := s.service.RegisterWorker(r.Context(), registration); err != nil {
		writeServiceError(w, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, map[string]string{"status": "registered", "worker_id": registration.ID})
}

func (s *HTTPServer) heartbeat(w http.ResponseWriter, r *http.Request) {
	var capacity plane.Capacity
	if err := decodeHTTPJSON(w, r, &capacity); err != nil {
		return
	}
	if err := s.service.Heartbeat(r.Context(), r.PathValue("workerID"), capacity); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) setWorkerDraining(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Draining bool `json:"draining"`
	}
	if err := decodeHTTPJSON(w, r, &request); err != nil {
		return
	}
	if err := s.service.SetWorkerDraining(
		r.Context(),
		r.PathValue("workerID"),
		request.Draining,
	); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) workerState(w http.ResponseWriter, r *http.Request) {
	var request struct {
		State  string `json:"state"`
		Reason string `json:"reason,omitempty"` // why a "lost" sandbox was lost
	}
	if err := decodeHTTPJSON(w, r, &request); err != nil {
		return
	}
	var err error
	if strings.TrimSpace(request.State) == "lost" {
		err = s.service.ReportWorkerLost(r.Context(), r.PathValue("workerID"), r.PathValue("sandboxID"), request.Reason)
	} else {
		err = s.service.ReportWorkerState(
			r.Context(),
			r.PathValue("workerID"),
			r.PathValue("sandboxID"),
			request.State,
		)
	}
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// workerInventory reconciles the sandboxes placed on a worker with the ones it
// holds (see Service.ReconcileWorker).
func (s *HTTPServer) workerInventory(w http.ResponseWriter, r *http.Request) {
	var inventory WorkerInventory
	if err := decodeHTTPJSON(w, r, &inventory); err != nil {
		return
	}
	result, err := s.service.ReconcileWorker(r.Context(), r.PathValue("workerID"), inventory)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, result)
}

func (s *HTTPServer) place(w http.ResponseWriter, r *http.Request) {
	var request struct {
		SandboxID string `json:"sandbox_id"`
		PlacementRequest
	}
	if err := decodeHTTPJSON(w, r, &request); err != nil {
		return
	}
	placement, err := s.service.Place(r.Context(), request.SandboxID, request.PlacementRequest)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, placement)
}

func (s *HTTPServer) getPlacement(w http.ResponseWriter, r *http.Request) {
	placement, ok, err := s.service.Placement(r.Context(), r.PathValue("sandboxID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if !ok {
		writeHTTPError(w, http.StatusNotFound, "placement_not_found", "sandbox has no placement")
		return
	}
	writeHTTPJSON(w, http.StatusOK, placement)
}

func (s *HTTPServer) releasePlacement(w http.ResponseWriter, r *http.Request) {
	if err := s.service.Release(r.Context(), r.PathValue("sandboxID")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) provision(w http.ResponseWriter, r *http.Request) {
	var request ProvisionRequest
	if err := decodeHTTPJSON(w, r, &request); err != nil {
		return
	}
	placement, err := s.service.Provision(r.Context(), request)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeHTTPJSON(w, http.StatusCreated, placement)
}

func (s *HTTPServer) pause(w http.ResponseWriter, r *http.Request) {
	if err := s.service.Pause(r.Context(), r.PathValue("sandboxID")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) resume(w http.ResponseWriter, r *http.Request) {
	if err := s.service.Resume(r.Context(), r.PathValue("sandboxID")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) destroy(w http.ResponseWriter, r *http.Request) {
	if err := s.service.Destroy(r.Context(), r.PathValue("sandboxID")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeHTTPJSON(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return err
	}
	return nil
}

func writeServiceError(w http.ResponseWriter, err error) {
	var lost *plane.LostError
	if errors.As(err, &lost) {
		writeHTTPJSON(w, http.StatusGone, map[string]string{
			"code": "sandbox_lost", "error": err.Error(), "reason": lost.Reason,
		})
		return
	}
	switch {
	case errors.Is(err, ErrNoCapacity):
		writeHTTPError(w, http.StatusServiceUnavailable, "no_capacity", err.Error())
	case errors.Is(err, ErrPlacementBusy):
		writeHTTPError(w, http.StatusServiceUnavailable, "scheduler_busy", err.Error())
	case errors.Is(err, ErrWorkerNotFound):
		writeHTTPError(w, http.StatusNotFound, "worker_not_found", err.Error())
	case errors.Is(err, ErrSandboxNotFound):
		writeHTTPError(w, http.StatusNotFound, "sandbox_not_found", err.Error())
	case errors.Is(err, ErrInvalidState):
		writeHTTPError(w, http.StatusConflict, "invalid_sandbox_state", err.Error())
	default:
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "invalid") ||
			strings.Contains(err.Error(), "required") ||
			strings.Contains(err.Error(), "must be") {
			status = http.StatusBadRequest
		}
		writeHTTPError(w, status, "orchestration_failed", err.Error())
	}
}

func writeHTTPError(w http.ResponseWriter, status int, code, message string) {
	writeHTTPJSON(w, status, map[string]string{"code": code, "error": message})
}

func writeHTTPJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
