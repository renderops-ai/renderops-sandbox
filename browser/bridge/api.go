package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// controller is what the internal API needs from the browser stack.
type controller interface {
	Start(context.Context) error
	Stop(context.Context) error
	Status() status
}

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Status  status `json:"status"`
}

// newAPI serves the internal control API:
//
//	GET  /v1/status  current state of the display and Chromium
//	POST /v1/start   start both if needed and wait until ready
//	POST /v1/stop    stop both
func newAPI(ctrl controller, startTimeout time.Duration, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, logger, http.StatusOK, ctrl.Status())
	})
	mux.HandleFunc("POST /v1/start", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), startTimeout)
		defer cancel()
		if err := ctrl.Start(ctx); err != nil {
			logger.Warn("starting browser stack", "err", err)
			writeJSON(w, logger, http.StatusServiceUnavailable, errorBody{
				Error: "start_failed", Message: err.Error(), Status: ctrl.Status(),
			})
			return
		}
		writeJSON(w, logger, http.StatusOK, ctrl.Status())
	})
	mux.HandleFunc("POST /v1/stop", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*stopGrace)
		defer cancel()
		if err := ctrl.Stop(ctx); err != nil {
			logger.Warn("stopping browser stack", "err", err)
			writeJSON(w, logger, http.StatusServiceUnavailable, errorBody{
				Error: "stop_failed", Message: err.Error(), Status: ctrl.Status(),
			})
			return
		}
		writeJSON(w, logger, http.StatusOK, ctrl.Status())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.Warn("writing api response", "err", err)
	}
}
