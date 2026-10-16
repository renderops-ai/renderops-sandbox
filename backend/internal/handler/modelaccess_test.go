package handler

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"backend/internal/modelaccess"
	"backend/internal/orchestrator"
)

// The grant core moved to internal/modelaccess; the public endpoint must keep
// answering with the same status codes and error codes as before.
func TestModelAccessErrorKeepsTheAPIContract(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{modelaccess.ErrUnavailable, http.StatusServiceUnavailable, "model_access_unavailable"},
		{modelaccess.ErrProviderNotReady, http.StatusBadRequest, "provider_unavailable"},
		{modelaccess.ErrInvalidProvider, http.StatusBadRequest, "invalid_provider"},
		{modelaccess.ErrSandboxNotFound, http.StatusNotFound, "sandbox_not_found"},
		{modelaccess.ErrSandboxNotActive, http.StatusConflict, "sandbox_not_active"},
		{modelaccess.ErrCredentialsMissing, http.StatusConflict, "credentials_missing"},
		{fmt.Errorf("%w: %w", modelaccess.ErrNotPlaced, orchestrator.ErrSandboxNotFound), http.StatusConflict, "sandbox_not_placed"},
		{fmt.Errorf("%w: %w", modelaccess.ErrPlacement, errors.New("down")), http.StatusServiceUnavailable, "placement_unavailable"},
		{fmt.Errorf("%w: %w", modelaccess.ErrWorker, errors.New("boom")), http.StatusBadGateway, "model_access_failed"},
		{errors.New("unexpected"), http.StatusInternalServerError, "model_access_failed"},
	}
	for _, c := range cases {
		status, code, message := modelAccessError(c.err)
		if status != c.status || code != c.code || message == "" {
			t.Errorf("%v: got %d %q %q, want %d %q", c.err, status, code, message, c.status, c.code)
		}
	}
}
