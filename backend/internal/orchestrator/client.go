package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"backend/internal/plane"
)

type Client struct {
	base      string
	token     string
	http      *http.Client
	lifecycle *http.Client // pause and resume: the worker may need minutes
}

func NewClient(base, token string) *Client {
	return &Client{
		base:  strings.TrimRight(strings.TrimSpace(base), "/"),
		token: strings.TrimSpace(token),
		http:  &http.Client{Timeout: 90 * time.Second},
		// Longer than the orchestrator's own pause/resume timeout towards the
		// worker (agent.LifecycleTimeout, 10m), so the worker's answer or error
		// gets back to the caller instead of a timeout here.
		lifecycle: &http.Client{Timeout: 11 * time.Minute},
	}
}

func (c *Client) Provision(ctx context.Context, request ProvisionRequest) (Placement, error) {
	var placement Placement
	err := c.do(ctx, http.MethodPost, "/internal/sandboxes", request, &placement)
	return placement, err
}

func (c *Client) Placement(ctx context.Context, sandboxID string) (Placement, error) {
	var placement Placement
	err := c.do(ctx, http.MethodGet, "/internal/placements/"+sandboxID, nil, &placement)
	return placement, err
}

func (c *Client) Pause(ctx context.Context, sandboxID string) error {
	return c.doWith(ctx, c.lifecycle, http.MethodPost, "/internal/sandboxes/"+sandboxID+"/pause", nil, nil)
}

func (c *Client) Resume(ctx context.Context, sandboxID string) error {
	return c.doWith(ctx, c.lifecycle, http.MethodPost, "/internal/sandboxes/"+sandboxID+"/resume", nil, nil)
}

func (c *Client) Destroy(ctx context.Context, sandboxID string) error {
	return c.do(ctx, http.MethodDelete, "/internal/sandboxes/"+sandboxID, nil, nil)
}

func (c *Client) RegisterWorker(ctx context.Context, registration WorkerRegistration) error {
	return c.do(ctx, http.MethodPut, "/internal/workers/"+registration.ID, registration, nil)
}

func (c *Client) Heartbeat(ctx context.Context, workerID string, capacity plane.Capacity) error {
	return c.do(ctx, http.MethodPost, "/internal/workers/"+workerID+"/heartbeat", capacity, nil)
}

func (c *Client) SetWorkerDraining(ctx context.Context, workerID string, draining bool) error {
	return c.do(
		ctx,
		http.MethodPost,
		"/internal/workers/"+workerID+"/draining",
		map[string]bool{"draining": draining},
		nil,
	)
}

func (c *Client) ReportWorkerState(ctx context.Context, workerID, sandboxID, state string) error {
	return c.do(
		ctx,
		http.MethodPost,
		"/internal/workers/"+workerID+"/sandboxes/"+sandboxID+"/state",
		map[string]string{"state": state},
		nil,
	)
}

func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/health", nil, nil)
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	return c.doWith(ctx, c.http, method, path, body, out)
}

func (c *Client) doWith(ctx context.Context, client *http.Client, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal orchestrator request: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set(AuthHeader, c.token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("orchestrator unreachable: %w", err)
	}
	defer response.Body.Close()

	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var apiError map[string]string
		if json.Unmarshal(raw, &apiError) == nil && apiError["error"] != "" {
			switch apiError["code"] {
			case "no_capacity":
				return fmt.Errorf("%w: %s", ErrNoCapacity, apiError["error"])
			case "scheduler_busy":
				return fmt.Errorf("%w: %s", ErrPlacementBusy, apiError["error"])
			case "sandbox_not_found":
				return fmt.Errorf("%w: %s", ErrSandboxNotFound, apiError["error"])
			case "worker_not_found":
				return fmt.Errorf("%w: %s", ErrWorkerNotFound, apiError["error"])
			case "invalid_state", "invalid_sandbox_state":
				return fmt.Errorf("%w: %s", ErrInvalidState, apiError["error"])
			default:
				return fmt.Errorf("orchestrator %s: %s", apiError["code"], apiError["error"])
			}
		}
		return fmt.Errorf("orchestrator returned %d: %s", response.StatusCode, string(raw))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode orchestrator response: %w", err)
		}
	}
	return nil
}
