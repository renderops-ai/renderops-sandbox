// Package agent is the control plane's client for a host agent: it speaks the
// internal/plane HTTP contract over an SSH tunnel and presents the shared
// X-Worker-Token on every call.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"backend/internal/plane"
)

// LifecycleTimeout bounds pause and resume. They snapshot the VM and move its
// memory and disk to or from Blob, which takes minutes for large sandboxes.
const LifecycleTimeout = 10 * time.Minute

// ExecMargin is added to an exec's own timeout for the HTTP call that carries
// it. The worker waits for the guest up to the command timeout plus 10s, so the
// worker's answer (or its timeout error) gets back instead of a timeout here.
const ExecMargin = 30 * time.Second

type Client struct {
	base      string // e.g. http://127.0.0.1:19876
	token     string
	http      *http.Client
	lifecycle *http.Client // pause and resume
}

func NewClient(base, token string) *Client {
	return &Client{
		base:      base,
		token:     token,
		http:      &http.Client{Timeout: 60 * time.Second},
		lifecycle: &http.Client{Timeout: LifecycleTimeout},
	}
}

// Create boots a sandbox on the agent and returns its assigned id + shape.
func (c *Client) Create(ctx context.Context, req plane.CreateRequest) (plane.CreateResponse, error) {
	var out plane.CreateResponse
	err := c.do(ctx, http.MethodPost, plane.RouteSandbox, req, &out)
	return out, err
}

func (c *Client) Run(ctx context.Context, id string, req plane.RunRequest) (plane.ExecResult, error) {
	var out plane.ExecResult
	err := c.do(ctx, http.MethodPost, plane.RouteSandboxPrefix+id+"/run", req, &out)
	return out, err
}

// Exec runs a shell command. The call is allowed the command's own timeout plus
// ExecMargin, so long commands (agent runs) are not cut off by the default
// client timeout.
func (c *Client) Exec(ctx context.Context, id string, req plane.ExecRequest) (plane.ExecResult, error) {
	var out plane.ExecResult
	err := c.doWith(ctx, c.execClient(req.TimeoutS), http.MethodPost, plane.RouteSandboxPrefix+id+"/exec", req, &out)
	return out, err
}

// execClient returns the HTTP client for an exec that may run for timeoutS
// seconds. Without a timeout the worker uses its short default, which the
// regular client already covers.
func (c *Client) execClient(timeoutS int) *http.Client {
	limit := time.Duration(timeoutS)*time.Second + ExecMargin
	if limit <= c.http.Timeout {
		return c.http
	}
	return &http.Client{Transport: c.http.Transport, Timeout: limit}
}

func (c *Client) Pause(ctx context.Context, id string) error {
	return c.doWith(ctx, c.lifecycle, http.MethodPost, plane.RouteSandboxPrefix+id+"/pause", nil, nil)
}

func (c *Client) Resume(ctx context.Context, id string) error {
	return c.doWith(ctx, c.lifecycle, http.MethodPost, plane.RouteSandboxPrefix+id+"/resume", nil, nil)
}

func (c *Client) Destroy(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, plane.RouteSandboxPrefix+id, nil, nil)
}

func (c *Client) StartIDE(ctx context.Context, id string) (plane.IDEInstance, error) {
	var out plane.IDEInstance
	err := c.do(ctx, http.MethodPost, plane.RouteSandboxPrefix+id+"/ide", nil, &out)
	return out, err
}

// GrantGitAccess gives the sandbox read-only fetch access to one repository
// through the worker's git proxy. The token stays on the worker.
func (c *Client) GrantGitAccess(ctx context.Context, id string, req plane.GitAccessRequest) (plane.GitAccessResponse, error) {
	var out plane.GitAccessResponse
	err := c.do(ctx, http.MethodPut, plane.RouteSandboxPrefix+id+"/git-access", req, &out)
	return out, err
}

// RevokeGitAccess drops the sandbox's git grant. It is idempotent.
func (c *Client) RevokeGitAccess(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, plane.RouteSandboxPrefix+id+"/git-access", nil, nil)
}

// GrantModelAccess lets the sandbox call one model through the worker's model
// proxy. The proxy token is not a provider key; the key stays on the control plane.
func (c *Client) GrantModelAccess(ctx context.Context, id string, req plane.ModelAccessRequest) (plane.ModelAccessResponse, error) {
	var out plane.ModelAccessResponse
	err := c.do(ctx, http.MethodPut, plane.RouteSandboxPrefix+id+"/model-access", req, &out)
	return out, err
}

// RevokeModelAccess drops the sandbox's model grant. It is idempotent.
func (c *Client) RevokeModelAccess(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, plane.RouteSandboxPrefix+id+"/model-access", nil, nil)
}

// Health reports whether the agent answers (used to wait for the tunnel).
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, plane.RouteHealth, nil, nil)
}

// do sends one request, presenting the worker token, and decodes a 2xx JSON
// body into out (out may be nil). Non-2xx becomes an error carrying the agent's
// structured message when present.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	return c.doWith(ctx, c.http, method, path, body, out)
}

func (c *Client) doWith(ctx context.Context, client *http.Client, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set(plane.AuthHeader, c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("agent unreachable: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e plane.ErrorResponse
		if json.Unmarshal(raw, &e) == nil && e.Error != "" {
			switch e.Code {
			case "no_capacity":
				return fmt.Errorf("%w: %s", plane.ErrNoCapacity, e.Error)
			case "sandbox_not_active":
				return fmt.Errorf("%w: %s", plane.ErrSandboxNotActive, e.Error)
			}
			return fmt.Errorf("agent %s: %s", e.Code, e.Error)
		}
		return fmt.Errorf("agent returned %d: %s", resp.StatusCode, string(raw))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode agent response: %w", err)
		}
	}
	return nil
}
