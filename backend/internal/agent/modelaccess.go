package agent

import (
	"context"

	"backend/internal/plane"
)

// ModelAccessClient reaches whichever worker a sandbox is placed on.
type ModelAccessClient struct {
	workerToken string
}

func NewModelAccessClient(workerToken string) *ModelAccessClient {
	return &ModelAccessClient{workerToken: workerToken}
}

func (c *ModelAccessClient) GrantModelAccess(ctx context.Context, endpoint, sandboxID string, req plane.ModelAccessRequest) (plane.ModelAccessResponse, error) {
	return NewClient(endpoint, c.workerToken).GrantModelAccess(ctx, sandboxID, req)
}

func (c *ModelAccessClient) RevokeModelAccess(ctx context.Context, endpoint, sandboxID string) error {
	return NewClient(endpoint, c.workerToken).RevokeModelAccess(ctx, sandboxID)
}
