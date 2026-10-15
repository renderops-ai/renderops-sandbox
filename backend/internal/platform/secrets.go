package platform

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ProviderSecret returns the encrypted credentials userID saved for a model
// provider on the Secrets page (decrypt with internal/secrets). found is false
// when there are none.
func (c *Client) ProviderSecret(ctx context.Context, userID, kind string) (ciphertext string, found bool, err error) {
	err = c.pool.QueryRow(ctx, `SELECT ciphertext FROM secrets WHERE user_id::text=$1 AND kind=$2`, userID, kind).Scan(&ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load provider secret: %w", err)
	}
	return ciphertext, true, nil
}
