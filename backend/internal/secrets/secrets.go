// Package secrets decrypts the model provider credentials users save on the
// dashboard's Secrets page. The platform encrypts them (renderops-platform
// src/lib/server/secrets-crypto.js); keep the two formats in step:
//
//	ciphertext = "v1:" + base64(nonce[12] || AES-256-GCM(JSON values) || tag[16])
//	key        = SECRETS_ENCRYPTION_KEY, 32 bytes, base64
//	AAD        = "secrets:<user id>:<kind>"
//
// The AAD binds a ciphertext to its owner and provider, so a row copied to
// another user or kind fails to decrypt.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const version = "v1:"

// Kinds the platform stores.
const (
	KindAnthropic = "anthropic"
	KindOpenAI    = "openai"
)

// ParseKey decodes SECRETS_ENCRYPTION_KEY.
func ParseKey(encoded string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return nil, fmt.Errorf("decode secrets key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("secrets key must be 32 bytes, got %d", len(key))
	}
	return key, nil
}

// Decrypt returns the variables of userID's kind secret.
func Decrypt(key []byte, userID, kind, ciphertext string) (map[string]string, error) {
	if !strings.HasPrefix(ciphertext, version) {
		return nil, errors.New("unknown secret format")
	}
	sealed, err := base64.StdEncoding.DecodeString(ciphertext[len(version):])
	if err != nil {
		return nil, fmt.Errorf("decode secret: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secrets cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secrets cipher: %w", err)
	}
	if len(sealed) < gcm.NonceSize()+gcm.Overhead() {
		return nil, errors.New("secret is too short")
	}
	nonce, body := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, body, []byte("secrets:"+userID+":"+kind))
	if err != nil {
		return nil, errors.New("secret does not decrypt for this user and kind")
	}
	values := map[string]string{}
	if err := json.Unmarshal(plain, &values); err != nil {
		return nil, fmt.Errorf("decode secret values: %w", err)
	}
	return values, nil
}
