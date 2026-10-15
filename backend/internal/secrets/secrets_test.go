package secrets

import "testing"

// Produced by the platform's encryptValues (secrets-crypto.js) with key bytes
// 0..31, so a format change on either side breaks this test.
const (
	jsKey    = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	jsUser   = "7c0d7e5e-1111-4222-8333-944445555666"
	jsSealed = "v1:ghFg01XDjTSqTpyIDoLnHVtTgJvhY8jB6i5B9+DZya+CWg58lQlNJJ22hGrDP0kII+ICwVOaNC6to9L1wyKnT1gcS+5fP8FJ4vPMQLR/yqx7NMcTgZ7OwjwlJQvOrVU7ts95kp2b9BBdsmq4w1TWVgxZ7YxLA9G1d7dbUYXVnxbWMqQwsEGHNQmvvbn4LSHtI2BLAduvBp/ecN9GEFe0sNCt31ki5xhIu7UxcGYrhDEShnCb9c8S"
)

func TestDecryptReadsThePlatformFormat(t *testing.T) {
	key, err := ParseKey(jsKey)
	if err != nil {
		t.Fatal(err)
	}
	values, err := Decrypt(key, jsUser, KindAnthropic, jsSealed)
	if err != nil {
		t.Fatal(err)
	}
	if values["ANTHROPIC_FOUNDRY_API_KEY"] != "test-key" || values["ANTHROPIC_MODEL"] != "claude-opus-5-5" ||
		values["ANTHROPIC_FOUNDRY_BASE_URL"] != "https://example.services.ai.azure.com/anthropic" {
		t.Fatalf("values = %v", values)
	}
}

func TestDecryptRefusesAnotherUserKindOrKey(t *testing.T) {
	key, _ := ParseKey(jsKey)
	if _, err := Decrypt(key, "someone-else", KindAnthropic, jsSealed); err == nil {
		t.Error("decrypted for another user")
	}
	if _, err := Decrypt(key, jsUser, KindOpenAI, jsSealed); err == nil {
		t.Error("decrypted for another kind")
	}
	other := make([]byte, 32)
	if _, err := Decrypt(other, jsUser, KindAnthropic, jsSealed); err == nil {
		t.Error("decrypted with another key")
	}
	if _, err := Decrypt(key, jsUser, KindAnthropic, "v2:abc"); err == nil {
		t.Error("accepted an unknown format")
	}
}

func TestParseKeyRequires32Bytes(t *testing.T) {
	if _, err := ParseKey("c2hvcnQ="); err == nil {
		t.Fatal("accepted a short key")
	}
}
