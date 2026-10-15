package agentsession

import (
	"strings"
	"testing"
)

func TestValidateGateway(t *testing.T) {
	token := strings.Repeat("x", 64)
	for _, ok := range []string{"http://172.16.0.1:7081", "http://172.16.0.1:7081/", "https://inference.internal.example"} {
		if err := validateGateway(ok, token); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"http://public.example", "http://10.0.0.5:8083", "http://172.16.0.1", "http://172.16.0.1:7081/elsewhere",
		"https://user:secret@public.example", "https://public.example?token=secret", "ftp://172.16.0.1:7081", "",
	} {
		if err := validateGateway(bad, token); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if err := validateGateway("http://172.16.0.1:7081", "short"); err == nil {
		t.Error("short token accepted")
	}
}
