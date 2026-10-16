package agentsession

import (
	"errors"
	"net/url"
	"strings"
)

// guestGateway is the guest's gateway address where worker proxies listen
// (slotnet.GatewayAddr).
const guestGateway = "172.16.0.1"

// validateGateway checks a model-access grant before its env is written into
// the guest: an HTTPS gateway, or the worker's in-sandbox model proxy on the
// guest gateway (http://172.16.0.1:<port>, internal/modelproxy). The proxy is
// reached inside the sandbox's own network namespace and only ever sees a
// per-grant proxy token, so that plain HTTP never leaves the sandbox. Any
// other plain-HTTP endpoint is refused.
func validateGateway(raw, token string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !allowedGatewayScheme(u) {
		return errors.New("an HTTPS inference gateway or the in-sandbox model proxy, without embedded credentials, is required")
	}
	if len(token) < 32 || strings.ContainsAny(token, "\x00\r\n") {
		return errors.New("session-scoped inference gateway token required")
	}
	return nil
}

func allowedGatewayScheme(u *url.URL) bool {
	switch u.Scheme {
	case "https":
		return true
	case "http":
		return u.Hostname() == guestGateway && u.Port() != "" && (u.Path == "" || u.Path == "/")
	}
	return false
}
