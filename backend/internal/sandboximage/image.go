package sandboximage

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const Default = "alpine"

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// Normalize returns the stable image identifier carried across all planes.
// An omitted image preserves the existing Alpine behavior.
func Normalize(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return Default, nil
	}
	if !idPattern.MatchString(value) {
		return "", fmt.Errorf("invalid image %q", value)
	}
	return value, nil
}

func PoolKey(image, sizeKey string) string {
	return image + ":" + sizeKey
}

// sizesByImage lists the size names each known image may run at. The standard
// Alpine image keeps the original menu; the agents image (Claude Code, Codex)
// needs 4 GB, so it only runs at "large".
var sizesByImage = map[string][]string{
	Default:  {"nano", "small", "medium"},
	"agents": {"large"},
}

// Sizes returns the size names image may run at, or nil when the image has no
// restriction (worker routing then decides whether it can run).
func Sizes(image string) []string { return sizesByImage[image] }

// SizeAllowed reports whether a sandbox of image may run at the named size.
func SizeAllowed(image, size string) bool {
	allowed, known := sizesByImage[image]
	return !known || slices.Contains(allowed, size)
}
