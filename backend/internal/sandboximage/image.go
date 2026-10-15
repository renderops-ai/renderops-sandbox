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

// The public model is three choices: an OS, a template built on that OS, and a
// size. Each (OS, template) pair is one image that workers boot; the image id
// stays the internal routing key between control plane, orchestrator and workers.

const (
	DefaultOS       = "alpine"
	DefaultTemplate = "base"
)

// Template is software installed on top of an OS.
type Template struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Sizes       []string `json:"sizes"`
	// Image is the rootfs image the template boots, as stored on a sandbox; clients
	// use it to label existing sandboxes with their OS and template.
	Image string `json:"image"`
}

// OS is a base operating system and the templates available on it.
type OS struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Templates []Template `json:"templates"`
}

var catalog = []OS{{
	ID:   "alpine",
	Name: "Alpine Linux 3.22",
	Templates: []Template{
		{ID: "base", Name: "Base", Description: "Python 3.12, Node 22, git", Image: Default},
		{ID: "agents", Name: "Agents", Description: "Base plus Claude Code and Codex", Image: "agents"},
	},
}}

// Catalog returns the OS and template choices, each template with its sizes.
func Catalog() []OS {
	out := make([]OS, len(catalog))
	for i, os := range catalog {
		out[i] = OS{ID: os.ID, Name: os.Name, Templates: make([]Template, len(os.Templates))}
		for j, t := range os.Templates {
			t.Sizes = Sizes(t.Image)
			out[i].Templates[j] = t
		}
	}
	return out
}

// Resolve returns the image to boot for a create request. os and template win
// (each defaulting when only the other is given); without them the legacy image
// field is used as before. An image that disagrees with os/template is an error.
func Resolve(os, template, image string) (string, error) {
	os = strings.ToLower(strings.TrimSpace(os))
	template = strings.ToLower(strings.TrimSpace(template))
	if os == "" && template == "" {
		return Normalize(image)
	}
	if os == "" {
		os = DefaultOS
	}
	if template == "" {
		template = DefaultTemplate
	}
	for _, o := range catalog {
		if o.ID != os {
			continue
		}
		for _, t := range o.Templates {
			if t.ID != template {
				continue
			}
			if image != "" {
				if normalized, err := Normalize(image); err != nil || normalized != t.Image {
					return "", fmt.Errorf("image %q conflicts with os %q and template %q", image, os, template)
				}
			}
			return t.Image, nil
		}
		return "", fmt.Errorf("unknown template %q for os %q (available: %s)", template, os, strings.Join(templateIDs(o), ", "))
	}
	return "", fmt.Errorf("unknown os %q (available: %s)", os, strings.Join(osIDs(), ", "))
}

// Describe returns the OS and template an image belongs to, or empty strings for
// images outside the catalog.
func Describe(image string) (os, template string) {
	for _, o := range catalog {
		for _, t := range o.Templates {
			if t.Image == image {
				return o.ID, t.ID
			}
		}
	}
	return "", ""
}

func osIDs() []string {
	ids := make([]string, len(catalog))
	for i, o := range catalog {
		ids[i] = o.ID
	}
	return ids
}

func templateIDs(o OS) []string {
	ids := make([]string, len(o.Templates))
	for i, t := range o.Templates {
		ids[i] = t.ID
	}
	return ids
}
