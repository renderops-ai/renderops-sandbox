package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"backend/internal/sandboximage"
	"backend/internal/vmsize"
)

type catalogSize struct {
	Name     string `json:"name"`
	VCPUs    int    `json:"vcpus"`
	MemoryMB int    `json:"memory_mb"`
	DiskGB   int    `json:"disk_gb"`
}

// catalogAgent is a coding agent a session can run in the agents template, and
// the model provider it uses (through the platform's model proxy).
type catalogAgent struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Template  string `json:"template"`
	Available bool   `json:"available"`
	Note      string `json:"note,omitempty"`
}

var catalogAgents = []catalogAgent{
	{ID: "claude-code", Name: "Claude Code", Provider: "anthropic", Template: "agents", Available: true},
	{ID: "codex", Name: "Codex", Provider: "openai", Template: "agents", Available: false, Note: "GPT models are coming soon"},
}

type catalogResponse struct {
	OS     []sandboximage.OS `json:"os"`
	Sizes  []catalogSize     `json:"sizes"`
	Agents []catalogAgent    `json:"agents"`
}

// CatalogHandler serves GET /v1/catalog: the OS -> template -> size choices a
// create request can make, so clients do not hardcode them.
func CatalogHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		response := catalogResponse{OS: sandboximage.Catalog(), Agents: catalogAgents}
		for _, sz := range vmsize.All() {
			response.Sizes = append(response.Sizes, catalogSize{Name: sz.Name, VCPUs: sz.VCPUs, MemoryMB: sz.MemoryMB, DiskGB: sz.DiskGB})
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "max-age=60")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "encode catalog", http.StatusInternalServerError)
		}
	}
}

func osOf(image string) string {
	os, _ := sandboximage.Describe(image)
	return os
}

func templateOf(image string) string {
	_, template := sandboximage.Describe(image)
	return template
}

func sizeNotAvailableMessage(image, size string) string {
	allowed := strings.Join(sandboximage.Sizes(image), ", ")
	if os, template := sandboximage.Describe(image); template != "" {
		return fmt.Sprintf("template %q on %q does not run at size %q (allowed: %s)", template, os, size, allowed)
	}
	return fmt.Sprintf("image %q does not run at size %q (allowed: %s)", image, size, allowed)
}
