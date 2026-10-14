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

type catalogResponse struct {
	OS    []sandboximage.OS `json:"os"`
	Sizes []catalogSize     `json:"sizes"`
}

// CatalogHandler serves GET /v1/catalog: the OS -> template -> size choices a
// create request can make, so clients do not hardcode them.
func CatalogHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		response := catalogResponse{OS: sandboximage.Catalog()}
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
