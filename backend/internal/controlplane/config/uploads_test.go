package config

import (
	"strings"
	"testing"
)

func setUploads(t *testing.T, env map[string]string) {
	t.Helper()
	setRequired(t)
	for _, name := range []string{"UPLOADS_CONTAINER_NAME", "UPLOADS_MAX_BYTES", "BLOB_STORAGE_ACCOUNT", "BLOB_SECRET_KEY"} {
		t.Setenv(name, env[name])
	}
}

func TestUploadsAreOffByDefault(t *testing.T) {
	// Blob credentials alone (a host shared with a worker) do not enable them.
	setUploads(t, map[string]string{"BLOB_STORAGE_ACCOUNT": "renderops", "BLOB_SECRET_KEY": "key"})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Uploads.Enabled() {
		t.Fatalf("uploads = %+v, want off", cfg.Uploads)
	}
}

func TestUploadsConfigured(t *testing.T) {
	setUploads(t, map[string]string{
		"UPLOADS_CONTAINER_NAME": " spawn-uploads ", "BLOB_STORAGE_ACCOUNT": "renderops", "BLOB_SECRET_KEY": "c2VjcmV0",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	u := cfg.Uploads
	if !u.Enabled() || u.Container != "spawn-uploads" || u.Account != "renderops" || u.AccountKey != "c2VjcmV0" || u.MaxBytes != 500<<20 {
		t.Fatalf("uploads = %+v", u)
	}
	t.Setenv("UPLOADS_MAX_BYTES", "1048576")
	if cfg, err := Load(); err != nil || cfg.Uploads.MaxBytes != 1<<20 {
		t.Fatalf("max bytes override: %+v, %v", cfg, err)
	}
}

func TestUploadsFailClosed(t *testing.T) {
	ok := map[string]string{"UPLOADS_CONTAINER_NAME": "spawn-uploads", "BLOB_STORAGE_ACCOUNT": "renderops", "BLOB_SECRET_KEY": "secret-key"}
	for name, change := range map[string]map[string]string{
		"no account":       {"BLOB_STORAGE_ACCOUNT": ""},
		"bad account":      {"BLOB_STORAGE_ACCOUNT": "Render-Ops"},
		"no key":           {"BLOB_SECRET_KEY": ""},
		"bad container":    {"UPLOADS_CONTAINER_NAME": "Uploads"},
		"double hyphen":    {"UPLOADS_CONTAINER_NAME": "spawn--uploads"},
		"short container":  {"UPLOADS_CONTAINER_NAME": "up"},
		"tiny max bytes":   {"UPLOADS_MAX_BYTES": "1024"},
		"huge max bytes":   {"UPLOADS_MAX_BYTES": "10737418240"},
		"max bytes not ok": {"UPLOADS_MAX_BYTES": "500MiB"},
	} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range ok {
				env[k] = v
			}
			for k, v := range change {
				env[k] = v
			}
			setUploads(t, env)
			if _, err := Load(); err == nil {
				t.Fatalf("Load accepted %v", change)
			} else if strings.Contains(err.Error(), "secret-key") {
				t.Fatal("error message contains the key")
			}
		})
	}
}
