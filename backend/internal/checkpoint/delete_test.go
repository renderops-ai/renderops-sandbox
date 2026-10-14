package checkpoint

import (
	"context"
	"strings"
	"testing"

	"backend/internal/template"

	"github.com/google/uuid"
)

func TestDeleteSandboxRemovesOnlyThatSandbox(t *testing.T) {
	ctx := context.Background()
	store, err := template.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(store, "sandbox-checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	target, other := uuid.NewString(), uuid.NewString()
	for _, id := range []string{target, other} {
		for _, name := range []string{"latest.json", "generations/g1/memory.mem", "generations/g1/manifest.json", "disk-chunks/abc"} {
			if err := store.Put(ctx, "sandbox-checkpoints/"+id+"/"+name, strings.NewReader("x")); err != nil {
				t.Fatal(err)
			}
		}
	}

	deleted, err := writer.DeleteSandbox(ctx, target)
	if err != nil || deleted != 4 {
		t.Fatalf("DeleteSandbox = %d, %v; want 4, nil", deleted, err)
	}
	if keys, _ := store.List(ctx, "sandbox-checkpoints/"+target+"/"); len(keys) != 0 {
		t.Fatalf("target objects left: %v", keys)
	}
	if keys, _ := store.List(ctx, "sandbox-checkpoints/"+other+"/"); len(keys) != 4 {
		t.Fatalf("other sandbox lost objects: %v", keys)
	}
	if deleted, err := writer.DeleteSandbox(ctx, target); err != nil || deleted != 0 {
		t.Fatalf("second DeleteSandbox = %d, %v; want 0, nil", deleted, err)
	}
	for _, bad := range []string{"", "..", "a/b", "not-a-uuid"} {
		if _, err := writer.DeleteSandbox(ctx, bad); err == nil {
			t.Errorf("DeleteSandbox(%q) accepted", bad)
		}
	}
}
