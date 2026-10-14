package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseWorkerImages(t *testing.T) {
	sum := strings.Repeat("a", 64)
	images, err := parseWorkerImages(" agents=agents-20261014-abc1234@" + sum + " ,")
	if err != nil || len(images) != 1 || images[0].Name != "agents" || images[0].Version != "agents-20261014-abc1234" || images[0].SHA256 != sum {
		t.Fatalf("parse = %+v, %v", images, err)
	}
	if images, err := parseWorkerImages(""); err != nil || len(images) != 0 {
		t.Fatalf("empty = %+v, %v", images, err)
	}
	for _, bad := range []string{"agents=v1", "agents=v1@short", "alpine=v1@" + sum, "../x=v1@" + sum} {
		if _, err := parseWorkerImages(bad); err == nil {
			t.Errorf("parseWorkerImages(%q) accepted", bad)
		}
	}
}

func TestWriteSparseRoundTrip(t *testing.T) {
	data := make([]byte, 300<<10) // zero blocks with data in the middle and a partial tail
	copy(data[70<<10:], "middle")
	data[len(data)-1] = 7
	path := filepath.Join(t.TempDir(), "out")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeSparse(f, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round trip mismatch: got %d bytes", len(got))
	}
}
