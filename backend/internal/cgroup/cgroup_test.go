package cgroup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadOOMKills(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    uint64
		wantErr bool
	}{
		{name: "oom killed", content: "low 0\nhigh 0\nmax 1902\noom 1\noom_kill 1\noom_group_kill 0\n", want: 1},
		{name: "never killed", content: "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n", want: 0},
		{name: "no counter", content: "low 0\n", wantErr: true},
		{name: "bad counter", content: "oom_kill x\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "memory.events")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := ReadOOMKills(path)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("ReadOOMKills() = %d, %v; want %d, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
	if _, err := ReadOOMKills(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing memory.events read without error")
	}
}

func TestCgroupOOMKillsReadsItsMemoryEvents(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte("oom_kill 3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := (&Cgroup{path: dir}).OOMKills()
	if err != nil || got != 3 {
		t.Fatalf("OOMKills() = %d, %v", got, err)
	}
}
