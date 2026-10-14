package main

import (
	"strings"
	"testing"
)

// recordWriter keeps every Write as a separate record, like /dev/kmsg.
type recordWriter struct{ records []string }

func (r *recordWriter) Write(p []byte) (int, error) {
	r.records = append(r.records, string(p))
	return len(p), nil
}

func TestKmsgWriterWritesOneDebugRecordPerLine(t *testing.T) {
	out := &recordWriter{}
	w := &kmsgWriter{out: out}
	input := "connection accepted fd=7\n\nconnection closed fd=7: EOF\n"
	n, err := w.Write([]byte(input))
	if err != nil || n != len(input) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(input))
	}
	want := []string{
		"<7>guest-agent: connection accepted fd=7",
		"<7>guest-agent: connection closed fd=7: EOF",
	}
	if strings.Join(out.records, "|") != strings.Join(want, "|") {
		t.Fatalf("records = %q, want %q", out.records, want)
	}
}

func TestKmsgWriterTruncatesLongLines(t *testing.T) {
	out := &recordWriter{}
	w := &kmsgWriter{out: out}
	if _, err := w.Write([]byte(strings.Repeat("x", 5000))); err != nil {
		t.Fatal(err)
	}
	if len(out.records) != 1 || len(out.records[0]) != len(kmsgPrefix)+kmsgMaxLine {
		t.Fatalf("got %d records, first %d bytes; want 1 record of %d bytes", len(out.records), len(out.records[0]), len(kmsgPrefix)+kmsgMaxLine)
	}
}
