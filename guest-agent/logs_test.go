package main

import (
	"encoding/json"
	"errors"
	"testing"
)

type feedWriter struct{ rows []guestLog }

func (w *feedWriter) Write(p []byte) (int, error) {
	var row guestLog
	if err := json.Unmarshal(p, &row); err != nil {
		return 0, err
	}
	w.rows = append(w.rows, row)
	if len(w.rows) == 3 {
		return 0, errors.New("reader closed")
	}
	return len(p), nil
}

func TestOutputBufferReplayReportsGapAndKeepsBoundedMemory(t *testing.T) {
	b := &outputBuffer{max: 6}
	b.write("stdout", []byte("abc"))
	b.write("stderr", []byte("def"))
	b.write("stdout", []byte("ghi"))
	if b.bytes != 6 || len(b.rows) != 2 || b.rows[0].Sequence != 2 {
		t.Fatalf("unexpected replay buffer: %+v", b)
	}
	w := &feedWriter{}
	_ = b.follow(w, "sandbox-1", "", 0)
	if len(w.rows) != 3 || w.rows[0].Stream != "system" || w.rows[0].Sequence != 1 || w.rows[1].Content != "def" {
		t.Fatalf("missing gap/replay: %+v", w.rows)
	}
}

func TestOutputWriterCapturesWithoutAnyTerminalAttachment(t *testing.T) {
	old := outputLogs
	defer func() { outputLogs = old }()
	outputLogs = &outputBuffer{max: 1024}
	n, err := outputWriter("stdout").Write([]byte("hello\n"))
	if n != 6 || err != nil || len(outputLogs.rows) != 1 || outputLogs.rows[0].Content != "hello\n" {
		t.Fatal("output not captured")
	}
}
