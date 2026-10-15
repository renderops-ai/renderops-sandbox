package agentsession

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/claude-stream-trial.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

var decoderOpts = DecoderOptions{RepoDir: "/workspace/repo", TurnTimeout: 45 * time.Minute}

// decodeAll feeds data in one go and returns every released event.
func decodeAll(t *testing.T, data []byte) ([]Event, *Decoder) {
	t.Helper()
	d := NewDecoder(0, decoderOpts)
	if _, err := d.Feed(0, data, false); err != nil {
		t.Fatal(err)
	}
	var events []Event
	for _, r := range d.Take() {
		events = append(events, r.Events...)
	}
	return events, d
}

func ofType(events []Event, typ string) []Event {
	var out []Event
	for _, e := range events {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

// The real trial stream (Claude Code 2.1.296, permission mode auto) decodes
// into the session's story: start, commands with exit codes and output,
// edits with line counts, the agent's messages; never thinking.
func TestDecodeTrialFixture(t *testing.T) {
	data := fixture(t)
	events, d := decodeAll(t, data)
	if d.Offset() != int64(len(data)) || d.Busy() {
		t.Fatalf("offset %d of %d, busy %v", d.Offset(), len(data), d.Busy())
	}
	if len(events) != 24 {
		t.Fatalf("got %d events, want 24", len(events))
	}
	if first := events[0]; first.Type != "step" || first.Payload["title"] != "Claude Code started (claude-opus-5-5)" {
		t.Fatalf("first event = %+v", first)
	}

	commands := ofType(events, "command")
	if len(commands) != 11 {
		t.Fatalf("got %d commands, want 11", len(commands))
	}
	if c := commands[0]; c.Payload["cmd"] != "cd /workspace/repo/backend && ls ingestion && cat ingestion/main.go" ||
		c.Payload["exitCode"] != 0 || !strings.HasPrefix(c.Payload["output"].(string), "Dockerfile\nmain.go\n") {
		t.Fatalf("first command = %+v", c.Payload)
	}
	failed := commands[2]
	if !strings.HasPrefix(failed.Payload["cmd"].(string), "gofmt -l ingestion; go vet") || failed.Payload["exitCode"] != 1 || failed.Payload["output"] != "" {
		t.Fatalf("failed command = %+v", failed.Payload)
	}
	for _, c := range commands {
		if out := c.Payload["output"].(string); len(out) > maxOutputBytes+len("…") {
			t.Fatalf("output is %d bytes", len(out))
		}
	}

	steps := ofType(events, "step")
	var titles []string
	for _, s := range steps {
		titles = append(titles, s.Payload["title"].(string))
	}
	wantTitles := []string{
		"Claude Code started (claude-opus-5-5)",
		"Read backend/ingestion/main.go",
		"Edited backend/ingestion/main.go",
		"Edited backend/ingestion/main.go",
		"Created backend/ingestion/main_test.go",
	}
	if !reflect.DeepEqual(titles, wantTitles) {
		t.Fatalf("steps = %q", titles)
	}
	if steps[2].Payload["additions"] != 7 || steps[2].Payload["deletions"] != 0 ||
		steps[3].Payload["additions"] != 1 || steps[4].Payload["additions"] != 40 {
		t.Fatalf("line counts = %v %v %v", steps[2].Payload, steps[3].Payload, steps[4].Payload)
	}

	messages := ofType(events, "message")
	if len(messages) != 8 {
		t.Fatalf("got %d messages, want 8", len(messages))
	}
	for _, m := range messages {
		if m.Actor != "agent" {
			t.Fatalf("message actor %q", m.Actor)
		}
	}
	if last := messages[len(messages)-1].Payload["text"].(string); !strings.HasPrefix(last, "`/ingest` now returns 400") {
		t.Fatalf("last message = %q", last)
	}

	for _, e := range events {
		raw, err := json.Marshal(e.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > 64_000 {
			t.Fatalf("%s payload is %d bytes", e.Type, len(raw))
		}
		if bytes.Contains(raw, []byte("signature")) || bytes.Contains(raw, []byte("thinking")) {
			t.Fatalf("thinking leaked: %s", raw)
		}
	}
}

// A runner may restart from any committed cursor: decoding the rest of the
// journal from there must publish exactly the events not yet committed.
func TestDecoderRestartFromCommittedCursorIsExact(t *testing.T) {
	data := fixture(t)
	want, _ := decodeAll(t, data)
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 200; trial++ {
		var got []Event
		d := NewDecoder(0, decoderOpts)
		committed := int64(0)
		for d.Offset() < int64(len(data)) {
			// Feed a random chunk, commit what is released, and sometimes
			// "crash" and restart from the committed cursor.
			end := min(int(d.Offset())+1+rng.Intn(9000), len(data))
			if _, err := d.Feed(d.Offset(), data[d.Offset():end], false); err != nil {
				t.Fatal(err)
			}
			for _, r := range d.Take() {
				got = append(got, r.Events...)
				committed = r.End
			}
			if rng.Intn(3) == 0 {
				d = NewDecoder(committed, decoderOpts)
			}
		}
		for _, r := range d.Take() {
			got = append(got, r.Events...)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d: replay published %d events, want %d", trial, len(got), len(want))
		}
	}
}

func lines(s ...string) []byte { return []byte(strings.Join(s, "\n") + "\n") }

const (
	bashUse    = `{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"go test ./..."}}]},"parent_tool_use_id":null}`
	bashResult = `{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":"Exit code 2\nFAIL\tpkg","is_error":true}]},"parent_tool_use_id":null}`
	textLine   = `{"type":"assistant","message":{"content":[{"type":"text","text":"Fixing the test."}]},"parent_tool_use_id":null}`
)

func TestDecoderHoldsEventsBehindAWaitingToolCall(t *testing.T) {
	d := NewDecoder(100, decoderOpts)
	first := lines(textLine, bashUse, textLine)
	if _, err := d.Feed(100, first, false); err != nil {
		t.Fatal(err)
	}
	released := d.Take()
	if len(released) != 1 || len(released[0].Events) != 1 || released[0].Events[0].Type != "message" {
		t.Fatalf("released before the result: %+v", released)
	}
	// The commit point stops before the waiting tool call.
	if want := int64(100 + len(textLine) + 1); released[0].End != want {
		t.Fatalf("end = %d, want %d", released[0].End, want)
	}
	if !d.Busy() {
		t.Fatal("decoder should be waiting for the tool result")
	}
	result := lines(bashResult)
	if _, err := d.Feed(d.Offset(), result, false); err != nil {
		t.Fatal(err)
	}
	released = d.Take()
	var events []Event
	for _, r := range released {
		events = append(events, r.Events...)
	}
	if len(events) != 2 || events[0].Type != "command" || events[1].Type != "message" {
		t.Fatalf("events = %+v", events)
	}
	if events[0].Payload["exitCode"] != 2 || events[0].Payload["output"] != "FAIL\tpkg" {
		t.Fatalf("command = %+v", events[0].Payload)
	}
	if last := released[len(released)-1].End; last != d.Offset() {
		t.Fatalf("last end %d, offset %d", last, d.Offset())
	}
}

func TestDecoderTurnMarkers(t *testing.T) {
	cases := []struct {
		name   string
		marker string
		want   string
	}{
		{"clean exit", `{"type":"spawn","event":"turn_end","seq":4,"exit":0,"reason":""}`, ""},
		{"interrupted", `{"type":"spawn","event":"turn_end","seq":4,"exit":130,"reason":"interrupted"}`, "Stopped."},
		{"timeout", `{"type":"spawn","event":"turn_end","seq":4,"exit":143,"reason":"timeout"}`, "The turn hit its 45-minute time limit and was stopped."},
		{"gnu timeout", `{"type":"spawn","event":"turn_end","seq":4,"exit":124}`, "The turn hit its 45-minute time limit and was stopped."},
		{"lost wrapper", `{"type":"spawn","event":"turn_end","seq":4,"exit":-1,"reason":"lost"}`, "The agent process exited unexpectedly. Send a message to continue."},
		{"start failed", `{"type":"spawn","event":"turn_end","seq":4,"exit":127,"reason":"start_failed"}`, "The agent could not be started in the sandbox."},
		{"crash", `{"type":"spawn","event":"turn_end","seq":4,"exit":1,"reason":""}`, "The agent exited with an error (exit code 1)."},
	}
	for _, c := range cases {
		events, _ := decodeAll(t, lines(`{"type":"spawn","event":"turn_start","seq":4}`, c.marker))
		if len(events) == 0 || events[0].Type != "status" || events[0].Payload["status"] != "running" || events[0].Actor != "system" {
			t.Fatalf("%s: turn_start = %+v", c.name, events)
		}
		rest := events[1:]
		if c.want == "" {
			if len(rest) != 0 {
				t.Fatalf("%s: unexpected %+v", c.name, rest)
			}
			continue
		}
		if len(rest) != 1 || rest[0].Type != "message" || rest[0].Actor != "system" || rest[0].Payload["text"] != c.want {
			t.Fatalf("%s: got %+v", c.name, rest)
		}
	}
}

func TestDecoderTurnEndResolvesUnfinishedToolCalls(t *testing.T) {
	events, d := decodeAll(t, lines(
		`{"type":"spawn","event":"turn_start","seq":2}`,
		bashUse,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t2","name":"Write","input":{"file_path":"/workspace/repo/a.txt","content":"one\ntwo\n"}}]}}`,
		`{"type":"spawn","event":"turn_end","seq":2,"exit":130,"reason":"interrupted"}`,
	))
	if d.Busy() {
		t.Fatal("still busy after turn_end")
	}
	if len(events) != 4 {
		t.Fatalf("events = %+v", events)
	}
	if c := events[1]; c.Type != "command" || c.Payload["cmd"] != "go test ./..." || c.Payload["exitCode"] != nil {
		t.Fatalf("unfinished command = %+v", c)
	}
	if s := events[2]; s.Type != "step" || s.Payload["title"] != "Edited a.txt" || s.Payload["additions"] != 2 {
		t.Fatalf("unfinished write = %+v", s)
	}
	if events[3].Payload["text"] != "Stopped." {
		t.Fatalf("last = %+v", events[3])
	}
}

func TestDecoderResultErrorsAreSanitized(t *testing.T) {
	cases := map[string]string{
		`{"type":"result","subtype":"error_max_turns","is_error":true}`:                                                           "step limit",
		`{"type":"result","subtype":"success","is_error":true,"api_error_status":401,"result":"invalid x-api-key sk-ant-secret"}`: "Secrets page",
		`{"type":"result","subtype":"success","is_error":true,"api_error_status":429,"result":"rate limited org 123"}`:            "rate limiting",
		`{"type":"result","subtype":"success","is_error":true,"api_error_status":529,"result":"overloaded"}`:                      "unavailable",
		`{"type":"result","subtype":"error_during_execution","is_error":true,"result":"Bearer abc"}`:                              "hit an error",
	}
	for line, want := range cases {
		events, _ := decodeAll(t, lines(
			`{"type":"spawn","event":"turn_start","seq":2}`, line,
			`{"type":"spawn","event":"turn_end","seq":2,"exit":1,"reason":""}`,
		))
		// status running, the reason; no second message for the exit code.
		if len(events) != 2 || events[1].Actor != "system" {
			t.Fatalf("%s: events = %+v", line, events)
		}
		text := events[1].Payload["text"].(string)
		if !strings.Contains(text, want) || strings.Contains(text, "sk-ant") || strings.Contains(text, "Bearer") || strings.Contains(text, "org 123") {
			t.Fatalf("%s: message %q", line, text)
		}
	}
	events, _ := decodeAll(t, lines(`{"type":"result","subtype":"success","is_error":false,"result":"done"}`))
	if len(events) != 0 {
		t.Fatalf("successful result published %+v", events)
	}
}

func TestDecoderIgnoresThinkingUnknownAndSubagentLines(t *testing.T) {
	events, d := decodeAll(t, lines(
		`{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"secret plan","signature":"sig"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"redacted_thinking","data":"x"},{"type":"text","text":"Visible."}]}}`,
		`{"type":"system","subtype":"thinking_tokens","estimated_tokens":50}`,
		`{"type":"system","subtype":"task_started","task_id":"b1"}`,
		`{"type":"system","subtype":"task_notification","task_id":"b1","status":"failed"}`,
		`{"type":"stream_event","event":{"type":"content_block_delta"}}`,
		`{"type":"assistant","message":{"content":[{"type":"text","text":"subagent chatter"}]},"parent_tool_use_id":"toolu_parent"}`,
		`not json at all`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t9","name":"WebSearch","input":{"query":"secret query"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t8","name":"TodoWrite","input":{"todos":[{"content":"Write test","status":"completed"},{"content":"Fix bug","status":"in_progress"}]}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t7","name":"Grep","input":{"pattern":"TODO"}}]}}`,
	))
	if d.Busy() {
		t.Fatal("busy")
	}
	var got []string
	for _, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("secret")) || bytes.Contains(raw, []byte("subagent")) {
			t.Fatalf("leaked: %s", raw)
		}
		if title, ok := e.Payload["title"].(string); ok {
			got = append(got, title)
		} else {
			got = append(got, e.Payload["text"].(string))
		}
	}
	want := []string{"Visible.", "Used WebSearch", "Updated the plan (1/2 done)", "Searched for “TODO”"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if detail := events[2].Payload["detail"]; detail != "[x] Write test\n[~] Fix bug" {
		t.Fatalf("todo detail = %q", detail)
	}
}

func TestDecoderSkipsALineLargerThanAPoll(t *testing.T) {
	d := NewDecoder(0, decoderOpts)
	huge := bytes.Repeat([]byte("x"), pollChunk)
	n, err := d.Feed(0, huge, true)
	if err != nil || n != pollChunk {
		t.Fatalf("consumed %d, err %v", n, err)
	}
	// The oversized line ends in the next chunk, then a normal line follows.
	rest := append([]byte(`"}`+"\n"), lines(textLine)...)
	if _, err := d.Feed(d.Offset(), rest, false); err != nil {
		t.Fatal(err)
	}
	var events []Event
	released := d.Take()
	for _, r := range released {
		events = append(events, r.Events...)
	}
	if len(events) != 1 || events[0].Payload["text"] != "Fixing the test." {
		t.Fatalf("events = %+v", events)
	}
	if released[len(released)-1].End != d.Offset() || d.Offset() != int64(pollChunk+len(rest)) {
		t.Fatalf("offset %d", d.Offset())
	}
}

func TestDecoderKeepsPartialLines(t *testing.T) {
	d := NewDecoder(0, decoderOpts)
	whole := lines(textLine)
	n, err := d.Feed(0, whole[:20], false)
	if err != nil || n != 0 || d.Offset() != 0 {
		t.Fatalf("consumed %d of a partial line", n)
	}
	if _, err := d.Feed(5, whole, false); err == nil {
		t.Fatal("accepted data from the wrong offset")
	}
}

func TestCleanStripsTerminalEscapes(t *testing.T) {
	in := "\x1b[31mred\x1b[0m \x1b]0;title\x07ok\r\n\x00bell\x07"
	if got := clean(in, -1); got != "red ok\nbell" {
		t.Fatalf("clean = %q", got)
	}
	long := strings.Repeat("é", 3000)
	if got := tail(long, maxOutputBytes); len(got) > maxOutputBytes+len("…") || !strings.HasPrefix(got, "…é") {
		t.Fatalf("tail broke a rune: %q", got[:10])
	}
	if got := clean(strings.Repeat("ab", 10), 5); got != "ababa…" {
		t.Fatalf("truncate = %q", got)
	}
}
