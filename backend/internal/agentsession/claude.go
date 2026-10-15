package agentsession

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Event is one session event as the platform's internal API takes it (see
// EVENT_TYPES in the platform's src/lib/agent-sessions.js).
type Event struct {
	Type    string         `json:"type"`
	Actor   string         `json:"actor"`
	Payload map[string]any `json:"payload"`
}

// Released is the events decoded from consecutive journal lines that are ready
// to commit. End is the journal offset just past the last of those lines: the
// runtime cursor to commit together with Events.
type Released struct {
	End    int64
	Events []Event
}

const (
	maxMessageBytes = 24 << 10 // JSON-escaped, stays under the platform's 64 KB payload limit
	maxOutputBytes  = 4 << 10  // command output: the last 4 KB
	maxCommandBytes = 2 << 10
	maxTitleBytes   = 300
	maxLineEvents   = 50
)

// DecoderOptions configure a Decoder.
type DecoderOptions struct {
	// RepoDir is stripped from file paths shown to the user.
	RepoDir string
	// TurnTimeout is named in the message for a turn that hit its time limit.
	TurnTimeout time.Duration
}

// Decoder turns the guest journal (events.jsonl: Claude Code's stream-json
// output plus the turn wrapper's spawn markers) into session events.
//
// It is stateful: a Bash or Edit tool_use becomes an event only when its
// tool_result arrives (exit code, output, line counts), and no event after a
// waiting tool_use is released before it. The commit point (Released.End) is
// therefore always a line boundary before every unresolved tool_use, so a
// runner that restarts from the committed cursor decodes the same events
// again: nothing is lost and nothing is published twice. Thinking blocks are
// never published; unknown line and block types are ignored.
type Decoder struct {
	opts     DecoderOptions
	offset   int64
	skipping bool
	queue    []*entry
	slots    map[string]*slot
	// resultSeen: the current turn's result line was decoded, so a non-zero
	// wrapper exit needs no message of its own.
	resultSeen bool
}

type entry struct {
	end     int64
	events  []*Event
	waiting int
}

type slot struct {
	entry *entry
	index int
	name  string
	input toolInput
}

// NewDecoder starts decoding at journal offset start (the committed cursor).
func NewDecoder(start int64, opts DecoderOptions) *Decoder {
	return &Decoder{opts: opts, offset: start, slots: map[string]*slot{}}
}

// Offset is the journal offset consumed so far.
func (d *Decoder) Offset() int64 { return d.offset }

// Busy reports whether consumed lines still wait for a tool result.
func (d *Decoder) Busy() bool { return len(d.queue) > 0 && d.queue[0].waiting > 0 }

// Feed decodes the complete lines of data, which must start at Offset(), and
// returns how many bytes it consumed (through the last newline). full means
// data was cut at the poll's read limit: a full chunk without any newline is
// part of an oversized line, which is skipped rather than blocking the journal.
func (d *Decoder) Feed(at int64, data []byte, full bool) (int, error) {
	if at != d.offset {
		return 0, fmt.Errorf("journal data starts at %d, decoder is at %d", at, d.offset)
	}
	consumed := 0
	for consumed < len(data) {
		rest := data[consumed:]
		nl := bytes.IndexByte(rest, '\n')
		if d.skipping {
			if nl < 0 {
				consumed += len(rest)
				break
			}
			consumed += nl + 1
			d.skipping = false
			d.queue = append(d.queue, &entry{end: at + int64(consumed)})
			continue
		}
		if nl < 0 {
			if full && consumed == 0 {
				// One line is larger than a whole poll chunk (for example a
				// tool result with a huge file). It carries nothing we
				// publish; skip to its end.
				d.skipping = true
				consumed = len(rest)
			}
			break
		}
		consumed += nl + 1
		d.line(rest[:nl], at+int64(consumed))
	}
	d.offset = at + int64(consumed)
	return consumed, nil
}

// Take returns the decoded events that are ready to commit, in journal order.
func (d *Decoder) Take() []Released {
	var out []Released
	for len(d.queue) > 0 && d.queue[0].waiting == 0 {
		e := d.queue[0]
		d.queue = d.queue[1:]
		r := Released{End: e.end}
		for _, ev := range e.events {
			if ev != nil {
				r.Events = append(r.Events, *ev)
			}
		}
		out = append(out, r)
	}
	return out
}

// Flush resolves tool calls that never got a result (the turn ended or was
// killed first), so everything consumed can be released.
func (d *Decoder) Flush() {
	for id, s := range d.slots {
		s.entry.events[s.index] = s.unresolved(d)
		s.entry.waiting--
		delete(d.slots, id)
	}
}

// streamLine is the subset of a stream-json line (or spawn marker) we read.
type streamLine struct {
	Type            string          `json:"type"`
	Subtype         string          `json:"subtype"`
	Model           string          `json:"model"`
	IsError         bool            `json:"is_error"`
	APIErrorStatus  *int            `json:"api_error_status"`
	ParentToolUseID *string         `json:"parent_tool_use_id"`
	Message         *streamMessage  `json:"message"`
	ToolUseResult   json.RawMessage `json:"tool_use_result"`

	// Spawn markers written by the guest turn wrapper.
	Event  string `json:"event"`
	Seq    int64  `json:"seq"`
	Exit   *int   `json:"exit"`
	Reason string `json:"reason"`
}

type streamMessage struct {
	Content json.RawMessage `json:"content"`
}

type contentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
}

type toolInput struct {
	Command      string `json:"command"`
	Description  string `json:"description"`
	FilePath     string `json:"file_path"`
	Path         string `json:"path"`
	NotebookPath string `json:"notebook_path"`
	Pattern      string `json:"pattern"`
	OldString    string `json:"old_string"`
	NewString    string `json:"new_string"`
	Content      string `json:"content"`
	Edits        []struct {
		OldString string `json:"old_string"`
		NewString string `json:"new_string"`
	} `json:"edits"`
	Todos []struct {
		Content string `json:"content"`
		Status  string `json:"status"`
	} `json:"todos"`
}

type toolResultMeta struct {
	Type            string `json:"type"`
	Content         string `json:"content"`
	Interrupted     bool   `json:"interrupted"`
	StructuredPatch []struct {
		Lines []string `json:"lines"`
	} `json:"structuredPatch"`
}

func (d *Decoder) line(raw []byte, end int64) {
	e := &entry{end: end}
	d.queue = append(d.queue, e)
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return
	}
	var l streamLine
	if err := json.Unmarshal(raw, &l); err != nil {
		// A process killed mid-write leaves a partial line; skip it.
		return
	}
	switch l.Type {
	case "spawn":
		d.marker(e, l)
	case "system":
		if l.Subtype == "init" {
			title := "Claude Code started"
			if model := clean(l.Model, 100); model != "" {
				title += " (" + model + ")"
			}
			d.add(e, step(title))
		}
	case "assistant":
		if l.ParentToolUseID != nil && *l.ParentToolUseID != "" {
			return // a subagent's own work
		}
		for _, b := range blocks(l.Message) {
			d.assistantBlock(e, b)
		}
	case "user":
		if l.ParentToolUseID != nil && *l.ParentToolUseID != "" {
			return
		}
		results := blocks(l.Message)
		var meta *toolResultMeta
		if len(results) == 1 && len(l.ToolUseResult) > 0 && l.ToolUseResult[0] == '{' {
			meta = &toolResultMeta{}
			if json.Unmarshal(l.ToolUseResult, meta) != nil {
				meta = nil
			}
		}
		for _, b := range results {
			if b.Type == "tool_result" {
				d.toolResult(b, meta)
			}
		}
	case "result":
		d.resultSeen = true
		if l.IsError || (l.Subtype != "" && l.Subtype != "success") {
			d.add(e, systemMessage(resultReason(l)))
		}
	}
}

func (d *Decoder) marker(e *entry, l streamLine) {
	switch l.Event {
	case "turn_start":
		d.Flush()
		d.resultSeen = false
		d.add(e, Event{Type: "status", Actor: "system", Payload: map[string]any{"status": "running"}})
	case "turn_end":
		d.Flush()
		exit := 0
		if l.Exit != nil {
			exit = *l.Exit
		}
		switch {
		case l.Reason == "interrupted":
			d.add(e, systemMessage("Stopped."))
		case l.Reason == "timeout" || exit == 124:
			d.add(e, systemMessage(fmt.Sprintf("The turn hit its %s time limit and was stopped.", formatDuration(d.opts.TurnTimeout))))
		case l.Reason == "lost":
			d.add(e, systemMessage("The agent process exited unexpectedly. Send a message to continue."))
		case l.Reason == "start_failed":
			d.add(e, systemMessage("The agent could not be started in the sandbox."))
		case exit != 0 && !d.resultSeen:
			d.add(e, systemMessage(fmt.Sprintf("The agent exited with an error (exit code %d).", exit)))
		}
		d.resultSeen = false
	}
}

func (d *Decoder) assistantBlock(e *entry, b contentBlock) {
	switch b.Type {
	case "text":
		if text := clean(b.Text, maxMessageBytes); strings.TrimSpace(text) != "" {
			d.add(e, Event{Type: "message", Actor: "agent", Payload: map[string]any{"text": text}})
		}
	case "tool_use":
		var in toolInput
		if len(b.Input) > 0 {
			// Unknown or partial input only loses detail, never the event.
			if err := json.Unmarshal(b.Input, &in); err != nil {
				in = toolInput{}
			}
		}
		switch b.Name {
		case "Bash", "Edit", "MultiEdit", "Write", "NotebookEdit":
			if b.ID == "" {
				return
			}
			if len(e.events) >= maxLineEvents {
				return
			}
			e.events = append(e.events, nil)
			e.waiting++
			d.slots[b.ID] = &slot{entry: e, index: len(e.events) - 1, name: b.Name, input: in}
		case "Read":
			d.add(e, step("Read "+d.path(in.FilePath)))
		case "Grep":
			d.add(e, step("Searched for "+quoteTitle(in.Pattern)))
		case "Glob":
			d.add(e, step("Listed files matching "+quoteTitle(in.Pattern)))
		case "TodoWrite":
			d.add(e, todoStep(in))
		case "Task", "Agent":
			title := "Started a subagent"
			if desc := clean(in.Description, 200); desc != "" {
				title += ": " + desc
			}
			d.add(e, step(title))
		default:
			if name := clean(b.Name, 80); name != "" {
				d.add(e, step("Used "+name))
			}
		}
	}
	// "thinking" and any other block type are never published.
}

func (d *Decoder) toolResult(b contentBlock, meta *toolResultMeta) {
	s := d.slots[b.ToolUseID]
	if s == nil {
		return
	}
	delete(d.slots, b.ToolUseID)
	s.entry.events[s.index] = s.resolved(d, b, meta)
	s.entry.waiting--
}

func (s *slot) resolved(d *Decoder, b contentBlock, meta *toolResultMeta) *Event {
	if s.name == "Bash" {
		text := resultText(b.Content)
		payload := map[string]any{"cmd": clean(s.input.Command, maxCommandBytes)}
		exitCode := 0
		if b.IsError {
			exitCode = 1
			if m := exitCodePattern.FindStringSubmatch(text); m != nil {
				if n, err := strconv.Atoi(m[1]); err == nil {
					exitCode = n
				}
				text = text[len(m[0]):]
			}
		}
		if meta != nil && meta.Interrupted {
			text = strings.TrimRight(text, "\n") + "\n(interrupted)"
		} else {
			payload["exitCode"] = exitCode
		}
		payload["output"] = tail(clean(text, -1), maxOutputBytes)
		return &Event{Type: "command", Actor: "agent", Payload: payload}
	}
	if b.IsError {
		return nil // a failed edit changed nothing; the agent will say what it does next
	}
	adds, dels := s.input.lineCounts(s.name)
	verb := "Edited "
	if meta != nil {
		if len(meta.StructuredPatch) > 0 {
			adds, dels = 0, 0
			for _, hunk := range meta.StructuredPatch {
				for _, line := range hunk.Lines {
					switch {
					case strings.HasPrefix(line, "+"):
						adds++
					case strings.HasPrefix(line, "-"):
						dels++
					}
				}
			}
		}
		if meta.Type == "create" {
			verb = "Created "
			adds, dels = countLines(meta.Content), 0
			if meta.Content == "" {
				adds = countLines(s.input.Content)
			}
		}
	}
	return editStep(verb+d.path(s.editPath()), adds, dels)
}

func (s *slot) unresolved(d *Decoder) *Event {
	if s.name == "Bash" {
		return &Event{Type: "command", Actor: "agent", Payload: map[string]any{
			"cmd":    clean(s.input.Command, maxCommandBytes),
			"output": "(the command did not finish before the turn ended)",
		}}
	}
	adds, dels := s.input.lineCounts(s.name)
	return editStep("Edited "+d.path(s.editPath()), adds, dels)
}

func (s *slot) editPath() string {
	if s.input.FilePath != "" {
		return s.input.FilePath
	}
	return s.input.NotebookPath
}

func (in toolInput) lineCounts(tool string) (int, int) {
	switch tool {
	case "Write":
		return countLines(in.Content), 0
	case "MultiEdit":
		adds, dels := 0, 0
		for _, e := range in.Edits {
			adds += countLines(e.NewString)
			dels += countLines(e.OldString)
		}
		return adds, dels
	default:
		return countLines(in.NewString), countLines(in.OldString)
	}
}

func (d *Decoder) add(e *entry, ev Event) {
	if len(e.events) < maxLineEvents {
		e.events = append(e.events, &ev)
	}
}

func (d *Decoder) path(p string) string {
	p = strings.TrimSpace(p)
	if d.opts.RepoDir != "" {
		p = strings.TrimPrefix(p, strings.TrimRight(d.opts.RepoDir, "/")+"/")
	}
	if p == "" {
		p = "a file"
	}
	return clean(p, maxTitleBytes)
}

func blocks(m *streamMessage) []contentBlock {
	if m == nil || len(m.Content) == 0 || m.Content[0] != '[' {
		return nil
	}
	var out []contentBlock
	if err := json.Unmarshal(m.Content, &out); err != nil {
		return nil
	}
	return out
}

// resultText reads a tool_result's content: a string or a list of text blocks.
func resultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

var exitCodePattern = regexp.MustCompile(`^Exit code (-?\d+)\n?`)

// resultReason turns an error result into a message without provider detail
// (raw provider errors can carry request IDs or account information).
func resultReason(l streamLine) string {
	if l.Subtype == "error_max_turns" {
		return "The agent stopped after reaching its step limit for this turn. Send a message to let it continue."
	}
	if l.APIErrorStatus != nil {
		switch status := *l.APIErrorStatus; {
		case status == 401 || status == 403:
			return "The model provider rejected the saved Anthropic credentials. Update them on the Secrets page."
		case status == 429:
			return "The model provider is rate limiting requests. Try again in a few minutes."
		case status >= 500:
			return "The model provider is unavailable right now. Try again in a few minutes."
		}
	}
	return "The agent hit an error and stopped this turn."
}

func step(title string) Event {
	return Event{Type: "step", Actor: "agent", Payload: map[string]any{"title": clean(title, maxTitleBytes)}}
}

func editStep(title string, adds, dels int) *Event {
	ev := step(title)
	ev.Payload["additions"] = adds
	ev.Payload["deletions"] = dels
	return &ev
}

func todoStep(in toolInput) Event {
	done := 0
	var detail strings.Builder
	for _, todo := range in.Todos {
		mark := "[ ] "
		switch todo.Status {
		case "completed":
			done++
			mark = "[x] "
		case "in_progress":
			mark = "[~] "
		}
		detail.WriteString(mark + clean(todo.Content, 200) + "\n")
	}
	ev := step(fmt.Sprintf("Updated the plan (%d/%d done)", done, len(in.Todos)))
	if detail.Len() > 0 {
		ev.Payload["detail"] = clean(strings.TrimRight(detail.String(), "\n"), 4<<10)
	}
	return ev
}

func systemMessage(text string) Event {
	return Event{Type: "message", Actor: "system", Payload: map[string]any{"text": text}}
}

func quoteTitle(s string) string {
	return "“" + clean(s, 200) + "”"
}

func countLines(s string) int {
	if s == "" {
		return 0
	}
	n := strings.Count(s, "\n")
	if !strings.HasSuffix(s, "\n") {
		n++
	}
	return n
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]`)

// clean strips terminal escapes and control characters (keeping newlines and
// tabs), repairs UTF-8 and, for max > 0, truncates to max bytes.
func clean(s string, max int) string {
	s = ansiPattern.ReplaceAllString(strings.ToValidUTF8(s, "�"), "")
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if r == '\r' {
			return -1
		}
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	if max > 0 && len(s) > max {
		cut := max
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + "…"
	}
	return s
}

// tail keeps the last max bytes of s on a rune boundary.
func tail(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := len(s) - max
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return "…" + s[cut:]
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "time"
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%d-hour", int(d/time.Hour))
	}
	if d%time.Minute == 0 {
		return fmt.Sprintf("%d-minute", int(d/time.Minute))
	}
	return d.String()
}
