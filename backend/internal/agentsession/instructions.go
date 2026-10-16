package agentsession

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Instructions are the agent instructions the platform renders for a session
// (the claim's session.instructions). The runner writes them into the guest
// as they are: System to system.txt (appended to Claude Code's system
// prompt), and Notices at the top of a turn's prompt file.
type Instructions struct {
	// Version names the platform's instruction template, for logs.
	Version string
	System  string
	Notices Notices
}

// Notices are prepended to a turn's prompt, inside one <spawn_notice> block,
// when their event happened since the previous turn. An empty notice is not
// sent.
type Notices struct {
	// Resumed: the sandbox was paused and resumed since the previous turn.
	Resumed string
	// Interrupted: the user stopped the previous turn.
	Interrupted string
	// TurnFailed: the previous turn failed or hit its time or step limit.
	TurnFailed string
}

const (
	maxSystemBytes  = 16 << 10
	maxNoticeBytes  = 2 << 10
	maxVersionBytes = 100

	// fallbackVersion is logged when the session uses the runner's own
	// instructions.
	fallbackVersion = "fallback"
)

// fallbackNotices are used when the platform sends no (valid) instructions.
var fallbackNotices = Notices{
	Resumed:     "The sandbox was paused and resumed since your last turn. Files and commits are intact, but processes you started earlier, such as servers, have stopped.",
	Interrupted: "The user stopped your previous turn before it finished. Check git status before you continue.",
	TurnFailed:  "Your previous turn ended early (an error, or its time or step limit). Check git status before you continue.",
}

// fallbackSystemPrompt is the short generic system prompt used when the
// platform sends no (valid) instructions.
func fallbackSystemPrompt(branch string) string {
	return fmt.Sprintf("You are a coding agent in an isolated Spawn sandbox, a Linux microVM created for this session only.\n"+
		"The repository is checked out at %s on branch %s. Commit your work on that branch and stay on it; the user publishes it from the session page.\n"+
		"The sandbox has no GitHub or other credentials, so you cannot push. Never print environment variables.\n",
		defaultRepoDir, branch)
}

// systemPrompt is appended to Claude Code's system prompt (system.txt): the
// platform's text, or the generic fallback.
func systemPrompt(s *Session, branch string) string {
	if s.Instructions != nil {
		return s.Instructions.System
	}
	return fallbackSystemPrompt(branch)
}

// notices are the session's turn notices: the platform's, or the fallback.
func (s *Session) notices() Notices {
	if s.Instructions != nil {
		return s.Instructions.Notices
	}
	return fallbackNotices
}

// instructionsVersion names the instructions the session uses, for logs.
func (s *Session) instructionsVersion() string {
	if s.Instructions != nil {
		return s.Instructions.Version
	}
	return fallbackVersion
}

// parseInstructions validates the claim's session.instructions. Absent (or
// null) instructions are (nil, nil): the session uses the fallback. Anything
// malformed is an error, and the caller falls back too.
func parseInstructions(raw json.RawMessage) (*Instructions, error) {
	if isAbsent(raw) {
		return nil, nil
	}
	var wire struct {
		Version json.RawMessage `json:"version"`
		System  json.RawMessage `json:"system"`
		Notices json.RawMessage `json:"notices"`
	}
	if err := decodeObject(raw, &wire); err != nil {
		return nil, fmt.Errorf("instructions: %w", err)
	}
	version, err := instructionString(wire.Version, "version", maxVersionBytes, true)
	if err != nil {
		return nil, err
	}
	system, err := instructionString(wire.System, "system", maxSystemBytes, true)
	if err != nil {
		return nil, err
	}
	out := &Instructions{Version: version, System: system}
	if isAbsent(wire.Notices) {
		return out, nil
	}
	var notices struct {
		Resumed     json.RawMessage `json:"resumed"`
		Interrupted json.RawMessage `json:"interrupted"`
		TurnFailed  json.RawMessage `json:"turn_failed"`
	}
	if err := decodeObject(wire.Notices, &notices); err != nil {
		return nil, fmt.Errorf("instructions.notices: %w", err)
	}
	for _, n := range []struct {
		raw  json.RawMessage
		name string
		dst  *string
	}{
		{notices.Resumed, "notices.resumed", &out.Notices.Resumed},
		{notices.Interrupted, "notices.interrupted", &out.Notices.Interrupted},
		{notices.TurnFailed, "notices.turn_failed", &out.Notices.TurnFailed},
	} {
		if *n.dst, err = instructionString(n.raw, n.name, maxNoticeBytes, false); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func isAbsent(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

// decodeObject decodes raw, which must be a JSON object, into dst.
func decodeObject(raw json.RawMessage, dst any) error {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("not an object")
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}

// instructionString reads one string field: a JSON string of at most max
// bytes without NUL. A required field must be present and not blank; an
// optional one may be absent or null.
func instructionString(raw json.RawMessage, name string, max int, required bool) (string, error) {
	if isAbsent(raw) {
		if required {
			return "", fmt.Errorf("instructions.%s is missing", name)
		}
		return "", nil
	}
	if trimmed := bytes.TrimSpace(raw); trimmed[0] != '"' {
		return "", fmt.Errorf("instructions.%s is not a string", name)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("instructions.%s: %w", name, err)
	}
	switch {
	case len(s) > max:
		return "", fmt.Errorf("instructions.%s is %d bytes, over the %d-byte limit", name, len(s), max)
	case strings.ContainsRune(s, 0):
		return "", fmt.Errorf("instructions.%s contains a NUL character", name)
	case required && strings.TrimSpace(s) == "":
		return "", fmt.Errorf("instructions.%s is empty", name)
	}
	return s, nil
}

// TurnEnd is how a turn ended: the turn_end marker the guest wrote for it.
type TurnEnd struct {
	Seq  int64
	Exit int
	// Reason is "" (Claude Code exited), interrupted, timeout, lost or
	// start_failed.
	Reason string
}

// interrupted reports a turn the user stopped.
func (e TurnEnd) interrupted() bool { return e.Reason == "interrupted" }

// failed reports a turn that did not finish normally for any other reason:
// a time limit, a lost or unstartable wrapper, or a non-zero exit (Claude
// Code exits 1 on an API error or at its step limit).
func (e TurnEnd) failed() bool {
	return !e.interrupted() && (e.Reason != "" || e.Exit != 0)
}

// turnNotice renders the <spawn_notice> block for the next turn: resumed when
// the sandbox was resumed since the previous turn, and how the previous turn
// ended (nil: unknown or already noticed). It returns "" when no notice
// applies, and the names of the notices it included.
func turnNotice(n Notices, resumed bool, previous *TurnEnd) (string, []string) {
	var lines, names []string
	add := func(name, text string) {
		if text = strings.TrimSpace(text); text != "" {
			lines = append(lines, text)
			names = append(names, name)
		}
	}
	if resumed {
		add("resumed", n.Resumed)
	}
	if previous != nil {
		switch {
		case previous.interrupted():
			add("interrupted", n.Interrupted)
		case previous.failed():
			add("turn_failed", n.TurnFailed)
		}
	}
	if len(lines) == 0 {
		return "", nil
	}
	return "<spawn_notice>\n" + strings.Join(lines, "\n") + "\n</spawn_notice>\n\n", names
}
