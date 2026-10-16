package sandboxfiles

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// output is a script's parsed answer (see scripts.go for the format).
type output struct {
	fields  map[string]string // first value of each "key value" line
	data    map[string][]byte // decoded data blocks
	errCode string
	ok      bool
}

// parseOutput reads a script's stdout. Lines it does not know are ignored;
// a data block must be complete and valid base64.
func parseOutput(stdout string) (*output, error) {
	out := &output{fields: map[string]string{}, data: map[string][]byte{}}
	header := false
	block := ""
	inBlock := false
	var encoded strings.Builder
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimRight(line, "\r")
		if inBlock {
			if line == "end" {
				raw, err := base64.StdEncoding.DecodeString(encoded.String())
				if err != nil {
					return nil, fmt.Errorf("decode %s: %w", block, err)
				}
				out.data[block] = raw
				inBlock = false
				encoded.Reset()
				continue
			}
			encoded.WriteString(line)
			continue
		}
		key, value, _ := strings.Cut(line, " ")
		switch key {
		case "spawn-files":
			header = value == "1"
		case "data":
			block, inBlock = value, true
		case "ok":
			out.ok = true
		case "error":
			if out.errCode == "" {
				out.errCode = value
			}
		default:
			if _, seen := out.fields[key]; !seen && key != "" {
				out.fields[key] = value
			}
		}
	}
	if !header {
		return nil, errors.New("script output has no header")
	}
	if inBlock {
		return nil, fmt.Errorf("data block %s was cut off", block)
	}
	return out, nil
}

// size is the "size" line, 0 when absent.
func (o *output) size() int64 {
	n, err := strconv.ParseInt(o.fields["size"], 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// err turns the script's error line into *Error, with the file's size for
// file_too_large (the patch's for patch_too_large), its current sha256 (nil:
// the file is gone) for conflict, and the refused archive entry ("entry
// <base64>", cut to maxEntryNameBytes) for extract errors.
func (o *output) err() *Error {
	e := newError(o.errCode)
	switch e.Code {
	case CodeFileTooLarge, CodePatchTooLarge:
		e.Size = o.size()
	case CodeConflict:
		if sum := o.fields["sha256"]; ValidSHA256(sum) {
			e.SHA256 = &sum
		}
	}
	if encoded := o.fields["entry"]; encoded != "" {
		if raw, decodeErr := base64.StdEncoding.DecodeString(encoded); decodeErr == nil {
			e.Entry = strings.ToValidUTF8(string(raw[:min(len(raw), maxEntryNameBytes)]), "�")
		}
	}
	return e
}

// splitNUL splits NUL-terminated records. An unterminated last record was
// cut off by a cap and is dropped.
func splitNUL(raw []byte) []string {
	records := strings.Split(string(raw), "\x00")
	return records[:len(records)-1]
}

// lineCount reads a numstat count; binary files count "-", reported as 0.
func lineCount(field string) int {
	n, err := strconv.Atoi(field)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// parseChanges merges git diff --name-status -z and --numstat -z (both with
// renames) with the untracked files, capped at limit files.
func parseChanges(status, numstat, untracked []byte, limit int) ([]Change, bool) {
	truncated := len(status) > maxListBytes || len(numstat) > maxListBytes

	type counts struct{ additions, deletions int }
	stats := map[string]counts{}
	tokens := splitNUL(numstat)
	for i := 0; i < len(tokens); {
		fields := strings.SplitN(tokens[i], "\t", 3)
		if len(fields) != 3 {
			break
		}
		name := fields[2]
		i++
		if name == "" { // rename: the old and new paths follow
			if i+1 >= len(tokens) {
				break
			}
			name = tokens[i+1]
			i += 2
		}
		stats[name] = counts{lineCount(fields[0]), lineCount(fields[1])}
	}

	files := make([]Change, 0)
	add := func(change Change) bool {
		if len(files) == limit {
			truncated = true
			return false
		}
		files = append(files, change)
		return true
	}
	tokens = splitNUL(status)
	for i := 0; i < len(tokens); {
		code := tokens[i]
		i++
		if code == "" {
			break
		}
		change := Change{Status: "M"}
		switch code[0] {
		case 'R', 'C':
			if i+1 >= len(tokens) {
				i = len(tokens)
				continue
			}
			change.From, change.Path = tokens[i], tokens[i+1]
			i += 2
			change.Status = "R"
			if code[0] == 'C' {
				// A copy adds a file (git reports none without --find-copies).
				change.From, change.Status = "", "A"
			}
		default:
			if i >= len(tokens) {
				continue
			}
			change.Path = tokens[i]
			i++
			switch code[0] {
			case 'A':
				change.Status = "A"
			case 'D':
				change.Status = "D"
			}
		}
		c := stats[change.Path]
		change.Additions, change.Deletions = c.additions, c.deletions
		if !add(change) {
			return files, true
		}
	}
	others := splitNUL(untracked)
	// The script lists at most limit+1 untracked files; reaching that means
	// there are more changes than limit.
	if len(others) > limit {
		truncated = true
	}
	for _, name := range others {
		// "dir/" is an untracked nested repository, not a file.
		if name == "" || strings.HasSuffix(name, "/") {
			continue
		}
		if !add(Change{Path: name, Status: "?"}) {
			return files, true
		}
	}
	return files, truncated
}
