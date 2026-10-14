package main

// Output feed is separate from the PTY attachment: closing the UI does not stop
// capture. It contains output only, never terminal input or environment values.
import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync"
	"time"
)

type guestLog struct {
	Epoch     string    `json:"epoch"`
	Sequence  uint64    `json:"sequence"`
	Stream    string    `json:"stream"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"ts"`
}

type outputBuffer struct {
	mu      sync.Mutex
	epoch   string
	sandbox string
	next    uint64
	bytes   int
	max     int
	rows    []guestLog
}

var outputLogs = &outputBuffer{max: 1 << 20}

func (b *outputBuffer) write(stream string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.epoch == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return
		}
		b.epoch = hex.EncodeToString(random[:])
	}
	for len(data) > 0 {
		n := min(len(data), 16*1024)
		b.next++
		b.rows = append(b.rows, guestLog{b.epoch, b.next, stream, string(data[:n]), time.Now().UTC()})
		b.bytes += n
		data = data[n:]
		for b.bytes > b.max && len(b.rows) > 0 {
			b.bytes -= len(b.rows[0].Content)
			b.rows[0] = guestLog{}
			b.rows = b.rows[1:]
		}
	}
}

type outputWriter string

func (w outputWriter) Write(data []byte) (int, error) {
	outputLogs.write(string(w), data)
	return len(data), nil
}

// A bounded replay buffer covers reconnects. Missing output is reported rather
// than silently claiming an exhaustive audit trail.
func (b *outputBuffer) follow(w io.Writer, sandbox, epoch string, after uint64) error {
	b.mu.Lock()
	if b.sandbox != "" && b.sandbox != sandbox {
		b.rows = nil
		b.bytes = 0
		b.next = 0
		b.epoch = ""
	}
	b.sandbox = sandbox
	b.mu.Unlock()
	encoder := json.NewEncoder(w)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastHeartbeat := time.Now()
	for {
		b.mu.Lock()
		current := b.epoch
		if epoch != current {
			after = 0
			epoch = current
		}
		gap := len(b.rows) > 0 && after+1 < b.rows[0].Sequence
		rows := make([]guestLog, 0)
		for _, row := range b.rows {
			if row.Sequence > after {
				rows = append(rows, row)
			}
		}
		b.mu.Unlock()
		if gap {
			// Sequence just before the first retained event, stable across retries.
			row := guestLog{current, rows[0].Sequence - 1, "system", "[output gap: guest replay buffer exceeded]\n", time.Now().UTC()}
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
		for _, row := range rows {
			if err := encoder.Encode(row); err != nil {
				return err
			}
			after = row.Sequence
		}
		if time.Since(lastHeartbeat) >= 10*time.Second {
			if err := encoder.Encode(map[string]bool{"heartbeat": true}); err != nil {
				return err
			}
			lastHeartbeat = time.Now()
		}
		<-ticker.C
	}
}
