package main

import (
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// The serial console stops transmitting after a snapshot restore: Firecracker v1.7
// does not re-raise the UART transmit interrupt the guest was waiting for, so console
// writes queue in the tty and block the writer once ~4 KB is pending. The agent logs
// on every connection, so a few pause/resume cycles left it blocked mid-log and the
// sandbox stopped answering. The agent logs to the kernel log instead, which never
// blocks; read it with dmesg inside the sandbox.

const (
	// kmsgPrefix marks agent lines as debug level: the kernel keeps them in its log
	// but does not print them to the console.
	kmsgPrefix = "<7>guest-agent: "
	// kmsgMaxLine keeps a record under the kernel's limit of 1024 bytes minus header.
	kmsgMaxLine = 900
)

// kmsgWriter writes each line it is given as one kernel log record. Records the
// kernel rejects are dropped: logging must never stop the agent.
type kmsgWriter struct {
	mu  sync.Mutex
	out io.Writer // /dev/kmsg: every write is one record
}

func (w *kmsgWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, line := range strings.Split(string(p), "\n") {
		if line == "" {
			continue
		}
		if len(line) > kmsgMaxLine {
			line = line[:kmsgMaxLine]
		}
		_, _ = io.WriteString(w.out, kmsgPrefix+line) // dropped on error, see above
	}
	return len(p), nil
}

// logToKernel moves the agent's log and its stdout/stderr from the serial console to
// the kernel log. If /dev/kmsg cannot be opened the agent keeps the console.
func logToKernel() {
	kmsg, err := os.OpenFile("/dev/kmsg", os.O_WRONLY, 0)
	if err != nil {
		log.Printf("warning: open /dev/kmsg, logging stays on the console: %v", err)
		return
	}
	// By default the kernel keeps only 10 lines per 5 seconds from one writer.
	if err := os.WriteFile("/proc/sys/kernel/printk_devkmsg", []byte("on\n"), 0o644); err != nil {
		log.Printf("warning: lift /dev/kmsg rate limit: %v", err)
	}
	log.SetOutput(&kmsgWriter{out: kmsg})
	// Raw writes to stdout and stderr (VSOCK_READY, a Go panic) go there too, at the
	// kernel's default level, so they still reach the console.
	for _, fd := range []int{1, 2} {
		if err := unix.Dup3(int(kmsg.Fd()), fd, 0); err != nil {
			log.Printf("warning: move fd %d to /dev/kmsg: %v", fd, err)
		}
	}
}
