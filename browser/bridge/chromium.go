package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

// Chromium runs headed on the virtual display, sized to fill it (there is no
// window manager), with CDP on loopback for the bridge.

func (s *stack) startChromium(stop <-chan struct{}) (*process, error) {
	if err := os.MkdirAll(s.cfg.profileDir, 0o700); err != nil {
		err = fmt.Errorf("creating chromium profile dir: %w", err)
		s.fail(&s.chromium, err)
		return nil, err
	}
	return s.launch(stop, &s.chromium, procSpec{
		name:  "chromium",
		path:  s.cfg.chromiumPath,
		args:  chromiumArgs(s.cfg),
		env:   []string{"DISPLAY=" + s.cfg.display},
		ready: cdpReady(s.cfg),
	})
}

func chromiumArgs(cfg config) []string {
	args := []string{
		"--remote-debugging-address=127.0.0.1",
		"--remote-debugging-port=" + strconv.Itoa(cfg.cdpPort),
		"--user-data-dir=" + cfg.profileDir,
		"--window-position=0,0",
		fmt.Sprintf("--window-size=%d,%d", cfg.width, cfg.height),
		"--no-first-run",
		"--no-default-browser-check",
		// There is no desktop keyring in the VM.
		"--password-store=basic",
		// /dev/shm may be small or missing in the guest; use /tmp instead.
		"--disable-dev-shm-usage",
	}
	if os.Geteuid() == 0 {
		// Chromium refuses to run as root with its own sandbox enabled. The
		// microVM is the isolation boundary here.
		args = append(args, "--no-sandbox")
	}
	return append(args, "about:blank")
}

// cdpReady reports ready once Chromium answers on its CDP HTTP endpoint.
func cdpReady(cfg config) func(context.Context) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/json/version", cfg.cdpPort)
	client := &http.Client{}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("building cdp probe: %w", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("querying cdp: %w", err)
		}
		_, drainErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("cdp returned %s", resp.Status)
		}
		if drainErr != nil {
			return fmt.Errorf("reading cdp probe: %w", drainErr)
		}
		if closeErr != nil {
			return fmt.Errorf("closing cdp probe: %w", closeErr)
		}
		return nil
	}
}
