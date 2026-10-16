package ide

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"backend/internal/executor"
)

type fakeRunner struct {
	commands []string
	result   executor.ExecutionResult
	err      error
}

// ExecQuiet is the runner's only way to run a command, so every IDE command
// these tests see ran quietly.
func (f *fakeRunner) ExecQuiet(_ context.Context, _ string, command string, _ int) (executor.ExecutionResult, error) {
	f.commands = append(f.commands, command)
	return f.result, f.err
}

func TestServiceRunsEveryCommandQuietly(t *testing.T) {
	runner := &fakeRunner{result: executor.ExecutionResult{Stdout: "ready"}}
	service, err := NewService(runner, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Start(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Status(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if err := service.Stop(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 3 {
		t.Fatalf("quiet commands = %d, want start, status and stop", len(runner.commands))
	}
}

func TestServiceStartsOpenVSCodeWithoutApplicationAuthentication(t *testing.T) {
	runner := &fakeRunner{}
	service, err := NewService(runner, Config{})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := service.Start(context.Background(), "sandbox-1")
	if err != nil {
		t.Fatal(err)
	}
	if instance.Port != DefaultPort || instance.State != "ready" {
		t.Fatalf("instance=%+v", instance)
	}
	command := runner.commands[0]
	if output, err := exec.Command("sh", "-n", "-c", command).CombinedOutput(); err != nil {
		t.Fatalf("invalid start command: %v: %s", err, output)
	}
	for _, expected := range []string{
		"/opt/openvscode-server/bin/openvscode-server",
		"--host 0.0.0.0",
		"--port 3001",
		"--without-connection-token",
		`--server-data-dir "$data_dir"`,
		"--default-folder '/workspace'",
		"--disable-workspace-trust",
		"--telemetry-level off",
		strings.Join(machineSettingsCommands(), "; "),
	} {
		if !strings.Contains(command, expected) {
			t.Fatalf("start command missing %q: %s", expected, command)
		}
	}
	// The settings must be in place before the server starts.
	if strings.Index(command, "settings_file") > strings.Index(command, "nohup") {
		t.Fatalf("settings are written after the server starts: %s", command)
	}
}

func TestMachineSettingsHoldTheIDEDefaults(t *testing.T) {
	var settings map[string]any
	if err := json.Unmarshal([]byte(machineSettings), &settings); err != nil {
		t.Fatalf("machine settings are not JSON: %v", err)
	}
	want := map[string]any{
		"workbench.startupEditor":                          "none",
		"workbench.welcomePage.walkthroughs.openOnInstall": false,
		"workbench.tips.enabled":                           false,
		"workbench.colorTheme":                             "Default Dark Modern",
		"extensions.ignoreRecommendations":                 true,
		"chat.disableAIFeatures":                           true,
		"git.openRepositoryInParentFolders":                "always",
		"git.autofetch":                                    false,
	}
	if len(settings) != len(want) {
		t.Fatalf("settings=%v, want %v", settings, want)
	}
	for key, value := range want {
		if settings[key] != value {
			t.Fatalf("%s=%v, want %v", key, settings[key], value)
		}
	}
}

func TestMachineSettingsCommandsWriteDefaultsOnce(t *testing.T) {
	home := t.TempDir()
	script := "set -eu; " + strings.Join(machineSettingsCommands(), "; ") + `; printf '%s' "$data_dir"`
	run := func() string {
		t.Helper()
		cmd := exec.Command("sh", "-c", script)
		cmd.Env = []string{"HOME=" + home + "/", "PATH=" + os.Getenv("PATH")}
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("settings commands failed: %v: %s", err, output)
		}
		return string(output)
	}

	dataDir := run()
	if dataDir != filepath.Join(home, ".openvscode-server") {
		t.Fatalf("data_dir=%q, want %q", dataDir, filepath.Join(home, ".openvscode-server"))
	}
	settingsFile := filepath.Join(dataDir, "data", "Machine", "settings.json")
	written, err := os.ReadFile(settingsFile)
	if err != nil {
		t.Fatalf("read machine settings: %v", err)
	}
	if string(written) != machineSettings {
		t.Fatalf("machine settings=%q, want %q", written, machineSettings)
	}
	info, err := os.Stat(filepath.Dir(settingsFile))
	if err != nil {
		t.Fatalf("stat machine settings directory: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("machine settings directory mode=%v, want 0700", info.Mode().Perm())
	}

	// A file that already exists is the user's: a restart keeps it as is.
	userSettings := "{\n  \"workbench.colorTheme\": \"Default Light Modern\"\n}\n"
	if err := os.WriteFile(settingsFile, []byte(userSettings), 0o644); err != nil {
		t.Fatalf("write user settings: %v", err)
	}
	run()
	kept, err := os.ReadFile(settingsFile)
	if err != nil {
		t.Fatalf("read machine settings after restart: %v", err)
	}
	if string(kept) != userSettings {
		t.Fatalf("restart rewrote the user's settings: %q", kept)
	}
	if _, err := os.Stat(settingsFile + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary settings file left behind: %v", err)
	}
}

func TestServiceRejectsRelativePaths(t *testing.T) {
	if _, err := NewService(&fakeRunner{}, Config{Binary: "openvscode-server"}); err == nil {
		t.Fatal("expected relative binary to be rejected")
	}
}

func TestServiceReportsCommandFailure(t *testing.T) {
	runner := &fakeRunner{result: executor.ExecutionResult{ExitCode: 127, Stderr: "binary missing"}}
	service, _ := NewService(runner, Config{})
	if _, err := service.Start(context.Background(), "sandbox-1"); err == nil || !strings.Contains(err.Error(), "binary missing") {
		t.Fatalf("start error=%v", err)
	}
	runner.err = errors.New("sandbox unavailable")
	if _, err := service.Status(context.Background(), "sandbox-1"); err == nil || !strings.Contains(err.Error(), "sandbox unavailable") {
		t.Fatalf("status error=%v", err)
	}
}
