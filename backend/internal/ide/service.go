package ide

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"backend/internal/executor"
	"backend/internal/plane"
)

const DefaultPort uint16 = 3001

// machineSettings are the defaults a sandbox's IDE starts with, so it opens
// ready to use instead of in first-run setup. OpenVSCode keeps the browser's
// own User settings in the browser (IndexedDB), so the only settings file in
// the sandbox it reads is the server's Machine settings ("Remote" in the
// Settings editor). Those apply every setting scope except application:
// workspace trust and telemetry are application settings, so startCommand
// turns them off with server flags instead.
const machineSettings = `{
  "workbench.startupEditor": "none",
  "workbench.welcomePage.walkthroughs.openOnInstall": false,
  "workbench.tips.enabled": false,
  "workbench.colorTheme": "Default Dark Modern",
  "extensions.ignoreRecommendations": true,
  "chat.disableAIFeatures": true,
  "git.openRepositoryInParentFolders": "always",
  "git.autofetch": false
}
`

type CommandRunner interface {
	Exec(context.Context, string, string, int) (executor.ExecutionResult, error)
}

type Manager interface {
	Start(context.Context, string) (Instance, error)
	Status(context.Context, string) (Instance, error)
	Stop(context.Context, string) error
}

type Config struct {
	Binary    string
	Workspace string
	Port      uint16
}

type Instance = plane.IDEInstance

type Service struct {
	runner CommandRunner
	config Config
}

func NewService(runner CommandRunner, config Config) (*Service, error) {
	if runner == nil {
		return nil, errors.New("IDE command runner is required")
	}
	if config.Binary == "" {
		config.Binary = "/opt/openvscode-server/bin/openvscode-server"
	}
	if config.Workspace == "" {
		config.Workspace = "/workspace"
	}
	if config.Port == 0 {
		config.Port = DefaultPort
	}
	if !filepath.IsAbs(config.Binary) || !filepath.IsAbs(config.Workspace) {
		return nil, errors.New("IDE binary and workspace must be absolute paths")
	}
	return &Service{runner: runner, config: config}, nil
}

func (s *Service) Start(ctx context.Context, sandboxID string) (Instance, error) {
	result, err := s.runner.Exec(ctx, sandboxID, s.startCommand(), 45)
	if err != nil {
		return Instance{}, fmt.Errorf("start IDE: %w", err)
	}
	if result.ExitCode != 0 {
		return Instance{}, fmt.Errorf("start IDE: %s", commandError(result))
	}
	return Instance{Port: s.config.Port, State: "ready"}, nil
}

func (s *Service) Status(ctx context.Context, sandboxID string) (Instance, error) {
	result, err := s.runner.Exec(ctx, sandboxID, s.statusCommand(), 5)
	if err != nil {
		return Instance{}, fmt.Errorf("check IDE status: %w", err)
	}
	state := strings.TrimSpace(result.Stdout)
	if state != "ready" {
		state = "stopped"
	}
	return Instance{Port: s.config.Port, State: state}, nil
}

func (s *Service) Stop(ctx context.Context, sandboxID string) error {
	result, err := s.runner.Exec(ctx, sandboxID, s.stopCommand(), 10)
	if err != nil {
		return fmt.Errorf("stop IDE: %w", err)
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("stop IDE: %s", commandError(result))
	}
	return nil
}

func (s *Service) startCommand() string {
	port := strconv.Itoa(int(s.config.Port))
	command := strings.Join([]string{
		"set -eu",
		"state_dir=/var/lib/renderops/ide",
		"pid_file=$state_dir/openvscode.pid",
		"log_file=$state_dir/openvscode.log",
		"mkdir -p \"$state_dir\" " + shellQuote(s.config.Workspace),
		"if [ -s \"$pid_file\" ] && kill -0 \"$(cat \"$pid_file\")\" 2>/dev/null; then exit 0; fi",
		"rm -f \"$pid_file\"",
		"[ -x " + shellQuote(s.config.Binary) + " ] || { printf 'OpenVSCode is not installed at %s\\n' " + shellQuote(s.config.Binary) + " >&2; exit 127; }",
		strings.Join(machineSettingsCommands(), "; "),
		// --default-folder opens the workspace (the server ignores a
		// positional folder). Workspace trust and telemetry are application
		// settings, which Machine settings cannot set, so they are flags.
		"nohup " + shellQuote(s.config.Binary) +
			" --host 0.0.0.0 --port " + port +
			" --without-connection-token" +
			" --server-data-dir \"$data_dir\"" +
			" --default-folder " + shellQuote(s.config.Workspace) +
			" --disable-workspace-trust" +
			" --telemetry-level off" +
			" >\"$log_file\" 2>&1 </dev/null & pid=$!",
		"printf '%s\\n' \"$pid\" >\"$pid_file\"",
		"i=0; while [ \"$i\" -lt 30 ]; do wget -q -O /dev/null http://127.0.0.1:" + port + "/ && exit 0; kill -0 \"$pid\" 2>/dev/null || break; i=$((i + 1)); sleep 1; done",
		"tail -n 20 \"$log_file\" >&2 || true",
		"exit 1",
	}, "; ")
	return "( " + command + " )"
}

// machineSettingsCommands set data_dir to OpenVSCode's server data directory
// and write machineSettings there unless the sandbox already has Machine
// settings. Theme and other changes made in the IDE are saved to that file,
// so an existing one is the user's and is never rewritten.
func machineSettingsCommands() []string {
	encoded := base64.StdEncoding.EncodeToString([]byte(machineSettings))
	return []string{
		// The server's default, ~/.openvscode-server, where ~ is $HOME or, if
		// that is unset or empty, root's home. The start command passes it
		// with --server-data-dir so the server reads the file written here.
		"home=${HOME:-/root}",
		"data_dir=${home%/}/.openvscode-server",
		"settings_dir=$data_dir/data/Machine",
		"settings_file=$settings_dir/settings.json",
		// 0700, as the server creates its own data directories.
		"(umask 077 && mkdir -p \"$settings_dir\")",
		"if [ ! -e \"$settings_file\" ]; then printf '%s' " + shellQuote(encoded) + " | base64 -d >\"$settings_file.tmp\"; mv -f \"$settings_file.tmp\" \"$settings_file\"; fi",
	}
}

func (s *Service) statusCommand() string {
	return `pid_file=/var/lib/renderops/ide/openvscode.pid; if [ -s "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then printf ready; else printf stopped; fi`
}

func (s *Service) stopCommand() string {
	return `pid_file=/var/lib/renderops/ide/openvscode.pid; if [ -s "$pid_file" ]; then pid=$(cat "$pid_file"); kill "$pid" 2>/dev/null || true; i=0; while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 10 ]; do i=$((i + 1)); sleep 1; done; kill -9 "$pid" 2>/dev/null || true; fi; rm -f "$pid_file"`
}

func commandError(result executor.ExecutionResult) string {
	if message := strings.TrimSpace(result.Stderr); message != "" {
		return message
	}
	return fmt.Sprintf("command exited with status %d", result.ExitCode)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
