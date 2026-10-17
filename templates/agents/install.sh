#!/bin/sh
# Build-time only. No login, user configuration, API keys or startup installs.
set -eu
: "${CODEX_VERSION:?exact Codex version required}"
: "${CLAUDE_VERSION:?exact Claude version required}"
: "${OPENCODE_VERSION:?exact OpenCode version required}"
for version in "$CODEX_VERSION" "$CLAUDE_VERSION" "$OPENCODE_VERSION"; do
  case "$version" in
    ''|*[!0-9.]*|.*|*.) echo 'agent versions must be numeric release versions' >&2; exit 1 ;;
  esac
done

# Fail instead of producing an image with unsupported Node or missing agents.
node -e 'if (+process.versions.node.split(".")[0] < 22) process.exit(1)'
npm install --global --prefix /opt/spawn/agents --no-audit --no-fund \
  "@openai/codex@$CODEX_VERSION" \
  "@anthropic-ai/claude-code@$CLAUDE_VERSION" \
  "opencode-ai@$OPENCODE_VERSION"

# Docker ENV is not inherited when its filesystem becomes a microVM disk.
# These wrappers carry the required settings into the actual guest process.
mkdir -p /usr/local/bin /etc/spawn
printf '%s\n' '#!/bin/sh' 'exec /opt/spawn/agents/bin/codex "$@"' > /usr/local/bin/codex
# Sandbox commands run as root inside the microVM. Claude Code refuses
# --dangerously-skip-permissions as root unless told it is in a sandbox; the
# microVM is that sandbox.
printf '%s\n' '#!/bin/sh' 'export DISABLE_AUTOUPDATER=1 USE_BUILTIN_RIPGREP=0 IS_SANDBOX=1' \
  'exec /opt/spawn/agents/bin/claude "$@"' > /usr/local/bin/claude
printf '%s\n' '#!/bin/sh' 'exec /opt/spawn/agents/bin/opencode "$@"' > /usr/local/bin/opencode
chmod 0755 /usr/local/bin/codex /usr/local/bin/claude /usr/local/bin/opencode
codex --version
claude --version
opencode --version

# Persistent agent sessions: the spawn CLI runs the agent inside tmux, so the
# user can close their terminal while it works and reattach later. This config
# keeps tmux out of the way: no status bar and no prefix key, so every key goes
# to the agent. Installed here, not in Dockerfile.rootfs, so that changing it
# never rebuilds and rolls out the base rootfs (deploy.yml's rootfs filter).
apk add --no-cache tmux
cat > /etc/spawn/tmux.conf <<'EOF'
# /etc/spawn/tmux.conf: the spawn CLI's persistent agent session.
# The user detaches by closing their terminal; tmux itself stays invisible.

# No prefix key: C-b and every other key go to the agent.
set -g prefix None
set -g prefix2 None
unbind-key C-b

set -g status off
set -s escape-time 0
set -g history-limit 50000
set -g remain-on-exit off

# Keys: send modified keys (Shift+Enter and friends) as extended keys.
set -s extended-keys always
set -as terminal-features ',*:extkeys:RGB:clipboard'

# Colour: tmux's own terminal type inside, true colour to the outer terminal.
set -g default-terminal tmux-256color
set -as terminal-overrides ',*:Tc'

# Mouse events go to the agent when it asks for them; focus events, the
# clipboard (OSC 52) and other escape sequences reach the user's terminal.
set -g mouse on
set -s focus-events on
set -s set-clipboard on
set -g allow-passthrough on
EOF
chmod 0644 /etc/spawn/tmux.conf
tmux -V
# source-file exits non-zero on any config error. The server has no sessions,
# so it exits by itself afterwards.
tmux -f /dev/null -L spawn-build start-server \; source-file /etc/spawn/tmux.conf
# Agent state lives in the default homes (/root/.claude, /root/.claude.json,
# /root/.codex) on the sandbox's writable disk, so it survives pause/resume.
# Projects go in /workspace. No credentials are baked in: callers pass
# ANTHROPIC_API_KEY / OPENAI_API_KEY as sandbox env at create time.
mkdir -p /workspace /root/.claude /root/.codex
printf '{"image":"agents","version":1,"codex":"%s","claude":"%s","opencode":"%s"}\n' \
  "$CODEX_VERSION" "$CLAUDE_VERSION" "$OPENCODE_VERSION" > /etc/spawn/agents.json
npm cache clean --force
