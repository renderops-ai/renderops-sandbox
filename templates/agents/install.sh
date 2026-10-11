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
printf '%s\n' '#!/bin/sh' 'export DISABLE_AUTOUPDATER=1 USE_BUILTIN_RIPGREP=0' \
  'exec /opt/spawn/agents/bin/claude "$@"' > /usr/local/bin/claude
printf '%s\n' '#!/bin/sh' 'exec /opt/spawn/agents/bin/opencode "$@"' > /usr/local/bin/opencode
chmod 0755 /usr/local/bin/codex /usr/local/bin/claude /usr/local/bin/opencode
codex --version
claude --version
opencode --version
printf '{"image":"agents","version":1,"codex":"%s","claude":"%s","opencode":"%s"}\n' \
  "$CODEX_VERSION" "$CLAUDE_VERSION" "$OPENCODE_VERSION" > /etc/spawn/agents.json
npm cache clean --force
