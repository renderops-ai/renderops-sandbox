#!/usr/bin/env bash
# Fails when a service imports a backend/internal package that its path filter
# in .github/workflows/deploy.yml doesn't cover. Without this, a change to that
# package merges green and never deploys (the deploy job is skipped).
#
# Usage (from the repository root): bash scripts/check-deploy-filters.sh
set -euo pipefail

workflow=.github/workflows/deploy.yml

# The patterns listed under one dorny/paths-filter key in deploy.yml (the key
# alone on its line; `key: ${{ ... }}` job outputs don't count).
filter_patterns() {
	awk -v key="$1:" '
		$1 == key && NF == 1 { inside = 1; next }
		inside && /^ *- / { gsub(/^ *- *'\''?|'\''? *$/, ""); print; next }
		inside { exit }
	' "$workflow"
}

# The backend/internal packages a set of commands is built from.
internal_deps() {
	(cd backend && go list -deps "$@") | grep '^backend/internal/' | sort -u
}

# check <filter key> <command package>...
check() {
	local key=$1
	shift
	local patterns missing=0
	patterns=$(filter_patterns "$key")
	if [ -z "$patterns" ]; then
		echo "deploy.yml has no '$key' filter" >&2
		return 1
	fi
	while read -r pkg; do
		local covered=0
		while read -r pattern; do
			local prefix=${pattern%/\*\*}
			if [ "$pkg" = "$prefix" ] || [[ "$pkg" == "$prefix"/* ]]; then
				covered=1
				break
			fi
		done <<<"$patterns"
		if [ "$covered" = 0 ]; then
			echo "deploy filter '$key' misses $pkg/** (imported by $*)" >&2
			missing=1
		fi
	done < <(internal_deps "$@")
	return "$missing"
}

status=0
check control_plane ./cmd/control-plane ./cmd/preview-gateway || status=1
check orchestrator ./cmd/orchestrator || status=1
check worker ./cmd/worker || status=1
if [ "$status" = 0 ]; then
	echo "deploy filters cover every backend/internal package each service imports"
fi
exit "$status"
