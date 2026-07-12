#!/bin/sh
set -e

root=${GITHUB_SCALER_ROOT:-}
state_dir="$root/var/lib/github-scaler"

if [ "${1:-}" = "purge" ] && [ ! -L "$state_dir" ] && [ -d "$state_dir" ]; then
	rm -f -- "$state_dir/state.db" "$state_dir/state.db-journal" "$state_dir/state.db-shm" "$state_dir/state.db-wal" "$state_dir/state.db.lock"
	rm -f -- "$state_dir"/state.db-*
	rmdir -- "$state_dir" 2>/dev/null || true
fi

if [ -z "$root" ] && command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
fi
