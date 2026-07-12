#!/bin/sh
set -e

root=${GITHUB_SCALER_ROOT:-}

if [ -z "$root" ] && [ "$1" = "remove" ] && command -v systemctl >/dev/null 2>&1; then
	systemctl stop github-scaler.service || true
fi
