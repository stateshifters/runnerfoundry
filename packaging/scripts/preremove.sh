#!/bin/sh
set -e

root=${RUNNERFOUNDRY_ROOT:-}

if [ -z "$root" ] && [ "$1" = "remove" ] && command -v systemctl >/dev/null 2>&1; then
	systemctl stop runnerfoundry.service || true
fi
