#!/bin/sh
set -e

root=${GITHUB_SCALER_ROOT:-}
state_dir="$root/var/lib/github-scaler"
config_dir="$root/etc/github-scaler"

if [ -z "$root" ] && ! getent group github-scaler >/dev/null 2>&1; then
	groupadd --system github-scaler
fi

if [ -z "$root" ] && ! id github-scaler >/dev/null 2>&1; then
	useradd --system --gid github-scaler --home-dir /nonexistent --shell /usr/sbin/nologin github-scaler
fi

install -d -m 0750 "$state_dir"
if [ -z "$root" ]; then
	chown github-scaler:github-scaler "$state_dir"
fi

if [ -d "$config_dir" ]; then
	if [ -z "$root" ]; then
		chown root:github-scaler "$config_dir"
	fi
	chmod 0750 "$config_dir"
fi

for config_file in "$config_dir/config.yaml" "$config_dir/github-app.pem"; do
	if [ -e "$config_file" ]; then
		if [ -z "$root" ]; then
			chown root:github-scaler "$config_file"
		fi
		chmod 0640 "$config_file"
	fi
done

if [ -z "$root" ] && command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
fi
