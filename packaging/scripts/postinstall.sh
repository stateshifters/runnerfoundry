#!/bin/sh
set -e

if ! getent group github-scaler >/dev/null 2>&1; then
	groupadd --system github-scaler
fi

if ! id github-scaler >/dev/null 2>&1; then
	useradd --system --gid github-scaler --home-dir /nonexistent --shell /usr/sbin/nologin github-scaler
fi

if [ -d /etc/github-scaler ]; then
	chown root:github-scaler /etc/github-scaler
	chmod 0750 /etc/github-scaler
fi

for config_file in /etc/github-scaler/config.yaml /etc/github-scaler/github-scaler.env; do
	if [ -e "$config_file" ]; then
		chown root:github-scaler "$config_file"
		chmod 0640 "$config_file"
	fi
done

if command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
fi
