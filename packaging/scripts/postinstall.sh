#!/bin/sh
set -e

root=${RUNNERFOUNDRY_ROOT:-}
state_dir="$root/var/lib/runnerfoundry"
config_dir="$root/etc/runnerfoundry"

if [ -z "$root" ] && ! getent group runnerfoundry >/dev/null 2>&1; then
	groupadd --system runnerfoundry
fi

if [ -z "$root" ] && ! id runnerfoundry >/dev/null 2>&1; then
	useradd --system --gid runnerfoundry --home-dir /nonexistent --shell /usr/sbin/nologin runnerfoundry
fi

install -d -m 0750 "$state_dir"
if [ -z "$root" ]; then
	chown runnerfoundry:runnerfoundry "$state_dir"
fi

if [ -d "$config_dir" ]; then
	if [ -z "$root" ]; then
		chown root:runnerfoundry "$config_dir"
	fi
	chmod 0750 "$config_dir"
fi

for config_file in "$config_dir/config.yaml" "$config_dir/github-app.pem"; do
	if [ -e "$config_file" ]; then
		if [ -z "$root" ]; then
			chown root:runnerfoundry "$config_file"
		fi
		chmod 0640 "$config_file"
	fi
done

if [ -z "$root" ] && command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
fi
