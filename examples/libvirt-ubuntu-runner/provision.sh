#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_RUNNER_SHA256:?GITHUB_RUNNER_SHA256 must be set}"
: "${GITHUB_RUNNER_VERSION:?GITHUB_RUNNER_VERSION must be set}"

export DEBIAN_FRONTEND=noninteractive

cloud-init status --wait
apt-get update
apt-get install --no-install-recommends --yes ca-certificates curl dmidecode

id runner >/dev/null 2>&1 || useradd --create-home --shell /bin/bash runner
runner_sudoers=/etc/sudoers.d/90-runner
printf 'runner ALL=(ALL:ALL) NOPASSWD:ALL\n' > "$runner_sudoers"
chmod 0440 "$runner_sudoers"
install --directory --owner=root --group=root --mode=0755 /opt/actions-runner-template

runner_archive="/tmp/actions-runner-linux-x64-${GITHUB_RUNNER_VERSION}.tar.gz"
runner_url="https://github.com/actions/runner/releases/download/v${GITHUB_RUNNER_VERSION}/actions-runner-linux-x64-${GITHUB_RUNNER_VERSION}.tar.gz"

curl --fail --location --silent --show-error --output "$runner_archive" "$runner_url"
printf '%s  %s\n' "$GITHUB_RUNNER_SHA256" "$runner_archive" | sha256sum --check --strict -
tar --extract --gzip --file="$runner_archive" --directory=/opt/actions-runner-template
rm -f "$runner_archive"

/opt/actions-runner-template/bin/installdependencies.sh
chown -R root:root /opt/actions-runner-template

install --owner=root --group=root --mode=0755 \
  /tmp/runnerfoundry-jit-launcher \
  /usr/local/sbin/runnerfoundry-jit-launcher
install --owner=root --group=root --mode=0644 \
  /tmp/runnerfoundry-jit-launcher.service \
  /etc/systemd/system/runnerfoundry-jit-launcher.service

systemctl daemon-reload
systemctl enable runnerfoundry-jit-launcher.service

apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
