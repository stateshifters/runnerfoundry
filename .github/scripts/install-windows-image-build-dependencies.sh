#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update
sudo apt-get install --no-install-recommends --yes ca-certificates curl

powershell_package=$(mktemp --suffix=.deb)
trap 'rm -f "$powershell_package"' EXIT

curl --fail --location --retry 3 \
  --output "$powershell_package" \
  https://github.com/PowerShell/PowerShell/releases/download/v7.6.6/powershell_7.6.6-1.deb_amd64.deb
echo "9585f38ab5a026c3fc0995486e26e12050777960fef47a22dca98b577c5d27a7  $powershell_package" | sha256sum --check
sudo apt-get install --no-install-recommends --yes \
  "$powershell_package" \
  kmod \
  ovmf \
  qemu-system-x86 \
  qemu-utils
