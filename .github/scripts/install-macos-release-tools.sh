#!/usr/bin/env bash
set -euo pipefail

command -v openssl > /dev/null
tool_dir="$(mktemp -d "$RUNNER_TEMP/runnerfoundry-release-tools.XXXXXX")"
curl --fail --silent --show-error --location https://github.com/anchore/quill/releases/download/v0.7.1/quill_0.7.1_linux_amd64.tar.gz --output "$tool_dir/quill.tar.gz"
printf '%s  %s\n' e58c6f86378a22507c1123e24412afd4ee2d3bb32ebd94d6059827dc0c1b3fbf "$tool_dir/quill.tar.gz" | sha256sum --check -
tar -xzf "$tool_dir/quill.tar.gz" -C "$tool_dir" quill
curl --fail --silent --show-error --location https://github.com/deploymenttheory/go-macos-pkg/releases/download/v0.7.3/macospkg_0.7.3_linux_amd64.tar.gz --output "$tool_dir/macospkg.tar.gz"
printf '%s  %s\n' 23bcf8fafa3e6041e50cc3a96952d252dc65b721bc6d59faa5de9a9c77c91031 "$tool_dir/macospkg.tar.gz" | sha256sum --check -
tar -xzf "$tool_dir/macospkg.tar.gz" -C "$tool_dir" macospkg
curl --fail --silent --show-error --location https://github.com/indygreg/apple-platform-rs/releases/download/apple-codesign/0.29.0/apple-codesign-0.29.0-x86_64-unknown-linux-musl.tar.gz --output "$tool_dir/rcodesign.tar.gz"
printf '%s  %s\n' dbe85cedd8ee4217b64e9a0e4c2aef92ab8bcaaa41f20bde99781ff02e600002 "$tool_dir/rcodesign.tar.gz" | sha256sum --check -
tar -xzf "$tool_dir/rcodesign.tar.gz" -C "$tool_dir" --strip-components=1 apple-codesign-0.29.0-x86_64-unknown-linux-musl/rcodesign
printf '%s\n' "$tool_dir" >> "$GITHUB_PATH"
