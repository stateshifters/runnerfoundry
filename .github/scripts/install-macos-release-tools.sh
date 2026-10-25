#!/usr/bin/env bash
set -euo pipefail

command -v openssl > /dev/null
tool_dir="$(mktemp -d "$RUNNER_TEMP/runnerfoundry-release-tools.XXXXXX")"
curl --fail --silent --show-error --location "https://github.com/anchore/quill/releases/download/v${QUILL_VERSION}/quill_${QUILL_VERSION}_linux_amd64.tar.gz" --output "$tool_dir/quill.tar.gz"
tar -xzf "$tool_dir/quill.tar.gz" -C "$tool_dir" quill
curl --fail --silent --show-error --location "https://github.com/deploymenttheory/go-macos-pkg/releases/download/v${MACOSPKG_VERSION}/macospkg_${MACOSPKG_VERSION}_linux_amd64.tar.gz" --output "$tool_dir/macospkg.tar.gz"
tar -xzf "$tool_dir/macospkg.tar.gz" -C "$tool_dir" macospkg
curl --fail --silent --show-error --location "https://github.com/indygreg/apple-platform-rs/releases/download/apple-codesign/${RCODESIGN_VERSION}/apple-codesign-${RCODESIGN_VERSION}-x86_64-unknown-linux-musl.tar.gz" --output "$tool_dir/rcodesign.tar.gz"
tar -xzf "$tool_dir/rcodesign.tar.gz" -C "$tool_dir" --strip-components=1 "apple-codesign-${RCODESIGN_VERSION}-x86_64-unknown-linux-musl/rcodesign"
printf '%s\n' "$tool_dir" >> "$GITHUB_PATH"
