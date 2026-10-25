APP_NAME := runnerfoundry
EXECUTABLE_NAME := runnerfoundry
BUILD_DIR := build
DIST_DIR := dist
METADATA_DIR := $(BUILD_DIR)/metadata
NOTICES_FILE := $(METADATA_DIR)/THIRD_PARTY_NOTICES.txt
SBOM_FILE := $(METADATA_DIR)/runnerfoundry-sbom.json
SOURCE_INFO_FILE := $(METADATA_DIR)/SOURCE.txt
VERSION ?= dev
NFPM := go run github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.47.0
QUILL ?= quill
MACOSPKG ?= macospkg
RCODESIGN ?= rcodesign
MACOS_PAYLOAD := $(BUILD_DIR)/macos/payload
MACOS_COMPONENT := $(BUILD_DIR)/macos/runnerfoundry-component.pkg
MACOS_PACKAGE := $(DIST_DIR)/runnerfoundry_$(VERSION)_darwin_arm64.pkg
GOLANGCI_LINT_TOOLCHAIN = go$(shell go list -m -f '{{.GoVersion}}')
E2E_POSTGRES_ROOT ?= $(abspath $(BUILD_DIR)/e2e/postgres)
E2E_POSTGRES_VERSION ?=
E2E_BACKEND ?= all
E2E_PACKAGES := ./internal/daemon
E2E_TOOL = $(abspath $(BUILD_DIR)/e2e/e2e-tool$(E2E_TOOL_EXTENSION))

ifeq ($(OS),Windows_NT)
OSFAMILY := Windows
E2E_TOOL_EXTENSION := .exe
E2E_TOOL_INVOKE = & "$(E2E_TOOL)"
override SHELL := pwsh.exe
.SHELLFLAGS := -NoLogo -NoProfile -Command
MKDIR := New-Item -ItemType Directory -Force -Path
REMOVE_DIR := Remove-Item -Recurse -Force -ErrorAction SilentlyContinue -Path
else
OSFAMILY := Unix
E2E_TOOL_INVOKE = "$(E2E_TOOL)"
override SHELL := bash
.SHELLFLAGS := -c
MKDIR := mkdir -p
REMOVE_DIR := rm -rf
endif

.PHONY: fmt-check lint lint-config lint-fix lint-no-nolint vet test coverage race e2e-tool prepare-e2e test-e2e build build-macos notices sbom sbom-license source-info package-deb package-macos prepare clean

fmt-check:
	gofmt -l . > gofmt.out
ifeq ($(OSFAMILY),Windows)
	@$$files = Get-Content gofmt.out; Remove-Item gofmt.out; if ($$files) { Write-Host 'Run gofmt on:'; $$files; exit 1 }
else
	@files=$$(cat gofmt.out); rm -f gofmt.out; if [ -n "$$files" ]; then echo 'Run gofmt on:'; echo "$$files"; exit 1; fi
endif

lint lint-config lint-fix: export GOTOOLCHAIN := $(GOLANGCI_LINT_TOOLCHAIN)

lint: lint-no-nolint lint-config
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run --build-tags=e2e
ifeq ($(OSFAMILY),Windows)
	@$$violations = go tool github.com/boyter/scc/v3 --by-file --include-ext go --no-gen --format csv . | ConvertFrom-Csv | Where-Object { [int]$$_.Complexity -gt 100 }; if ($$violations) { $$violations | ForEach-Object { Write-Host "$$($$_.Provider) complexity=$$($$_.Complexity)" }; exit 1 }
else
	@violations=$$(go tool github.com/boyter/scc/v3 --by-file --include-ext go --no-gen --format csv . | awk -F, 'NR > 1 && $$8 > 100 { print $$2 " complexity=" $$8 }'); if [ -n "$$violations" ]; then echo "$$violations"; exit 1; fi
endif

lint-config:
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint config verify

lint-no-nolint:
ifeq ($(OSFAMILY),Windows)
	@$$violations = git grep -n '//nolint' -- '*.go'; if ($$violations) { $$violations; Write-Host '//nolint directives are forbidden'; exit 1 }; exit 0
else
	@if violations=$$(git grep -n '//nolint' -- '*.go'); then echo "$$violations"; echo '//nolint directives are forbidden'; exit 1; fi
endif

lint-fix:
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run --fix

vet:
	go vet ./...
	go -C tools/e2e vet ./...

test:
	go test ./... -count=1 -timeout=60s
	go -C tools/e2e test ./... -count=1 -timeout=60s

coverage:
	go test ./... '-coverprofile=coverage.out' -count=1 -timeout=60s
ifeq ($(OSFAMILY),Windows)
	@$$total = go tool cover '-func=coverage.out' | Select-Object -Last 1; $$value = [double](($$total -split '\s+')[-1].TrimEnd('%')); if ($$value -lt 100) { Write-Host "Meaningful Go test coverage is $$value%, want 100%"; exit 1 }; Write-Host $$total
else
	@total=$$(go tool cover '-func=coverage.out' | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); awk -v coverage="$$total" 'BEGIN { if (coverage < 100) { printf "Meaningful Go test coverage is %s%%, want 100%%\n", coverage; exit 1 } }'; go tool cover '-func=coverage.out' | tail -n 1
endif

race:
	go test -race ./... -count=1 -timeout=60s

e2e-tool:
ifeq ($(OSFAMILY),Windows)
	@$(MKDIR) "$(BUILD_DIR)/e2e" | Out-Null
else
	@$(MKDIR) "$(BUILD_DIR)/e2e"
endif
	@go -C tools/e2e build -o "$(E2E_TOOL)" .

prepare-e2e: e2e-tool
	@$(E2E_TOOL_INVOKE) prepare --repository-root "$(CURDIR)" --postgres-root "$(E2E_POSTGRES_ROOT)" --postgres-version "$(E2E_POSTGRES_VERSION)"

test-e2e: e2e-tool
	@$(E2E_TOOL_INVOKE) test --repository-root "$(CURDIR)" --postgres-root "$(E2E_POSTGRES_ROOT)" --postgres-version "$(E2E_POSTGRES_VERSION)" --backend "$(E2E_BACKEND)" --packages "$(E2E_PACKAGES)"

build: export GOOS := linux
build: export GOARCH := amd64
build: export CGO_ENABLED := 0
build:
	@$(MKDIR) "$(BUILD_DIR)/linux/amd64" > /dev/null
	go build -ldflags "-X github.com/stateshifters/runnerfoundry/internal/buildinfo.version=$(VERSION)" -o "$(BUILD_DIR)/linux/amd64/$(EXECUTABLE_NAME)" ./cmd/runnerfoundry

build-macos: export GOOS := darwin
build-macos: export GOARCH := arm64
build-macos: export CGO_ENABLED := 0
build-macos:
	@$(MKDIR) "$(BUILD_DIR)/darwin/arm64"
	go build -ldflags "-X github.com/stateshifters/runnerfoundry/internal/buildinfo.version=$(VERSION)" -o "$(BUILD_DIR)/darwin/arm64/$(EXECUTABLE_NAME)" ./cmd/runnerfoundry

notices:
	@$(MKDIR) "$(METADATA_DIR)" > /dev/null
	go run github.com/google/go-licenses@v1.6.0 report --ignore github.com/stateshifters/runnerfoundry ./... > "$(NOTICES_FILE)"

sbom:
	@$(MKDIR) "$(METADATA_DIR)" > /dev/null
	go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.9.0 mod -licenses -json -output "$(SBOM_FILE)"

sbom-license: sbom
ifeq ($(OSFAMILY),Windows)
	@$$bom = Get-Content -Raw "$(SBOM_FILE)" | ConvertFrom-Json; $$choice = [pscustomobject]@{ license = [pscustomobject]@{ id = 'AGPL-3.0-or-later' } }; $$bom.metadata.component | Add-Member -Force -NotePropertyName licenses -NotePropertyValue @($$choice); $$bom | ConvertTo-Json -Depth 100 | Set-Content "$(SBOM_FILE)"
else
	@awk 'BEGIN { added = 0 } !added && /^    "component": \{$$/ { print; print "      \"licenses\": [{\"license\": {\"id\": \"AGPL-3.0-or-later\"}}],"; added = 1; next } { print } END { if (!added) exit 1 }' "$(SBOM_FILE)" > "$(SBOM_FILE).tmp" && mv "$(SBOM_FILE).tmp" "$(SBOM_FILE)"
endif

source-info:
	@$(MKDIR) "$(METADATA_DIR)" > /dev/null
ifeq ($(OSFAMILY),Windows)
	@$$content = "RunnerFoundry $(VERSION)`nCorresponding source: https://github.com/stateshifters/runnerfoundry/tree/v$(VERSION)`n"; Set-Content -NoNewline -Path "$(SOURCE_INFO_FILE)" -Value $$content
else
	@printf 'RunnerFoundry %s\nCorresponding source: https://github.com/stateshifters/runnerfoundry/tree/v%s\n' "$(VERSION)" "$(VERSION)" > "$(SOURCE_INFO_FILE)"
endif

package-deb: export VERSION := $(VERSION)
package-deb: build notices sbom-license source-info
	@$(MKDIR) "$(DIST_DIR)" > /dev/null
	$(NFPM) package --packager deb --config nfpm.yaml --target "$(DIST_DIR)"

package-macos: build-macos notices sbom-license source-info
ifeq ($(OSFAMILY),Windows)
	@Write-Error 'Signed macOS packaging requires the Linux release environment'; exit 1
else
	@test -n "$${QUILL_SIGN_P12:?Application signing certificate is required}"
	@test -n "$${QUILL_SIGN_PASSWORD:?Application signing password is required}"
	@test -n "$${MACOS_INSTALLER_P12_PATH:?Installer signing certificate path is required}"
	@test -n "$${MACOSPKG_P12_PASSWORD:?Installer signing password is required}"
	@test -n "$${APPLE_KEY_ID:?Notarization key ID is required}"
	@test -n "$${APPLE_ISSUER_ID:?Notarization issuer ID is required}"
	@test -n "$${APPLE_PRIVATE_KEY_PATH:?Notarization private key path is required}"
	@test -n "$${MACOS_TEAM_ID:?Apple team ID is required}"
	@$(REMOVE_DIR) "$(BUILD_DIR)/macos"
	install -d -m 0755 "$(MACOS_PAYLOAD)/usr" "$(MACOS_PAYLOAD)/usr/local" "$(MACOS_PAYLOAD)/usr/local/bin" "$(MACOS_PAYLOAD)/usr/local/share" "$(MACOS_PAYLOAD)/usr/local/share/runnerfoundry" "$(MACOS_PAYLOAD)/Library/LaunchDaemons" "$(DIST_DIR)"
	install -m 0755 "$(BUILD_DIR)/darwin/arm64/$(EXECUTABLE_NAME)" "$(MACOS_PAYLOAD)/usr/local/bin/$(EXECUTABLE_NAME)"
	install -m 0644 packaging/macos/config.example.yaml LICENSE COPYRIGHT "$(NOTICES_FILE)" "$(SBOM_FILE)" "$(SOURCE_INFO_FILE)" "$(MACOS_PAYLOAD)/usr/local/share/runnerfoundry/"
	install -m 0644 packaging/macos/com.stateshifterlabs.runnerfoundry.plist "$(MACOS_PAYLOAD)/Library/LaunchDaemons/"
	$(QUILL) sign "$(MACOS_PAYLOAD)/usr/local/bin/$(EXECUTABLE_NAME)"
	$(MACOSPKG) build "$(MACOS_PAYLOAD)" "$(MACOS_COMPONENT)" --identifier com.stateshifterlabs.runnerfoundry --version "$(VERSION)" --install-location / --scripts packaging/macos/scripts
	$(MACOSPKG) product "$(MACOS_PACKAGE)" --package "$(MACOS_COMPONENT)" --identifier com.stateshifterlabs.runnerfoundry --version "$(VERSION)" --host-architectures arm64 --min-os-version 13.0 --sign-p12 "$$MACOS_INSTALLER_P12_PATH"
	@set -eu; umask 077; \
		notary_key_file=$$(mktemp); \
		trap 'rm -f -- "$$notary_key_file"' EXIT; \
		$(RCODESIGN) encode-app-store-connect-api-key -o "$$notary_key_file" "$$APPLE_ISSUER_ID" "$$APPLE_KEY_ID" "$$APPLE_PRIVATE_KEY_PATH"; \
		$(RCODESIGN) notary-submit --api-key-file "$$notary_key_file" --staple --max-wait-seconds 1800 "$(MACOS_PACKAGE)"
	$(MACOSPKG) verify "$(MACOS_PACKAGE)" --team-id "$$MACOS_TEAM_ID" --require-developer-id --require-stapled --online
endif

prepare:
	@$(REMOVE_DIR) "$(DIST_DIR)"
	$(MAKE) package-deb package-macos VERSION="$(VERSION)"

clean:
	@$(REMOVE_DIR) "$(BUILD_DIR)"; $(REMOVE_DIR) "$(DIST_DIR)"; $(REMOVE_DIR) coverage; $(REMOVE_DIR) coverage.out
