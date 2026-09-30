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
GOLANGCI_LINT_TOOLCHAIN = go$(shell go list -m -f '{{.GoVersion}}')
E2E_POSTGRES_ROOT ?= $(abspath $(BUILD_DIR)/e2e/postgres)
E2E_POSTGRES_VERSION ?=
E2E_BACKEND ?= all
E2E_PACKAGES := ./internal/daemon

ifeq ($(OS),Windows_NT)
OSFAMILY := Windows
override SHELL := pwsh.exe
.SHELLFLAGS := -NoLogo -NoProfile -Command
MKDIR := New-Item -ItemType Directory -Force -Path
REMOVE_DIR := Remove-Item -Recurse -Force -ErrorAction SilentlyContinue -Path
else
OSFAMILY := Unix
override SHELL := bash
.SHELLFLAGS := -c
MKDIR := mkdir -p
REMOVE_DIR := rm -rf
endif

.PHONY: fmt-check lint lint-config lint-fix lint-no-nolint vet test coverage race prepare-e2e test-e2e build notices sbom sbom-license source-info package-deb prepare clean

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

test:
	go test ./... -count=1 -timeout=60s

coverage:
	go test ./... '-coverprofile=coverage.out' -count=1 -timeout=60s
ifeq ($(OSFAMILY),Windows)
	@$$total = go tool cover '-func=coverage.out' | Select-Object -Last 1; $$value = [double](($$total -split '\s+')[-1].TrimEnd('%')); if ($$value -lt 100) { Write-Host "Meaningful Go test coverage is $$value%, want 100%"; exit 1 }; Write-Host $$total
else
	@total=$$(go tool cover '-func=coverage.out' | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); awk -v coverage="$$total" 'BEGIN { if (coverage < 100) { printf "Meaningful Go test coverage is %s%%, want 100%%\n", coverage; exit 1 } }'; go tool cover '-func=coverage.out' | tail -n 1
endif

race:
	go test -race ./... -count=1 -timeout=60s

prepare-e2e:
ifeq ($(OSFAMILY),Windows)
	@$$env:E2E_POSTGRES_ROOT = '$(E2E_POSTGRES_ROOT)'; $$env:E2E_POSTGRES_VERSION = '$(E2E_POSTGRES_VERSION)'; & ./scripts/prepare-e2e-postgres.ps1; if ($$LASTEXITCODE -ne 0) { exit $$LASTEXITCODE }
else
	@E2E_POSTGRES_ROOT='$(E2E_POSTGRES_ROOT)' E2E_POSTGRES_VERSION='$(E2E_POSTGRES_VERSION)' bash ./scripts/prepare-e2e-postgres.sh
endif

test-e2e:
ifeq ($(OSFAMILY),Windows)
	@$$env:E2E_POSTGRES_ROOT = '$(E2E_POSTGRES_ROOT)'; $$env:E2E_POSTGRES_VERSION = '$(E2E_POSTGRES_VERSION)'; $$env:E2E_BACKEND = '$(E2E_BACKEND)'; $$env:E2E_PACKAGES = '$(E2E_PACKAGES)'; & ./scripts/test-e2e.ps1; if ($$LASTEXITCODE -ne 0) { exit $$LASTEXITCODE }
else
	@E2E_POSTGRES_ROOT='$(E2E_POSTGRES_ROOT)' E2E_POSTGRES_VERSION='$(E2E_POSTGRES_VERSION)' E2E_BACKEND='$(E2E_BACKEND)' E2E_PACKAGES='$(E2E_PACKAGES)' bash ./scripts/test-e2e.sh
endif

build: export GOOS := linux
build: export GOARCH := amd64
build: export CGO_ENABLED := 0
build:
	@$(MKDIR) "$(BUILD_DIR)/linux/amd64" > /dev/null
	go build -ldflags "-X github.com/stateshifters/runnerfoundry/internal/buildinfo.version=$(VERSION)" -o "$(BUILD_DIR)/linux/amd64/$(EXECUTABLE_NAME)" ./cmd/runnerfoundry

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
	@$(REMOVE_DIR) "$(DIST_DIR)"
	@$(MKDIR) "$(DIST_DIR)" > /dev/null
	$(NFPM) package --packager deb --config nfpm.yaml --target "$(DIST_DIR)"

prepare:
	$(MAKE) package-deb VERSION="$(VERSION)"

clean:
	@$(REMOVE_DIR) "$(BUILD_DIR)"; $(REMOVE_DIR) "$(DIST_DIR)"; $(REMOVE_DIR) coverage; $(REMOVE_DIR) coverage.out
