APP_NAME := runnerfoundry
EXECUTABLE_NAME := runnerfoundry
BUILD_DIR := build
DIST_DIR := dist
METADATA_DIR := $(BUILD_DIR)/metadata
NOTICES_FILE := $(METADATA_DIR)/THIRD_PARTY_NOTICES.txt
SBOM_FILE := $(METADATA_DIR)/runnerfoundry-sbom.json
VERSION ?= dev
NFPM := go run github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.47.0
GOLANGCI_LINT_TOOLCHAIN = go$(shell go list -m -f '{{.GoVersion}}')

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

.PHONY: fmt-check lint lint-config lint-fix lint-no-nolint vet test coverage race build notices sbom package-deb prepare clean

fmt-check:
	gofmt -l . > gofmt.out
ifeq ($(OSFAMILY),Windows)
	@$$files = Get-Content gofmt.out; Remove-Item gofmt.out; if ($$files) { Write-Host 'Run gofmt on:'; $$files; exit 1 }
else
	@files=$$(cat gofmt.out); rm -f gofmt.out; if [ -n "$$files" ]; then echo 'Run gofmt on:'; echo "$$files"; exit 1; fi
endif

lint lint-config lint-fix: export GOTOOLCHAIN := $(GOLANGCI_LINT_TOOLCHAIN)

lint: lint-no-nolint lint-config
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run
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

build: export GOOS := linux
build: export GOARCH := amd64
build: export CGO_ENABLED := 0
build:
	@$(MKDIR) "$(BUILD_DIR)/linux/amd64" > /dev/null
	go build -ldflags "-X github.com/meza/RunnerFoundry/internal/buildinfo.version=$(VERSION)" -o "$(BUILD_DIR)/linux/amd64/$(EXECUTABLE_NAME)" ./cmd/runnerfoundry

notices:
	@$(MKDIR) "$(METADATA_DIR)" > /dev/null
	go run github.com/google/go-licenses@v1.6.0 report ./... > "$(NOTICES_FILE)"

sbom:
	@$(MKDIR) "$(METADATA_DIR)" > /dev/null
	go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.9.0 mod -licenses -json -output "$(SBOM_FILE)"

package-deb: export VERSION := $(VERSION)
package-deb: build notices sbom
	@$(REMOVE_DIR) "$(DIST_DIR)"
	@$(MKDIR) "$(DIST_DIR)" > /dev/null
	$(NFPM) package --packager deb --config nfpm.yaml --target "$(DIST_DIR)"

prepare:
	$(MAKE) package-deb VERSION="$(VERSION)"

clean:
	@$(REMOVE_DIR) "$(BUILD_DIR)"; $(REMOVE_DIR) "$(DIST_DIR)"; $(REMOVE_DIR) coverage; $(REMOVE_DIR) coverage.out
