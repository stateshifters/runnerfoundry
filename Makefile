APP_NAME := github-scaler
EXECUTABLE_NAME := github-scaler
BUILD_DIR := build
DIST_DIR := dist
METADATA_DIR := $(BUILD_DIR)/metadata
NOTICES_FILE := $(METADATA_DIR)/THIRD_PARTY_NOTICES.txt
SBOM_FILE := $(METADATA_DIR)/github-scaler-sbom.json
VERSION ?= dev
NFPM := go run github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.47.0
GOLANGCI_LINT_TOOLCHAIN := go1.26.4

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

.PHONY: fmt-check lint lint-config lint-fix vet test test-race coverage build smoke notices sbom package-deb prepare clean

fmt-check:
	gofmt -l . > gofmt.out
ifeq ($(OSFAMILY),Windows)
	@$$files = Get-Content gofmt.out; Remove-Item gofmt.out; if ($$files) { Write-Host 'Run gofmt on:'; $$files; exit 1 }
else
	@files=$$(cat gofmt.out); rm -f gofmt.out; if [ -n "$$files" ]; then echo 'Run gofmt on:'; echo "$$files"; exit 1; fi
endif

lint lint-config lint-fix: export GOTOOLCHAIN := $(GOLANGCI_LINT_TOOLCHAIN)

lint: lint-config
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run

lint-config:
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint config verify

lint-fix:
	@go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run --fix

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

coverage:
	go test ./... -coverprofile=coverage
	go tool cover -func=coverage > coverage.out
ifeq ($(OSFAMILY),Windows)
	@$$total = (Select-String -Path coverage.out '^total:').ToString().Split()[-1]; if ($$total -ne '100.0%') { Get-Content coverage.out; Write-Host "coverage total must be 100.0%, got $$total"; exit 1 }
else
	@total=$$(awk '/^total:/ { print $$3 }' coverage.out); if [ "$$total" != "100.0%" ]; then cat coverage.out; echo "coverage total must be 100.0%, got $$total"; exit 1; fi
endif

build: export GOOS := linux
build: export GOARCH := amd64
build: export CGO_ENABLED := 0
build:
	@$(MKDIR) "$(BUILD_DIR)/linux/amd64" > /dev/null
	go build -o "$(BUILD_DIR)/linux/amd64/$(EXECUTABLE_NAME)" ./cmd/github-scaler

smoke: build
	@go run ./cmd/github-scaler --config packaging/config/config.yaml 2> smoke.err; exit 0
ifeq ($(OSFAMILY),Windows)
	@$$message=Get-Content -Raw smoke.err; Remove-Item smoke.err -ErrorAction SilentlyContinue; if ($$message -notmatch 'github.app.app_id must be positive') { Write-Host $$message; exit 1 }
else
	@message=$$(cat smoke.err); rm -f smoke.err; if ! printf '%s' "$$message" | grep -q 'github.app.app_id must be positive'; then printf '%s\n' "$$message"; exit 1; fi
endif

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
