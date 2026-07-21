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

.PHONY: fmt-check lint lint-config lint-fix lint-no-nolint vet test test-integration test-race coverage build smoke notices sbom package-deb prepare clean

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
	go test ./...

test-integration:
	go test -tags=integration ./...

test-race:
	go test -tags=integration -race ./...

coverage:
	go test -tags=integration -coverpkg=./... ./... -coverprofile=coverage
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
	go test ./internal/daemon -run TestClusteredLifecycleSmoke -count=1
	go test ./cmd/github-scaler -run 'Test(MigrationStatusWritesMachineReadableStdout|MigrationUpAndDownRemainSilent|ExecuteStoreMigrationDispatchesTypedActions)' -count=1
	go test -tags=integration ./internal/cluster -run 'TestPostgreSQL(AggregatesSharedAndDisjointHostCapacity|PreservesGlobalFIFOWithinHostLimits|LeaderLeaseFailsOver|MigrationLifecycleIsReversible)' -count=1

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
