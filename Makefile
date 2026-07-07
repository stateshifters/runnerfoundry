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

.PHONY: fmt-check lint lint-fix vet test test-race coverage build notices sbom package-deb prepare clean

ifeq ($(OS),Windows_NT)
OSFAMILY := Windows
else
OSFAMILY := Unix
endif

fmt-check:
ifeq ($(OSFAMILY),Windows)
	@powershell -NoProfile -Command "$$files = gofmt -l .; if ($$files) { Write-Host 'Run gofmt on:'; $$files; exit 1 }"
else
	@files=$$(gofmt -l $$(find . -name '*.go')); if [ -n "$$files" ]; then echo 'Run gofmt on:'; echo "$$files"; exit 1; fi
endif

lint:
ifeq ($(OSFAMILY),Windows)
	@powershell -NoProfile -Command "$$env:GOTOOLCHAIN='$(GOLANGCI_LINT_TOOLCHAIN)'; go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run"
else
	@GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run
endif

lint-fix:
ifeq ($(OSFAMILY),Windows)
	@powershell -NoProfile -Command "$$env:GOTOOLCHAIN='$(GOLANGCI_LINT_TOOLCHAIN)'; go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run --fix"
else
	@GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) go tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint run --fix
endif

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

coverage:
ifeq ($(OSFAMILY),Windows)
	go test ./... -coverprofile=coverage
	@powershell -NoProfile -Command "$$total = (go tool cover -func=coverage | Select-String '^total:').ToString().Split()[-1]; if ($$total -ne '100.0%') { go tool cover -func=coverage; Write-Host \"coverage total must be 100.0%, got $$total\"; exit 1 }"
else
	go test ./... -coverprofile=coverage
	@total=$$(go tool cover -func=coverage | awk '/^total:/ { print $$3 }'); \
	if [ "$$total" != "100.0%" ]; then \
		go tool cover -func=coverage; \
		echo "coverage total must be 100.0%, got $$total"; \
		exit 1; \
	fi
endif

build:
ifeq ($(OSFAMILY),Windows)
	powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(BUILD_DIR)/linux/amd64' | Out-Null; $$env:GOOS='linux'; $$env:GOARCH='amd64'; $$env:CGO_ENABLED='0'; go build -o '$(BUILD_DIR)/linux/amd64/$(EXECUTABLE_NAME)' ./cmd/github-scaler"
else
	mkdir -p "$(BUILD_DIR)/linux/amd64"
	GOOS=linux GOARCH=amd64 go build -o "$(BUILD_DIR)/linux/amd64/$(EXECUTABLE_NAME)" ./cmd/github-scaler
endif

notices:
ifeq ($(OSFAMILY),Windows)
	powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(METADATA_DIR)' | Out-Null; go run github.com/google/go-licenses@v1.6.0 report ./... | Out-File -FilePath '$(NOTICES_FILE)' -Encoding utf8"
else
	mkdir -p "$(METADATA_DIR)"
	go run github.com/google/go-licenses@v1.6.0 report ./... > "$(NOTICES_FILE)"
endif

sbom:
ifeq ($(OSFAMILY),Windows)
	powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(METADATA_DIR)' | Out-Null; go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.9.0 mod -licenses -json -output '$(SBOM_FILE)'"
else
	mkdir -p "$(METADATA_DIR)"
	go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.9.0 mod -licenses -json -output "$(SBOM_FILE)"
endif

package-deb: build notices sbom
ifeq ($(OSFAMILY),Windows)
	powershell -NoProfile -Command "Remove-Item -Recurse -Force '$(DIST_DIR)' -ErrorAction SilentlyContinue; New-Item -ItemType Directory -Force -Path '$(DIST_DIR)' | Out-Null; $$env:VERSION='$(VERSION)'; $(NFPM) package --packager deb --config nfpm.yaml --target '$(DIST_DIR)'"
else
	rm -rf "$(DIST_DIR)"
	mkdir -p "$(DIST_DIR)"
	VERSION="$(VERSION)" $(NFPM) package --packager deb --config nfpm.yaml --target "$(DIST_DIR)"
endif

prepare:
	$(MAKE) package-deb VERSION="$(VERSION)"

clean:
ifeq ($(OSFAMILY),Windows)
	powershell -NoProfile -Command "foreach ($$path in '$(BUILD_DIR)','$(DIST_DIR)','coverage','coverage.out') { if (Test-Path -LiteralPath $$path) { Remove-Item -Recurse -Force -LiteralPath $$path } }"
else
	rm -rf "$(BUILD_DIR)" "$(DIST_DIR)" coverage coverage.out
endif
