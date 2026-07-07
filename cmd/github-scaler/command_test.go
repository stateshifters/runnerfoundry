package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/meza/github-scaler/internal/daemon"
)

func TestParseConfigPathRequiresConfigFlag(t *testing.T) {
	if _, err := parseConfigPath(nil); err == nil {
		t.Fatal("expected missing config error")
	}
}

func TestParseConfigPathReturnsConfigFlag(t *testing.T) {
	configPath, err := parseConfigPath([]string{"--config", "config.yaml"})
	if err != nil {
		t.Fatalf("expected config path, got %v", err)
	}
	if configPath != "config.yaml" {
		t.Fatalf("expected config.yaml, got %q", configPath)
	}
}

func TestParseConfigPathSurfacesFlagErrors(t *testing.T) {
	if _, err := parseConfigPath([]string{"--missing"}); err == nil {
		t.Fatal("expected flag parse error")
	}
}

func TestRunDelegatesToDaemon(t *testing.T) {
	originalRunDaemon := runDaemon
	defer func() {
		runDaemon = originalRunDaemon
	}()
	var capturedOptions daemon.Options
	runDaemon = func(ctx context.Context, options daemon.Options) error {
		capturedOptions = options
		return nil
	}

	reloadRequests := make(chan struct{})
	if err := run(context.Background(), []string{"--config", "config.yaml"}, reloadRequests); err != nil {
		t.Fatalf("expected run success, got %v", err)
	}
	if capturedOptions.ConfigPath != "config.yaml" {
		t.Fatalf("expected daemon config path, got %#v", capturedOptions)
	}
	if capturedOptions.ReloadRequests != reloadRequests {
		t.Fatal("expected reload requests channel to be passed to daemon")
	}
}

func TestRunSurfacesParseAndDaemonErrors(t *testing.T) {
	if err := run(context.Background(), nil, nil); err == nil {
		t.Fatal("expected parse error")
	}

	originalRunDaemon := runDaemon
	defer func() {
		runDaemon = originalRunDaemon
	}()
	runDaemon = func(ctx context.Context, options daemon.Options) error {
		return errors.New("daemon failed")
	}
	if err := run(context.Background(), []string{"--config", "config.yaml"}, nil); err == nil {
		t.Fatal("expected daemon error")
	}
}

func TestRunMainReportsExitStatus(t *testing.T) {
	originalRunDaemon := runDaemon
	defer func() {
		runDaemon = originalRunDaemon
	}()

	runDaemon = func(ctx context.Context, options daemon.Options) error {
		return nil
	}
	var stderr bytes.Buffer
	if exitCode := runMain([]string{"--config", "config.yaml"}, &stderr); exitCode != 0 {
		t.Fatalf("expected success exit, got %d", exitCode)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr, got %q", stderr.String())
	}

	runDaemon = func(ctx context.Context, options daemon.Options) error {
		return errors.New("daemon failed")
	}
	if exitCode := runMain([]string{"--config", "config.yaml"}, &stderr); exitCode != 1 {
		t.Fatalf("expected failure exit, got %d", exitCode)
	}
	if !strings.Contains(stderr.String(), "daemon failed") {
		t.Fatalf("expected daemon error on stderr, got %q", stderr.String())
	}
}

func TestRunMainHandlesStderrFailures(t *testing.T) {
	if exitCode := runMain(nil, failingWriter{}); exitCode != 1 {
		t.Fatalf("expected failure exit, got %d", exitCode)
	}
}

func TestMainExitsWithRunMainStatus(t *testing.T) {
	originalArgs := os.Args
	originalExitProcess := exitProcess
	defer func() {
		os.Args = originalArgs
		exitProcess = originalExitProcess
	}()
	os.Args = []string{"github-scaler"}
	var exitCode int
	exitProcess = func(code int) {
		exitCode = code
	}

	main()
	if exitCode != 1 {
		t.Fatalf("expected failure exit, got %d", exitCode)
	}
}

type failingWriter struct{}

func (writer failingWriter) Write(bytes []byte) (int, error) {
	return 0, io.ErrClosedPipe
}
