package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestParseDaemonRequestReturnsConfiguredPath(t *testing.T) {
	request, err := parseDaemonRequest([]string{"--config", "config.yaml"}, io.Discard)
	if err != nil {
		t.Fatalf("parse daemon request: %v", err)
	}
	if request.configPath != "config.yaml" {
		t.Fatalf("config path = %q, want config.yaml", request.configPath)
	}
}

func TestParseDaemonRequestReportsInvalidArguments(t *testing.T) {
	if _, err := parseDaemonRequest(nil, io.Discard); err == nil {
		t.Fatal("expected missing config error")
	}
	if _, err := parseDaemonRequest([]string{"--missing"}, io.Discard); err == nil {
		t.Fatal("expected unknown flag error")
	}
}

func TestParseDaemonRequestWritesHelp(t *testing.T) {
	var output bytes.Buffer
	if _, err := parseDaemonRequest([]string{"--help"}, &output); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v", err)
	}
	expected := "Usage of runnerfoundry:\n" +
		"  -active-runner-policy string\n" +
		"    \taction for active runners during shutdown or incompatible reload: leave or terminate (default \"leave\")\n" +
		"  -config string\n" +
		"    \tpath to scaler YAML config\n"
	if output.String() != expected {
		t.Fatalf("help output snapshot mismatch\nexpected: %q\nactual:   %q", expected, output.String())
	}
}

func TestMigrationArgumentsRejectInvalidCommands(t *testing.T) {
	for _, args := range [][]string{
		{"migrate"},
		{"migrate", "unknown", "--config", "config.yaml"},
		{"migrate", "status"},
		{"migrate", "status", "--config", "config.yaml", "--active-runner-policy", "leave"},
		{"migrate", "up", "--config", "config.yaml", "--active-runner-policy", "leave"},
		{"migrate", "down", "--config", "config.yaml", "--to", "0", "--active-runner-policy", "leave"},
		{"migrate", "down", "--config", "config.yaml"},
		{"migrate", "down", "--to", "invalid", "--config", "config.yaml"},
		{"migrate", "up", "--to", "1", "--config", "config.yaml"},
		{"migrate", "up", "--unknown", "value", "--config", "config.yaml"},
	} {
		if err := executeCommand(t.Context(), args, nil, io.Discard, io.Discard, nil, nil); err == nil {
			t.Fatalf("expected %v to fail", args)
		}
	}
}

func TestMigrationHelpIsVisible(t *testing.T) {
	var output bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "status", "--help"}, nil,
		io.Discard, &output, nil, nil); err == nil {
		t.Fatal("expected help request")
	}
	if !strings.Contains(output.String(), "Usage of runnerfoundry migrate status") ||
		!strings.Contains(output.String(), "-config") {
		t.Fatalf("help output = %q", output.String())
	}
}
