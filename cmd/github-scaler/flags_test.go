package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestParseConfigPathReturnsConfiguredPath(t *testing.T) {
	configPath, err := parseConfigPath([]string{"--config", "config.yaml"}, io.Discard)
	if err != nil {
		t.Fatalf("parse config path: %v", err)
	}
	if configPath != "config.yaml" {
		t.Fatalf("config path = %q, want config.yaml", configPath)
	}
}

func TestParseConfigPathReportsInvalidArguments(t *testing.T) {
	if _, err := parseConfigPath(nil, io.Discard); err == nil {
		t.Fatal("expected missing config error")
	}
	if _, err := parseConfigPath([]string{"--missing"}, io.Discard); err == nil {
		t.Fatal("expected unknown flag error")
	}
}

func TestParseConfigPathWritesHelp(t *testing.T) {
	var output bytes.Buffer
	if _, err := parseConfigPath([]string{"--help"}, &output); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v", err)
	}
	if !strings.Contains(output.String(), "Usage of github-scaler") || !strings.Contains(output.String(), "-config") {
		t.Fatalf("help output = %q", output.String())
	}
}

func TestMigrationArgumentsRejectInvalidCommands(t *testing.T) {
	for _, args := range [][]string{
		{"migrate"},
		{"migrate", "unknown", "--config", "config.yaml"},
		{"migrate", "status"},
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
	if !strings.Contains(output.String(), "Usage of github-scaler migrate status") ||
		!strings.Contains(output.String(), "-config") {
		t.Fatalf("help output = %q", output.String())
	}
}
