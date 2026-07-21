package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

func TestRunTracedOperationLogsSuccessfulLifecycle(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	attributeBacking := []slog.Attr{
		slog.String("repository", "https://github.com/acme/repo"),
		slog.String("guard", "unchanged"),
	}
	initialAttributes := attributeBacking[:1]
	operationCalls := 0

	err := runTracedOperation(
		context.Background(),
		logger,
		slog.LevelDebug,
		"discovery",
		initialAttributes,
		func() tracedOperationResult {
			operationCalls++
			return tracedOperationResult{
				terminalAttributes: []slog.Attr{slog.Int("repositoryCount", 1)},
				err:                nil,
			}
		},
	)
	if err != nil {
		t.Fatalf("run traced operation: %v", err)
	}
	if operationCalls != 1 {
		t.Fatalf("operation calls = %d, want 1", operationCalls)
	}
	if attributeBacking[1].Key != "guard" || attributeBacking[1].Value.String() != "unchanged" {
		t.Fatalf("initial attribute backing was mutated: %#v", attributeBacking)
	}

	events := structuredActionEvents(t, logs.Bytes(), "discovery")
	if len(events) != 2 {
		t.Fatalf("discovery events = %d, want 2: %s", len(events), logs.String())
	}
	if events[0]["phase"] != "started" || events[0]["repository"] != "https://github.com/acme/repo" {
		t.Fatalf("unexpected started event: %#v", events[0])
	}
	if _, exists := events[0]["duration"]; exists {
		t.Fatalf("started event contains duration: %#v", events[0])
	}
	if _, exists := events[0]["repositoryCount"]; exists {
		t.Fatalf("started event contains terminal attributes: %#v", events[0])
	}
	if events[1]["phase"] != "completed" || events[1]["repository"] != "https://github.com/acme/repo" {
		t.Fatalf("unexpected completed event: %#v", events[1])
	}
	if events[1]["repositoryCount"] != float64(1) {
		t.Fatalf("completed event repository count = %#v, want 1", events[1]["repositoryCount"])
	}
	if _, exists := events[1]["duration"]; !exists {
		t.Fatalf("completed event is missing duration: %#v", events[1])
	}
}

func TestRunTracedOperationLogsFailedLifecycleAndReturnsOperationError(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	operationErr := errors.New("discovery failed")
	operationCalls := 0

	err := runTracedOperation(
		context.Background(),
		logger,
		slog.LevelDebug,
		"discovery",
		[]slog.Attr{slog.String("repository", "https://github.com/acme/repo")},
		func() tracedOperationResult {
			operationCalls++
			return tracedOperationResult{
				terminalAttributes: []slog.Attr{slog.Int("repositoryCount", 1)},
				err:                operationErr,
			}
		},
	)
	if !errors.Is(err, operationErr) {
		t.Fatalf("run traced operation error = %v, want %v", err, operationErr)
	}
	if operationCalls != 1 {
		t.Fatalf("operation calls = %d, want 1", operationCalls)
	}

	events := structuredActionEvents(t, logs.Bytes(), "discovery")
	if len(events) != 2 {
		t.Fatalf("discovery events = %d, want 2: %s", len(events), logs.String())
	}
	if events[0]["phase"] != "started" {
		t.Fatalf("unexpected started event: %#v", events[0])
	}
	if events[1]["phase"] != "failed" || events[1]["level"] != "ERROR" {
		t.Fatalf("unexpected failed event: %#v", events[1])
	}
	if events[1]["error"] != operationErr.Error() || events[1]["repositoryCount"] != float64(1) {
		t.Fatalf("failed event is missing operation context: %#v", events[1])
	}
	if _, exists := events[1]["duration"]; !exists {
		t.Fatalf("failed event is missing duration: %#v", events[1])
	}
}

func TestRunTracedOperationExecutesOperationWithoutLogger(t *testing.T) {
	operationCalls := 0
	err := runTracedOperation(
		context.Background(),
		nil,
		slog.LevelInfo,
		"discovery",
		nil,
		func() tracedOperationResult {
			operationCalls++
			return tracedOperationResult{terminalAttributes: nil, err: nil}
		},
	)
	if err != nil {
		t.Fatalf("run traced operation: %v", err)
	}
	if operationCalls != 1 {
		t.Fatalf("operation calls = %d, want 1", operationCalls)
	}
}

func structuredActionEvents(t *testing.T, logBytes []byte, action string) []map[string]any {
	t.Helper()
	events := []map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(logBytes), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode structured log %q: %v", line, err)
		}
		if event["action"] == action {
			events = append(events, event)
		}
	}
	return events
}
