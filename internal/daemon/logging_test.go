package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

func TestRunTracedOperationExecutesSuccessfulOperationOnce(t *testing.T) {
	result := runSuccessfulTracedOperation(t)
	if result.operationCalls != 1 {
		t.Fatalf("operation calls = %d, want 1", result.operationCalls)
	}
	if result.attributeBacking[1].Key != "guard" || result.attributeBacking[1].Value.String() != "unchanged" {
		t.Fatalf("initial attribute backing was mutated: %#v", result.attributeBacking)
	}
}

func TestRunTracedOperationLogsSuccessfulStart(t *testing.T) {
	events := runSuccessfulTracedOperation(t).events
	if len(events) != 2 {
		t.Fatalf("discovery events = %d, want 2", len(events))
	}
	if events[0]["phase"] != "started" || events[0]["repository"] != "https://github.com/acme/repo" {
		t.Fatalf("started event = %#v", events[0])
	}
	if _, exists := events[0]["duration"]; exists {
		t.Fatalf("started event contains duration: %#v", events[0])
	}
	if _, exists := events[0]["repositoryCount"]; exists {
		t.Fatalf("started event contains terminal attributes: %#v", events[0])
	}
}

func TestRunTracedOperationLogsSuccessfulCompletion(t *testing.T) {
	events := runSuccessfulTracedOperation(t).events
	if events[1]["phase"] != "completed" || events[1]["repository"] != "https://github.com/acme/repo" {
		t.Fatalf("completed event = %#v", events[1])
	}
	if events[1]["repositoryCount"] != float64(1) {
		t.Fatalf("repository count = %#v, want 1", events[1]["repositoryCount"])
	}
	if _, exists := events[1]["duration"]; !exists {
		t.Fatalf("completed event is missing duration: %#v", events[1])
	}
}

type successfulTraceResult struct {
	operationCalls   int
	attributeBacking []slog.Attr
	events           []map[string]any
}

func runSuccessfulTracedOperation(t *testing.T) successfulTraceResult {
	t.Helper()
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
	events := structuredActionEvents(t, logs.Bytes(), "discovery")
	return successfulTraceResult{operationCalls: operationCalls, attributeBacking: attributeBacking, events: events}
}

func TestRunTracedOperationReturnsOperationError(t *testing.T) {
	result := runFailedTracedOperation(t)
	if !errors.Is(result.err, result.operationErr) {
		t.Fatalf("operation error = %v, want %v", result.err, result.operationErr)
	}
	if result.operationCalls != 1 {
		t.Fatalf("operation calls = %d, want 1", result.operationCalls)
	}
}

func TestRunTracedOperationLogsFailedLifecycle(t *testing.T) {
	result := runFailedTracedOperation(t)
	if len(result.events) != 2 || result.events[0]["phase"] != "started" {
		t.Fatalf("lifecycle events = %#v", result.events)
	}
	failedEvent := result.events[1]
	if failedEvent["phase"] != "failed" || failedEvent["level"] != "ERROR" {
		t.Fatalf("failed event = %#v", failedEvent)
	}
	if failedEvent["error"] != result.operationErr.Error() || failedEvent["repositoryCount"] != float64(1) {
		t.Fatalf("failed event context = %#v", failedEvent)
	}
	if _, exists := failedEvent["duration"]; !exists {
		t.Fatalf("failed event is missing duration: %#v", failedEvent)
	}
}

type failedTraceResult struct {
	err            error
	operationErr   error
	operationCalls int
	events         []map[string]any
}

func runFailedTracedOperation(t *testing.T) failedTraceResult {
	t.Helper()
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
	events := structuredActionEvents(t, logs.Bytes(), "discovery")
	return failedTraceResult{err: err, operationErr: operationErr, operationCalls: operationCalls, events: events}
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
