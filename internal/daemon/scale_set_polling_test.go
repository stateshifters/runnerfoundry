package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

func TestPollUntilShutdownHandlesPollErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pollCount := 0
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage: func() {
			pollCount++
			if pollCount > 1 {
				cancel()
			}
		},
	}, Handler: scaleSetHandler{}}
	pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, time.Nanosecond)
}

func TestPollUntilShutdownExitsDuringRetryDelayWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}
	returned := make(chan struct{})

	go func() {
		pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, time.Hour)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("expected poller to exit promptly after cancellation")
	}
}

func TestPollUntilShutdownExitsWhenCanceledDuringRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	firstPoll := make(chan struct{})
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage: func() {
			select {
			case <-firstPoll:
			default:
				close(firstPoll)
			}
		},
	}, Handler: scaleSetHandler{}}
	returned := make(chan struct{})

	go func() {
		pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, time.Hour)
		close(returned)
	}()

	select {
	case <-firstPoll:
	case <-time.After(time.Second):
		t.Fatal("expected first poll")
	}
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("expected poller to exit during retry delay")
	}
}

func TestPollUntilShutdownDoesNotLogShutdownCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: context.Canceled,
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "repo", "linux", poller, time.Nanosecond)

	if logs.Len() != 0 {
		t.Fatalf("expected no shutdown cancellation log, got %q", logs.String())
	}
}

func TestPollUntilShutdownLogsPollActionsAtDebugLevel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: context.Canceled,
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "https://github.com/acme/repo", "linux", poller, time.Nanosecond)

	assertStructuredAction(t, logs.Bytes(), "message_poll", "started")
	assertStructuredAction(t, logs.Bytes(), "message_poll", "canceled")
}

func TestPollUntilShutdownLogsSustainedFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pollCount := 0
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage: func() {
			pollCount++
			if pollCount > sustainedPollFailureThreshold {
				cancel()
			}
		},
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "https://github.com/acme/repo", "linux", poller, time.Nanosecond)

	if !strings.Contains(logs.String(), "sustained poll failures") || !strings.Contains(logs.String(), "repository=https://github.com/acme/repo") {
		t.Fatalf("expected sustained failure log, got %q", logs.String())
	}
}
