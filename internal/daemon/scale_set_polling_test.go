package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

func controlledPollRetry(wait func(context.Context) bool) pollRetryPolicy {
	return pollRetryPolicy{wait: func(ctx context.Context, _ time.Duration) bool { return wait(ctx) }}
}

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
	pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, controlledPollRetry(func(context.Context) bool { return true }))
}

func TestPollUntilShutdownStopsWhenPollFailureCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}
	pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, controlledPollRetry(func(context.Context) bool {
		t.Fatal("canceled poll failure invoked retry wait")
		return true
	}))
}

func TestPollUntilShutdownExitsWhenCanceledDuringRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	firstPoll := make(chan struct{})
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage:  func() { close(firstPoll) },
	}, Handler: scaleSetHandler{}}
	returned := make(chan struct{})
	retryStarted := make(chan struct{})

	go func() {
		pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, controlledPollRetry(func(retryCtx context.Context) bool {
			close(retryStarted)
			<-retryCtx.Done()
			return false
		}))
		close(returned)
	}()

	<-firstPoll
	<-retryStarted
	cancel()
	<-returned
}

func TestPollUntilShutdownDoesNotLogShutdownCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: context.Canceled,
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "repo", "linux", poller, controlledPollRetry(func(context.Context) bool {
		t.Fatal("shutdown cancellation invoked retry wait")
		return true
	}))

	if logs.Len() != 0 {
		t.Fatalf("expected no shutdown cancellation log, got %q", logs.String())
	}
}

func TestPollUntilShutdownTreatsNoMessageAsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{onGetMessage: cancel}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, controlledPollRetry(func(context.Context) bool {
		t.Fatal("successful poll invoked retry wait")
		return true
	}))
}

func TestPollUntilShutdownLogsPollActionsAtDebugLevel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: context.Canceled,
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "https://github.com/acme/repo", "linux", poller, controlledPollRetry(func(context.Context) bool {
		t.Fatal("canceled poll invoked retry wait")
		return true
	}))

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

	pollUntilShutdown(ctx, logger, "https://github.com/acme/repo", "linux", poller, controlledPollRetry(func(context.Context) bool { return true }))

	if !strings.Contains(logs.String(), "sustained poll failures") || !strings.Contains(logs.String(), "repository=https://github.com/acme/repo") {
		t.Fatalf("expected sustained failure log, got %q", logs.String())
	}
}
