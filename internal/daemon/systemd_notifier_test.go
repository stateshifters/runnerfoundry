package daemon

import (
	"context"
	"testing"
)

func TestSystemdNotifierIgnoresMissingNotifySocket(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	notifier := systemdNotifier{}
	if err := notifier.Ready(context.Background()); err != nil {
		t.Fatalf("expected ready notification without socket to be ignored, got %v", err)
	}
	if err := notifier.Reloading(context.Background()); err != nil {
		t.Fatalf("expected reloading notification without socket to be ignored, got %v", err)
	}
	if err := notifier.Status(context.Background(), clusterServiceStatusLeaderOperational); err != nil {
		t.Fatalf("expected status notification without socket to be ignored, got %v", err)
	}
}

func TestSystemdNotifierSurfacesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := notifySystemd(ctx, "READY=1"); err == nil {
		t.Fatal("expected canceled context error")
	}
}

func TestNoopNotifierSupportsReloading(t *testing.T) {
	notifier := noopNotifier{}
	if err := notifier.Reloading(context.Background()); err != nil {
		t.Fatalf("expected noop reloading success, got %v", err)
	}
}

func TestSystemdNotifierSurfacesNotifySocketFailure(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "/path/that/does/not/exist/github-scaler.sock")
	if err := notifySystemd(context.Background(), "READY=1"); err == nil {
		t.Fatal("expected notify socket error")
	}
}
