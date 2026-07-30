package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestSystemdNotifierIgnoresMissingNotifySocket(t *testing.T) {
	notifier := systemdNotifier{notify: func(bool, string) (bool, error) { return false, nil }}
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
	if err := notifySystemd(ctx, "READY=1", func(bool, string) (bool, error) { return false, nil }); err == nil {
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
	failure := errors.New("failed")
	if err := notifySystemd(context.Background(), "READY=1", func(bool, string) (bool, error) {
		return false, failure
	}); err == nil {
		t.Fatal("expected notify socket error")
	}
}

func TestProductionSystemdNotifierProvidesNotificationBoundary(t *testing.T) {
	if newSystemdNotifier().notify == nil {
		t.Fatal("missing systemd notification boundary")
	}
}
