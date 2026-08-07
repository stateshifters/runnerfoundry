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
	if err := notifier.Status(context.Background(), operationalStatus{health: healthOperational, leadership: leadershipLeader}); err != nil {
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

func TestSystemdNotifierPublishesSourceNeutralHealthAndLeadership(t *testing.T) {
	states := make([]string, 0, 2)
	notifier := systemdNotifier{notify: func(_ bool, state string) (bool, error) {
		states = append(states, state)
		return true, nil
	}}
	if err := notifier.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := notifier.Status(context.Background(), operationalStatus{health: healthDegraded, leadership: leadershipStandby}); err != nil {
		t.Fatal(err)
	}
	want := []string{"READY=1\nSTATUS=ready leadership=unknown", "STATUS=degraded leadership=standby"}
	for index := range want {
		if states[index] != want[index] {
			t.Fatalf("state %d = %q, want %q", index, states[index], want[index])
		}
	}
}

func TestSystemdNotifierReadyPreservesHealthAlreadyReportedByMaintenance(t *testing.T) {
	t.Parallel()

	states := make([]string, 0, 2)
	notifier := systemdNotifier{notify: func(_ bool, state string) (bool, error) {
		states = append(states, state)
		return true, nil
	}}
	status := operationalStatus{health: healthDegraded, leadership: leadershipLeader}
	if err := notifier.Status(context.Background(), status); err != nil {
		t.Fatal(err)
	}
	if err := notifier.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"STATUS=degraded leadership=leader",
		"READY=1\nSTATUS=degraded leadership=leader",
	}
	for index := range want {
		if states[index] != want[index] {
			t.Fatalf("state %d = %q, want %q", index, states[index], want[index])
		}
	}
}

func TestSystemdNotifierReloadReadinessDoesNotReusePreviousGenerationHealth(t *testing.T) {
	t.Parallel()

	states := make([]string, 0, 3)
	notifier := systemdNotifier{notify: func(_ bool, state string) (bool, error) {
		states = append(states, state)
		return true, nil
	}}
	if err := notifier.Status(context.Background(), operationalStatus{
		health: healthOperational, leadership: leadershipLeader,
	}); err != nil {
		t.Fatal(err)
	}
	if err := notifier.Reloading(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := notifier.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := states[len(states)-1]; state != "READY=1\nSTATUS=ready leadership=unknown" {
		t.Fatalf("replacement ready state = %q", state)
	}
}

func TestSystemdNotifierFailedReloadNotificationPreservesPreviousHealth(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("reload notification failed")
	states := make([]string, 0, 3)
	notifier := systemdNotifier{notify: func(_ bool, state string) (bool, error) {
		states = append(states, state)
		if len(states) == 2 {
			return false, wantErr
		}
		return true, nil
	}}
	status := operationalStatus{health: healthOperational, leadership: leadershipLeader}
	if err := notifier.Status(context.Background(), status); err != nil {
		t.Fatal(err)
	}
	if err := notifier.Reloading(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("reload notification error = %v, want %v", err, wantErr)
	}
	if err := notifier.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := states[len(states)-1]; state != "READY=1\nSTATUS=operational leadership=leader" {
		t.Fatalf("restored ready state = %q", state)
	}
}
