package daemon

import (
	"context"
	"fmt"

	"github.com/coreos/go-systemd/v22/daemon"
)

type systemdNotifier struct {
	notify func(bool, string) (bool, error)
}

func newSystemdNotifier() systemdNotifier {
	return systemdNotifier{notify: daemon.SdNotify}
}

func (notifier systemdNotifier) Ready(ctx context.Context) error {
	return notifySystemd(ctx, daemon.SdNotifyReady+"\nSTATUS=operational leadership=unknown", notifier.notify)
}

func (notifier systemdNotifier) Reloading(ctx context.Context) error {
	state := daemon.SdNotifyReloading + "\n" + daemon.SdNotifyMonotonicUsec()
	return notifySystemd(ctx, state, notifier.notify)
}

func (notifier systemdNotifier) Status(ctx context.Context, status operationalStatus) error {
	return notifySystemd(ctx, "STATUS="+string(status.health)+" leadership="+string(status.leadership), notifier.notify)
}

func notifySystemd(ctx context.Context, state string, notify func(bool, string) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("notify context: %w", err)
	}
	_, err := notify(false, state)
	if err != nil {
		return fmt.Errorf("notify systemd: %w", err)
	}
	return nil
}
