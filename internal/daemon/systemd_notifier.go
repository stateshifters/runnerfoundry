package daemon

import (
	"context"
	"fmt"

	"github.com/coreos/go-systemd/v22/daemon"
)

type systemdNotifier struct{}

func (notifier systemdNotifier) Ready(ctx context.Context) error {
	return notifySystemd(ctx, daemon.SdNotifyReady)
}

func (notifier systemdNotifier) Reloading(ctx context.Context) error {
	state := daemon.SdNotifyReloading + "\n" + daemon.SdNotifyMonotonicUsec()
	return notifySystemd(ctx, state)
}

func notifySystemd(ctx context.Context, state string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("notify context: %w", err)
	}
	_, err := daemon.SdNotify(false, state)
	if err != nil {
		return fmt.Errorf("notify systemd: %w", err)
	}
	return nil
}
