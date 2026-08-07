package daemon

import (
	"context"
	"fmt"
	"sync"

	"github.com/coreos/go-systemd/v22/daemon"
)

type systemdNotifier struct {
	notify     func(bool, string) (bool, error)
	mutex      sync.Mutex
	lastStatus operationalStatus
	hasStatus  bool
}

func newSystemdNotifier() *systemdNotifier {
	return &systemdNotifier{notify: daemon.SdNotify}
}

func (notifier *systemdNotifier) Ready(ctx context.Context) error {
	notifier.mutex.Lock()
	defer notifier.mutex.Unlock()
	status := "ready leadership=unknown"
	if notifier.hasStatus {
		status = fmt.Sprintf("%s leadership=%s", notifier.lastStatus.health, notifier.lastStatus.leadership)
	}
	return notifySystemd(ctx, daemon.SdNotifyReady+"\nSTATUS="+status, notifier.notify)
}

func (notifier *systemdNotifier) Reloading(ctx context.Context) error {
	notifier.mutex.Lock()
	defer notifier.mutex.Unlock()
	state := daemon.SdNotifyReloading + "\n" + daemon.SdNotifyMonotonicUsec()
	if err := notifySystemd(ctx, state, notifier.notify); err != nil {
		return err
	}
	notifier.hasStatus = false
	return nil
}

func (notifier *systemdNotifier) Status(ctx context.Context, status operationalStatus) error {
	notifier.mutex.Lock()
	defer notifier.mutex.Unlock()
	notifier.lastStatus = status
	notifier.hasStatus = true
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
