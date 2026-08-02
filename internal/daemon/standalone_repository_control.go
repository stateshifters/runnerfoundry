package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func (control *standaloneControl) Reconcile(
	ctx context.Context,
	repository githubapp.Repository,
	priority repositoryWorkPriority,
) {
	logCtx := context.WithoutCancel(ctx)
	control.pool.enqueue(repository.URL, priority, func(workCtx context.Context) error {
		control.active.RepositoryMutex.Lock()
		managed := control.active.Repositories[repository.URL]
		control.active.RepositoryMutex.Unlock()
		managed, err := reconcileRepository(workCtx, control.daemon, control.active, repository, managed)
		control.active.RepositoryMutex.Lock()
		control.active.Repositories[repository.URL] = managed
		control.active.RepositoryMutex.Unlock()
		if err == nil {
			err = control.persistRegistrations(workCtx, repository.URL, managed)
		}
		if err == nil {
			err = schedulePersistedDemand(workCtx, control.active)
		}
		return err
	}, func(err error) {
		control.completed(logCtx, repository, err)
	})
}

func (control *standaloneControl) enqueueReconnect(ctx context.Context, repository githubapp.Repository) {
	control.Reconcile(ctx, repository, repositoryWorkUrgent)
}

func (control *standaloneControl) persistRegistrations(
	ctx context.Context,
	repositoryURL string,
	managed managedRepository,
) error {
	configured := configuredScaleSetNames(control.active.Config.ScaleSets)
	if err := control.upsertActiveRegistrations(ctx, repositoryURL, managed); err != nil {
		return err
	}
	registrations := control.currentRegistrations()
	if err := control.deleteObsoleteRegistrations(ctx, repositoryURL, configured, registrations); err != nil {
		return err
	}
	return control.refreshRepositorySnapshot(ctx)
}

func configuredScaleSetNames(scaleSets []config.ScaleSetConfig) map[string]struct{} {
	configured := make(map[string]struct{}, len(scaleSets))
	for _, scaleSetConfig := range scaleSets {
		configured[scaleSetConfig.Name] = struct{}{}
	}
	return configured
}

func (control *standaloneControl) upsertActiveRegistrations(
	ctx context.Context,
	repositoryURL string,
	managed managedRepository,
) error {
	for _, scaleSetConfig := range control.active.Config.ScaleSets {
		lifecycle, present := managed.Lifecycles[scaleSetConfig.Name]
		if !present {
			continue
		}
		registration := repositorystate.Registration{
			AppID: control.active.Credentials.AppID, RepositoryURL: repositoryURL,
			ScaleSetName: scaleSetConfig.Name, RunnerGroup: scaleSetConfig.RunnerGroup,
			GitHubScaleSetID: lifecycle.RunnerScaleSetID,
		}
		err := control.active.RepositoryState.UpsertRegistration(
			ctx, control.active.Credentials.AppID, control.epoch, registration,
		)
		if err != nil {
			return fmt.Errorf("upsert registration %q for %q: %w", scaleSetConfig.Name, repositoryURL, err)
		}
	}
	return nil
}

func (control *standaloneControl) currentRegistrations() []repositorystate.Registration {
	control.active.RepositoryMutex.Lock()
	defer control.active.RepositoryMutex.Unlock()
	return append([]repositorystate.Registration(nil), control.active.RepositorySnapshot.Registrations...)
}

func (control *standaloneControl) deleteObsoleteRegistrations(
	ctx context.Context,
	repositoryURL string,
	configured map[string]struct{},
	registrations []repositorystate.Registration,
) error {
	for _, registration := range registrations {
		if registration.RepositoryURL != repositoryURL {
			continue
		}
		if _, retained := configured[registration.ScaleSetName]; retained {
			continue
		}
		err := control.active.RepositoryState.DeleteRegistration(
			ctx, control.active.Credentials.AppID, control.epoch, repositoryURL, registration.ScaleSetName,
		)
		if err != nil {
			return fmt.Errorf("delete obsolete registration %q for %q: %w", registration.ScaleSetName, repositoryURL, err)
		}
	}
	return nil
}

func (control *standaloneControl) refreshRepositorySnapshot(ctx context.Context) error {
	snapshot, err := control.active.RepositoryState.LoadSnapshot(ctx, control.active.Credentials.AppID)
	if err != nil {
		return fmt.Errorf("refresh standalone repository snapshot: %w", err)
	}
	control.active.RepositoryMutex.Lock()
	control.active.RepositorySnapshot = snapshot
	control.active.RepositoryMutex.Unlock()
	return nil
}

func (control *standaloneControl) enqueueRemoval(
	ctx context.Context,
	repositoryURL string,
	completion func(error),
) {
	logCtx := context.WithoutCancel(ctx)
	control.pool.enqueue(repositoryURL, repositoryWorkRoutine, func(workCtx context.Context) error {
		control.active.RepositoryMutex.Lock()
		repository, present := control.active.Repositories[repositoryURL]
		control.active.RepositoryMutex.Unlock()
		if !present {
			return nil
		}
		if err := cleanupRepositoryImmediately(workCtx, control.active, repositoryURL, repository); err != nil {
			return err
		}
		control.active.RepositoryMutex.Lock()
		delete(control.active.Repositories, repositoryURL)
		control.active.RepositoryMutex.Unlock()
		return nil
	}, func(err error) {
		if err != nil {
			control.completed(logCtx, githubapp.Repository{URL: repositoryURL}, err)
		}
		if completion != nil {
			completion(err)
		}
	})
}

func (control *standaloneControl) completed(ctx context.Context, repository githubapp.Repository, err error) {
	control.mutex.Lock()
	previousAttempts := control.retries[repository.URL]
	if err == nil || control.stopped() || errors.Is(err, repositorystate.ErrControllerFenced) {
		control.completeRepair(ctx, repository.URL, previousAttempts, err)
		return
	}
	control.scheduleRepair(ctx, repository, previousAttempts, err)
}

func (control *standaloneControl) completeRepair(
	ctx context.Context,
	repositoryURL string,
	previousAttempts int,
	err error,
) {
	control.retries[repositoryURL] = 0
	delete(control.timers, repositoryURL)
	control.mutex.Unlock()
	if err == nil && previousAttempts > 0 {
		logAction(ctx, control.active.Logger, slog.LevelInfo, "repository_repair", "completed",
			slog.String("repository", repositoryURL), slog.Int("attempt", previousAttempts+1),
			slog.Int64("epoch", control.epoch))
	}
}

func (control *standaloneControl) scheduleRepair(
	ctx context.Context,
	repository githubapp.Repository,
	previousAttempts int,
	repairErr error,
) {
	attempt := previousAttempts
	control.retries[repository.URL] = attempt + 1
	delay := standaloneRetryDelay(attempt)
	control.timers[repository.URL] = control.retryAfter(delay, func() {
		if control.stopped() {
			return
		}
		logAction(ctx, control.active.Logger, slog.LevelInfo, "repository_repair", "retrying",
			slog.String("repository", repository.URL), slog.Int("attempt", attempt+2),
			slog.Duration("retryAfter", delay), slog.Int64("epoch", control.epoch))
		control.Reconcile(ctx, repository, repositoryWorkUrgent)
	})
	control.mutex.Unlock()
	logAction(ctx, control.active.Logger, slog.LevelWarn, "repository_repair", "pending",
		slog.String("repository", repository.URL), slog.Int("attempt", attempt+1),
		slog.Duration("retryAfter", delay), slog.Int64("epoch", control.epoch),
		slog.String("error", repairErr.Error()))
}

func standaloneRetryDelay(attempt int) time.Duration {
	delayIndex := attempt
	if delayIndex >= len(standaloneRepositoryRetryDelays) {
		delayIndex = len(standaloneRepositoryRetryDelays) - 1
	}
	return standaloneRepositoryRetryDelays[delayIndex]
}
