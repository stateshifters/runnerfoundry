package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

const standaloneGitHubAttemptTimeout = 30 * time.Second

var standaloneRepositoryRetryDelays = [...]time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	60 * time.Second,
}

type standaloneControlOptions struct {
	attemptTimeout time.Duration
	retryAfter     func(time.Duration, func()) standaloneRetryTimer
}

type standaloneRetryTimer interface {
	Stop() bool
}

type standaloneControl struct {
	done             <-chan struct{}
	cancel           context.CancelFunc
	cancelDiscovery  context.CancelFunc
	daemon           standaloneDaemon
	active           *activeConfiguration
	pool             *repositoryWorkerPool
	epoch            int64
	mutex            sync.Mutex
	background       sync.WaitGroup
	retries          map[string]int
	timers           map[string]standaloneRetryTimer
	discoveryRunning bool
	retryAfter       func(time.Duration, func()) standaloneRetryTimer
}

func startStandaloneControl(
	ctx context.Context,
	daemon standaloneDaemon,
	active *activeConfiguration,
	options standaloneControlOptions,
) (*standaloneControl, error) {
	options = normalizeStandaloneControlOptions(options)
	controlCtx, cancel := context.WithCancel(ctx)
	snapshot, err := initializeStandaloneState(controlCtx, active)
	if err != nil {
		cancel()
		return nil, err
	}
	control := newStandaloneControl(controlCtx, cancel, daemon, active, snapshot, options)
	for _, coordinator := range active.Coordinators {
		coordinator.SetControllerIdentity(snapshot.Control.AppID, snapshot.Control.ControllerEpoch)
		coordinator.SetSchedulingErrorHandler(func(err error) {
			active.Logger.Error("standalone capacity obligation failed", slog.String("error", err.Error()))
		})
		coordinator.StartSchedulingSafetyLoop(controlCtx, 5*time.Second)
	}
	logStandaloneCatalogLoaded(controlCtx, active, snapshot)
	control.enqueueDurableRepositories(controlCtx, snapshot.Repositories)
	return control, nil
}

func normalizeStandaloneControlOptions(options standaloneControlOptions) standaloneControlOptions {
	if options.attemptTimeout <= 0 {
		options.attemptTimeout = standaloneGitHubAttemptTimeout
	}
	if options.retryAfter == nil {
		options.retryAfter = func(delay time.Duration, retry func()) standaloneRetryTimer {
			return time.AfterFunc(delay, retry)
		}
	}
	return options
}

func initializeStandaloneState(
	ctx context.Context,
	active *activeConfiguration,
) (repositorystate.Snapshot, error) {
	snapshot, err := active.RepositoryState.StartController(ctx, active.Credentials.AppID)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("start standalone repository controller: %w", err)
	}
	if active.ConfirmedDiscovery != nil {
		snapshot, err = applyConfirmedStandaloneDiscovery(ctx, active, snapshot.Control.ControllerEpoch)
		if err != nil {
			return repositorystate.Snapshot{}, err
		}
	}
	if err = persistConfirmedStandaloneRegistrations(ctx, active, snapshot.Control.ControllerEpoch); err != nil {
		return repositorystate.Snapshot{}, err
	}
	if len(active.ConfirmedRegistrations) > 0 {
		snapshot, err = active.RepositoryState.LoadSnapshot(ctx, active.Credentials.AppID)
		if err != nil {
			return repositorystate.Snapshot{}, fmt.Errorf("load confirmed standalone registrations: %w", err)
		}
	}
	active.ConfirmedDiscovery = nil
	active.ConfirmedRegistrations = nil
	if active.RepositoryMutex == nil {
		active.RepositoryMutex = &sync.Mutex{}
	}
	active.RepositorySnapshot = snapshot
	return snapshot, nil
}

func persistConfirmedStandaloneRegistrations(
	ctx context.Context,
	active *activeConfiguration,
	epoch int64,
) error {
	for _, registration := range active.ConfirmedRegistrations {
		err := active.RepositoryState.UpsertRegistration(ctx, active.Credentials.AppID, epoch, registration)
		if err != nil {
			return fmt.Errorf("persist confirmed standalone registration %q: %w", registration.ScaleSetName, err)
		}
	}
	return nil
}

func newStandaloneControl(
	ctx context.Context,
	cancel context.CancelFunc,
	daemon standaloneDaemon,
	active *activeConfiguration,
	snapshot repositorystate.Snapshot,
	options standaloneControlOptions,
) *standaloneControl {
	return &standaloneControl{
		done: ctx.Done(), cancel: cancel, daemon: daemon, active: active,
		pool: newRepositoryWorkerPool(ctx, options.attemptTimeout), epoch: snapshot.Control.ControllerEpoch,
		retries: map[string]int{}, timers: map[string]standaloneRetryTimer{}, retryAfter: options.retryAfter,
	}
}

func logStandaloneCatalogLoaded(
	ctx context.Context,
	active *activeConfiguration,
	snapshot repositorystate.Snapshot,
) {
	logAction(ctx, active.Logger, slog.LevelInfo, "repository_catalog", "loaded",
		slog.Int("repositoryCount", len(snapshot.Repositories)),
		slog.Int("registrationCount", len(snapshot.Registrations)),
		slog.Int64("generation", snapshot.Control.DiscoveryGeneration),
		slog.Int64("epoch", snapshot.Control.ControllerEpoch),
		slog.Bool("initialized", snapshot.Control.Initialized))
}

func (control *standaloneControl) enqueueDurableRepositories(
	ctx context.Context,
	repositories []repositorystate.Repository,
) {
	for _, durableRepository := range repositories {
		repository := githubapp.Repository{InstallationID: durableRepository.InstallationID, URL: durableRepository.URL}
		control.active.RepositoryMutex.Lock()
		managed := control.active.Repositories[repository.URL]
		control.active.RepositoryMutex.Unlock()
		if managed.RegistrationStatus == repositoryRegistrationOperational && len(managed.Lifecycles) > 0 {
			control.Reconcile(ctx, repository, repositoryWorkRoutine)
			continue
		}
		control.enqueueReconnect(ctx, repository)
	}
}

func applyConfirmedStandaloneDiscovery(
	ctx context.Context,
	active *activeConfiguration,
	epoch int64,
) (repositorystate.Snapshot, error) {
	discovery := *active.ConfirmedDiscovery
	repositories, failedInstallationIDs := standaloneDiscoveryState(active.Credentials.AppID, discovery)
	snapshot, err := active.RepositoryState.ApplyDiscovery(
		ctx, active.Credentials.AppID, epoch, discovery.Installations,
		failedInstallationIDs, repositories, time.Now(),
	)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("apply confirmed standalone discovery: %w", err)
	}
	return snapshot, nil
}

func standaloneDiscoveryState(
	appID int64,
	discovery githubapp.DiscoveryResult,
) ([]repositorystate.Repository, []githubapp.InstallationID) {
	repositories := make([]repositorystate.Repository, 0, len(discovery.Repositories))
	for _, repository := range discovery.Repositories {
		repositories = append(repositories, repositorystate.Repository{
			AppID: appID, URL: repository.URL, InstallationID: repository.InstallationID,
		})
	}
	failedInstallationIDs := make([]githubapp.InstallationID, 0, len(discovery.InstallationFailures))
	for installationID := range discovery.InstallationFailures {
		failedInstallationIDs = append(failedInstallationIDs, installationID)
	}
	return repositories, failedInstallationIDs
}

func (control *standaloneControl) Stop() {
	control.cancel()
	control.mutex.Lock()
	if control.cancelDiscovery != nil {
		control.cancelDiscovery()
	}
	for _, timer := range control.timers {
		timer.Stop()
	}
	control.timers = map[string]standaloneRetryTimer{}
	control.mutex.Unlock()
	control.pool.wait()
	control.background.Wait()
	for _, coordinator := range control.active.Coordinators {
		coordinator.WaitForScheduling()
	}
}

func (control *standaloneControl) RequestDiscovery(ctx context.Context) {
	control.mutex.Lock()
	if control.discoveryRunning || control.stopped() {
		control.mutex.Unlock()
		return
	}
	discoveryCtx, cancelDiscovery := context.WithTimeout(ctx, standaloneGitHubAttemptTimeout)
	control.discoveryRunning = true
	control.cancelDiscovery = cancelDiscovery
	control.background.Add(1)
	control.mutex.Unlock()
	go control.discover(discoveryCtx)
}

func (control *standaloneControl) discover(ctx context.Context) {
	defer control.finishDiscovery()
	discovery, err := control.runDiscovery(ctx)
	if err != nil {
		control.logDiscoveryFailure(ctx, err)
		return
	}
	if err = control.validateDiscovery(discovery); err != nil {
		control.active.Logger.Error("validate repository discovery failed", slog.String("error", err.Error()))
		return
	}
	snapshot, err := control.persistDiscovery(ctx, discovery)
	if err != nil {
		if !errors.Is(err, repositorystate.ErrControllerFenced) {
			control.active.Logger.Error("persist repository discovery failed", slog.String("error", err.Error()))
		}
		return
	}
	control.logDiscoveryApplied(ctx, snapshot)
	control.applySnapshot(ctx, snapshot)
}

func (control *standaloneControl) finishDiscovery() {
	control.mutex.Lock()
	control.cancelDiscovery()
	control.cancelDiscovery = nil
	control.discoveryRunning = false
	control.mutex.Unlock()
	control.background.Done()
}

func (control *standaloneControl) runDiscovery(ctx context.Context) (githubapp.DiscoveryResult, error) {
	var discovery githubapp.DiscoveryResult
	err := runTracedOperation(ctx, control.active.Logger, slog.LevelInfo, "discovery", nil, func() tracedOperationResult {
		result, discoveryErr := control.active.Discoverer.Discover(ctx)
		discovery = result
		return tracedOperationResult{terminalAttributes: []slog.Attr{
			slog.Int("repositoryCount", len(result.Repositories)),
			slog.Int("installationFailureCount", len(result.InstallationFailures)),
		}, err: discoveryErr}
	})
	return discovery, err
}

func (control *standaloneControl) logDiscoveryFailure(ctx context.Context, err error) {
	if control.active.RepositorySnapshot.Control.Initialized {
		logAction(ctx, control.active.Logger, slog.LevelWarn, "discovery", "stale",
			slog.Int64("generation", control.active.RepositorySnapshot.Control.DiscoveryGeneration),
			slog.Int64("epoch", control.epoch), slog.String("error", err.Error()))
		return
	}
	control.active.Logger.Error("repository discovery failed", slog.String("error", err.Error()))
}

func (control *standaloneControl) validateDiscovery(discovery githubapp.DiscoveryResult) error {
	control.active.RepositoryMutex.Lock()
	desired := desiredRepositories(control.active, discovery)
	control.active.RepositoryMutex.Unlock()
	return validateDiscoveredNames(desired, control.active.Config.ScaleSets)
}

func (control *standaloneControl) persistDiscovery(
	ctx context.Context,
	discovery githubapp.DiscoveryResult,
) (repositorystate.Snapshot, error) {
	repositories, failedInstallationIDs := standaloneDiscoveryState(control.active.Credentials.AppID, discovery)
	snapshot, err := control.active.RepositoryState.ApplyDiscovery(
		ctx, control.active.Credentials.AppID, control.epoch, discovery.Installations,
		failedInstallationIDs, repositories, time.Now(),
	)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("apply standalone discovery: %w", err)
	}
	return snapshot, nil
}

func (control *standaloneControl) logDiscoveryApplied(ctx context.Context, snapshot repositorystate.Snapshot) {
	logAction(ctx, control.active.Logger, slog.LevelInfo, "repository_catalog", "applied",
		slog.Int("repositoryCount", len(snapshot.Repositories)),
		slog.Int("registrationCount", len(snapshot.Registrations)),
		slog.Int64("generation", snapshot.Control.DiscoveryGeneration),
		slog.Int64("epoch", snapshot.Control.ControllerEpoch))
}

func (control *standaloneControl) applySnapshot(ctx context.Context, snapshot repositorystate.Snapshot) {
	desired := make(map[string]repositorystate.Repository, len(snapshot.Repositories))
	for _, repository := range snapshot.Repositories {
		desired[repository.URL] = repository
		control.Reconcile(ctx, githubapp.Repository{
			InstallationID: repository.InstallationID, URL: repository.URL,
		}, repositoryWorkRoutine)
	}
	control.active.RepositoryMutex.Lock()
	removed := removedRepositoryURLs(control.active.Repositories, desired)
	control.active.RepositorySnapshot = snapshot
	control.active.RepositoryMutex.Unlock()
	for _, repositoryURL := range removed {
		control.enqueueRemoval(ctx, repositoryURL, nil)
	}
}

func removedRepositoryURLs(
	managed map[string]managedRepository,
	desired map[string]repositorystate.Repository,
) []string {
	removed := make([]string, 0)
	for repositoryURL := range managed {
		if _, retained := desired[repositoryURL]; !retained {
			removed = append(removed, repositoryURL)
		}
	}
	return removed
}

func (control *standaloneControl) stopped() bool {
	select {
	case <-control.done:
		return true
	default:
		return false
	}
}
