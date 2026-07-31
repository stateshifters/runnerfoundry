package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

type scaleSetSessionOperations struct {
	BuildReconciler func(
		ctx context.Context,
		repositoryURL string,
		scaleSetConfig config.ScaleSetConfig,
		client scaleSetJITClient,
		scaleSetID int,
	) (scaler.Reconciler, error)
	RunnerHostname func() string
	PollRetryDelay time.Duration
}

func startCoordinatedScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	coordinator *scaler.Coordinator,
) (scaleSetLifecycle, error) {
	return runCoordinatedScaleSet(
		ctx, logger, client, repositoryURL, scaleSetConfig, coordinator, productionScaleSetSessionOperations(),
	)
}

func productionScaleSetSessionOperations() scaleSetSessionOperations {
	return scaleSetSessionOperations{
		BuildReconciler: buildReconciler,
		RunnerHostname:  runnerHostname,
		PollRetryDelay:  pollRetryDelay,
	}
}

func runCoordinatedScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	coordinator *scaler.Coordinator,
	scaleSets scaleSetSessionOperations,
) (scaleSetLifecycle, error) {
	runnerScaleSet, messageClient, err := prepareScaleSetSession(ctx, logger, client, repositoryURL, scaleSetConfig, scaleSets.RunnerHostname())
	if err != nil {
		return scaleSetLifecycle{}, err
	}
	return runPreparedCoordinatedScaleSet(ctx, preparedCoordinatedScaleSet{
		logger: logger, client: client, repositoryURL: repositoryURL, scaleSetConfig: scaleSetConfig,
		coordinator: coordinator, operations: scaleSets, runnerScaleSet: runnerScaleSet, messageClient: messageClient,
	})
}

func startConfirmedCoordinatedScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	coordinator *scaler.Coordinator,
	runnerScaleSet *upstream.RunnerScaleSet,
) (scaleSetLifecycle, error) {
	scaleSets := productionScaleSetSessionOperations()
	messageClient, err := createMessageSession(
		ctx, logger, client, repositoryURL, scaleSetConfig.Name, scaleSets.RunnerHostname(), runnerScaleSet.ID,
	)
	if err != nil {
		return scaleSetLifecycle{}, err
	}
	return runPreparedCoordinatedScaleSet(ctx, preparedCoordinatedScaleSet{
		logger: logger, client: client, repositoryURL: repositoryURL, scaleSetConfig: scaleSetConfig,
		coordinator: coordinator, operations: scaleSets, runnerScaleSet: runnerScaleSet, messageClient: messageClient,
	})
}

type preparedCoordinatedScaleSet struct {
	logger         *slog.Logger
	client         scaleSetClient
	repositoryURL  string
	scaleSetConfig config.ScaleSetConfig
	coordinator    *scaler.Coordinator
	operations     scaleSetSessionOperations
	runnerScaleSet *upstream.RunnerScaleSet
	messageClient  scaleSetMessageClient
}

func runPreparedCoordinatedScaleSet(ctx context.Context, prepared preparedCoordinatedScaleSet) (scaleSetLifecycle, error) {
	logAction(ctx, prepared.logger, slog.LevelDebug, "reconciler_build", "started",
		slog.String("repository", prepared.repositoryURL), slog.String("scaleSet", prepared.scaleSetConfig.Name))
	reconciler, err := prepared.operations.BuildReconciler(
		ctx, prepared.repositoryURL, prepared.scaleSetConfig, prepared.client, prepared.runnerScaleSet.ID,
	)
	if err != nil {
		logActionFailure(ctx, prepared.logger, "reconciler_build", err,
			slog.String("repository", prepared.repositoryURL), slog.String("scaleSet", prepared.scaleSetConfig.Name))
		return scaleSetLifecycle{}, closeMessageSessionAfterReconcilerFailure(
			ctx, prepared.scaleSetConfig.Name, prepared.messageClient, err,
		)
	}
	logAction(ctx, prepared.logger, slog.LevelDebug, "reconciler_build", "completed",
		slog.String("repository", prepared.repositoryURL), slog.String("scaleSet", prepared.scaleSetConfig.Name))
	return startCoordinatedLifecycle(ctx, coordinatedLifecycleConfig{
		Logger: prepared.logger, Client: prepared.client, RepositoryURL: prepared.repositoryURL,
		ScaleSetConfig: prepared.scaleSetConfig, Coordinator: prepared.coordinator, RunnerScaleSet: prepared.runnerScaleSet,
		MessageClient: prepared.messageClient, Reconciler: reconciler, RetryDelay: prepared.operations.PollRetryDelay,
		RunnerReapRetries: newRunnerReapRetryTracker(time.Now),
	}), nil
}

type coordinatedLifecycleConfig struct {
	Logger            *slog.Logger
	Client            scaleSetDeletionClient
	RepositoryURL     string
	ScaleSetConfig    config.ScaleSetConfig
	Coordinator       *scaler.Coordinator
	RunnerScaleSet    *upstream.RunnerScaleSet
	MessageClient     scaleSetMessageClient
	Reconciler        scaler.Reconciler
	RetryDelay        time.Duration
	RunnerReapRetries *runnerReapRetryTracker
}

func startCoordinatedLifecycle(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig) scaleSetLifecycle {
	if lifecycleConfig.RunnerReapRetries == nil {
		lifecycleConfig.RunnerReapRetries = newRunnerReapRetryTracker(time.Now)
	}
	provider := scaler.RunnerProviderDocker
	if lifecycleConfig.ScaleSetConfig.Libvirt != nil {
		provider = scaler.RunnerProviderLibvirt
	}
	if err := lifecycleConfig.Coordinator.ConfigureRunnerLifecycle(
		&lifecycleConfig.Reconciler, lifecycleConfig.RepositoryURL, provider,
	); err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_lifecycle_configure", err,
			slog.String("repository", lifecycleConfig.RepositoryURL), slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name))
	}
	worker := coordinatedRepositoryWorker{
		Reconciler:    lifecycleConfig.Reconciler,
		MessageClient: lifecycleConfig.MessageClient,
		Logger:        lifecycleConfig.Logger,
		Repository:    lifecycleConfig.RepositoryURL,
		ScaleSet:      lifecycleConfig.ScaleSetConfig.Name,
	}
	lifecycleConfig.Coordinator.AddRepository(lifecycleConfig.RepositoryURL, worker)
	if statistics := lifecycleConfig.MessageClient.initialStatistics(); statistics != nil {
		lifecycleConfig.Coordinator.ObserveAssigned(lifecycleConfig.RepositoryURL, statistics.TotalAssignedJobs)
	}
	pollerCtx, stopPolling := context.WithCancel(ctx)
	pollerDone := startCoordinatedPoller(pollerCtx, lifecycleConfig)
	return coordinatedScaleSetLifecycle(lifecycleConfig, stopPolling, pollerDone)
}

func startCoordinatedPoller(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig) <-chan struct{} {
	poller := newCoordinatedPoller(lifecycleConfig)
	reapPendingRunners(ctx, lifecycleConfig)
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "started",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
	pollerDone := startCoordinatedWorkers(ctx, lifecycleConfig, &poller)
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "completed",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
	return pollerDone
}

func newCoordinatedPoller(lifecycleConfig coordinatedLifecycleConfig) scalesetpoller.Poller {
	return scalesetpoller.Poller{
		Client: lifecycleConfig.MessageClient,
		Handler: coordinatedScaleSetHandler{
			Coordinator: lifecycleConfig.Coordinator,
			Repository:  lifecycleConfig.RepositoryURL,
			Logger:      lifecycleConfig.Logger,
			ScaleSet:    lifecycleConfig.ScaleSetConfig.Name,
		},
		MaxCapacity: lifecycleConfig.ScaleSetConfig.MaxRunners,
	}
}

func startCoordinatedWorkers(
	ctx context.Context,
	lifecycleConfig coordinatedLifecycleConfig,
	poller *scalesetpoller.Poller,
) <-chan struct{} {
	pollerDone := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		pollUntilShutdown(
			ctx, lifecycleConfig.Logger, lifecycleConfig.RepositoryURL, lifecycleConfig.ScaleSetConfig.Name, poller,
			pollRetryAfter(lifecycleConfig.RetryDelay),
		)
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		runRunnerReaper(ctx, lifecycleConfig, ticker.C)
	}()
	go func() {
		workers.Wait()
		close(pollerDone)
	}()
	return pollerDone
}

func runRunnerReaper(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			reapPendingRunners(ctx, lifecycleConfig)
		}
	}
}

func reapPendingRunners(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig) {
	if lifecycleConfig.RunnerReapRetries == nil {
		lifecycleConfig.RunnerReapRetries = newRunnerReapRetryTracker(time.Now)
	}
	if !requestExpiredRunnerReaps(ctx, lifecycleConfig) {
		return
	}
	pending, loaded := loadPendingRunnerReaps(ctx, lifecycleConfig)
	if !loaded {
		return
	}
	lifecycleConfig.RunnerReapRetries.prune(pending)
	for _, runner := range pending {
		if !lifecycleConfig.RunnerReapRetries.ready(runner) {
			continue
		}
		reapPendingRunner(ctx, lifecycleConfig, runner)
	}
}

func requestExpiredRunnerReaps(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig) bool {
	timeout, err := lifecycleConfig.ScaleSetConfig.JobStartDuration()
	if err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_timeout_parse", err,
			slog.String("repository", lifecycleConfig.RepositoryURL), slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name))
		return false
	}
	expired, err := lifecycleConfig.Coordinator.RequestExpiredRunnerReaps(
		ctx, lifecycleConfig.RepositoryURL, timeout, time.Now().UTC(),
	)
	if err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_timeout_reconcile", err,
			slog.String("repository", lifecycleConfig.RepositoryURL), slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name))
		return false
	}
	if expired > 0 {
		logAction(ctx, lifecycleConfig.Logger, slog.LevelWarn, "runner_timeout", "cleanup_requested",
			slog.String("repository", lifecycleConfig.RepositoryURL), slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
			slog.Int("runners", expired), slog.Duration("jobStartTimeout", timeout))
	}
	return true
}

func loadPendingRunnerReaps(
	ctx context.Context,
	lifecycleConfig coordinatedLifecycleConfig,
) ([]scaler.RunnerLifecycle, bool) {
	pending, err := lifecycleConfig.Coordinator.PendingRunnerReaps(ctx, lifecycleConfig.RepositoryURL)
	if err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_reap_load", err,
			slog.String("repository", lifecycleConfig.RepositoryURL), slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name))
		return nil, false
	}
	return pending, true
}

func reapPendingRunner(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig, runner scaler.RunnerLifecycle) {
	reapCtx, cancelReap := context.WithTimeout(ctx, libvirtReapTimeout)
	reaped, err := lifecycleConfig.Reconciler.ReapRunnerLifecycle(reapCtx, runner)
	cancelReap()
	if errors.Is(err, scaler.ErrRunnerStillBusy) {
		retryAfter := lifecycleConfig.RunnerReapRetries.deferRunner(runner)
		logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "runner_reap", "deferred",
			append(runnerLogAttributes(runner), slog.Duration("retryAfter", retryAfter))...)
		return
	}
	if err != nil {
		lifecycleConfig.RunnerReapRetries.clear(runner)
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_reap", err, runnerLogAttributes(runner)...)
		return
	}
	if !reaped {
		lifecycleConfig.RunnerReapRetries.clear(runner)
		return
	}
	if err = lifecycleConfig.Coordinator.ConfirmRunnerReaped(ctx, runner); err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_reap_confirm", err, runnerLogAttributes(runner)...)
		return
	}
	lifecycleConfig.RunnerReapRetries.clear(runner)
	logAction(ctx, lifecycleConfig.Logger, slog.LevelInfo, "runner_reap", "completed", runnerLogAttributes(runner)...)
	if err = lifecycleConfig.Coordinator.Schedule(ctx); err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "runner_replacement", err,
			slog.String("repository", runner.Repository), slog.String("scaleSet", runner.ScaleSet))
	}
}

func runnerLogAttributes(runner scaler.RunnerLifecycle) []slog.Attr {
	return []slog.Attr{slog.String("repository", runner.Repository), slog.String("scaleSet", runner.ScaleSet),
		slog.String("runner", runner.RunnerName), slog.String("reason", runner.ReapReason)}
}

func runLibvirtReaper(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			reapPendingLibvirtRunners(ctx, lifecycleConfig)
		}
	}
}

func reapPendingLibvirtRunners(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig) {
	pending, err := lifecycleConfig.Coordinator.PendingLibvirtReaps(ctx, lifecycleConfig.RepositoryURL)
	if err != nil {
		logActionFailure(ctx, lifecycleConfig.Logger, "libvirt_reap_load", err,
			slog.String("repository", lifecycleConfig.RepositoryURL), slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name))
		return
	}
	for _, runner := range pending {
		reapCtx, cancelReap := context.WithTimeout(ctx, libvirtReapTimeout)
		reaped, reapErr := lifecycleConfig.Reconciler.ReapRunner(reapCtx, runner.RunnerName)
		cancelReap()
		if reapErr != nil {
			logActionFailure(ctx, lifecycleConfig.Logger, "libvirt_reap", reapErr,
				slog.String("repository", runner.Repository), slog.String("scaleSet", runner.ScaleSet),
				slog.String("runner", runner.RunnerName))
			continue
		}
		if !reaped {
			continue
		}
		if confirmErr := lifecycleConfig.Coordinator.ConfirmLibvirtReaped(ctx, runner); confirmErr != nil {
			logActionFailure(ctx, lifecycleConfig.Logger, "libvirt_reap_confirm", confirmErr,
				slog.String("repository", runner.Repository), slog.String("scaleSet", runner.ScaleSet),
				slog.String("runner", runner.RunnerName))
		}
	}
}

func coordinatedScaleSetLifecycle(
	lifecycleConfig coordinatedLifecycleConfig,
	stopPolling context.CancelFunc,
	pollerDone <-chan struct{},
) scaleSetLifecycle {
	return scaleSetLifecycle{
		StopPolling: func() {
			stopPolling()
			<-pollerDone
		},
		CloseSession: func(ctx context.Context) error {
			return closeScaleSetMessageSession(
				ctx, lifecycleConfig.Logger, lifecycleConfig.RepositoryURL, lifecycleConfig.ScaleSetConfig.Name,
				lifecycleConfig.MessageClient,
			)
		},
		Running:       lifecycleConfig.Reconciler.Running,
		DestroyActive: lifecycleConfig.Reconciler.DestroyActive,
		Shutdown: func(ctx context.Context) error {
			return shutdownScaleSet(
				ctx, lifecycleConfig.Logger, lifecycleConfig.RepositoryURL, lifecycleConfig.ScaleSetConfig.Name,
				lifecycleConfig.MessageClient, lifecycleConfig.Client, lifecycleConfig.RunnerScaleSet,
			)
		},
	}
}

type coordinatedRepositoryWorker struct {
	Reconciler    scaler.Reconciler
	MessageClient scaleSetMessageClient
	Logger        *slog.Logger
	Repository    string
	ScaleSet      string
}

func (worker coordinatedRepositoryWorker) Running(ctx context.Context) (int, error) {
	runningCount, err := worker.Reconciler.Running(ctx)
	if err != nil {
		return 0, fmt.Errorf("inspect repository runners: %w", err)
	}
	return runningCount, nil
}

func (worker coordinatedRepositoryWorker) Exists(ctx context.Context, requestID int64) (bool, error) {
	exists, err := worker.Reconciler.Exists(ctx, requestID)
	if err != nil {
		return false, fmt.Errorf("inspect runner for request %d: %w", requestID, err)
	}
	return exists, nil
}

func (worker coordinatedRepositoryWorker) Acquire(ctx context.Context, requestID int64) error {
	if err := worker.MessageClient.Acquire(ctx, requestID); err != nil {
		return err
	}
	effectiveLogger(worker.Logger).Info("job acquired", append(
		[]any{slog.String("action", "job_acquire"), slog.String("phase", "completed")},
		jobLogAttributes(worker.Repository, worker.ScaleSet, requestID)...,
	)...)
	return nil
}

func (worker coordinatedRepositoryWorker) StartOne(ctx context.Context, requestID int64) error {
	if err := worker.Reconciler.StartNamed(ctx, requestID); err != nil {
		return fmt.Errorf("start repository runner: %w", err)
	}
	effectiveLogger(worker.Logger).Info("runner started", append(
		[]any{slog.String("action", "runner_start"), slog.String("phase", "completed")},
		jobLogAttributes(worker.Repository, worker.ScaleSet, requestID)...,
	)...)
	return nil
}

func (worker coordinatedRepositoryWorker) StartAssigned(ctx context.Context) error {
	if err := worker.Reconciler.StartOne(ctx); err != nil {
		return fmt.Errorf("start assigned repository runner: %w", err)
	}
	return nil
}

func (worker coordinatedRepositoryWorker) ReconcileRunnerUsability(ctx context.Context) (int, error) {
	reapRequests, err := worker.Reconciler.ReconcileRunnerUsability(ctx)
	if err != nil {
		return 0, fmt.Errorf("reconcile repository runner usability: %w", err)
	}
	if reapRequests > 0 {
		effectiveLogger(worker.Logger).Warn("unusable runner capacity found",
			slog.String("action", "runner_orphan_detect"), slog.String("phase", "completed"),
			slog.String("repository", worker.Repository), slog.String("scaleSet", worker.ScaleSet),
			slog.Int("reapRequests", reapRequests),
		)
	}
	return reapRequests, nil
}

func (worker coordinatedRepositoryWorker) RunnerPresent(ctx context.Context, runnerName string) (bool, error) {
	present, err := worker.Reconciler.RunnerPresent(ctx, runnerName)
	if err != nil {
		return false, fmt.Errorf("inspect repository runner: %w", err)
	}
	return present, nil
}
