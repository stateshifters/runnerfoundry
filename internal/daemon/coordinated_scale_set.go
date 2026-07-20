package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

type scaleSetSessionOperations struct {
	BuildReconciler func(
		ctx context.Context,
		repositoryURL string,
		scaleSetConfig config.ScaleSetConfig,
		client scaleSetClient,
		scaleSetID int,
	) (scaler.Reconciler, error)
	RunnerHostname func() string
	PollRetryDelay time.Duration
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
	logAction(ctx, logger, slog.LevelDebug, "reconciler_build", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
	reconciler, err := scaleSets.BuildReconciler(ctx, repositoryURL, scaleSetConfig, client, runnerScaleSet.ID)
	if err != nil {
		logActionFailure(ctx, logger, "reconciler_build", err,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
		return scaleSetLifecycle{}, closeMessageSessionAfterReconcilerFailure(ctx, scaleSetConfig.Name, messageClient, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "reconciler_build", "completed",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
	return startCoordinatedLifecycle(ctx, coordinatedLifecycleConfig{
		Logger: logger, Client: client, RepositoryURL: repositoryURL,
		ScaleSetConfig: scaleSetConfig, Coordinator: coordinator, RunnerScaleSet: runnerScaleSet,
		MessageClient: messageClient, Reconciler: reconciler, RetryDelay: scaleSets.PollRetryDelay,
	}), nil
}

type coordinatedLifecycleConfig struct {
	Logger         *slog.Logger
	Client         scaleSetClient
	RepositoryURL  string
	ScaleSetConfig config.ScaleSetConfig
	Coordinator    *scaler.Coordinator
	RunnerScaleSet *upstream.RunnerScaleSet
	MessageClient  scaleSetMessageClient
	Reconciler     scaler.Reconciler
	RetryDelay     time.Duration
}

func startCoordinatedLifecycle(ctx context.Context, lifecycleConfig coordinatedLifecycleConfig) scaleSetLifecycle {
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
	poller := scalesetpoller.Poller{
		Client: lifecycleConfig.MessageClient,
		Handler: coordinatedScaleSetHandler{
			Coordinator: lifecycleConfig.Coordinator,
			Repository:  lifecycleConfig.RepositoryURL,
			Logger:      lifecycleConfig.Logger,
			ScaleSet:    lifecycleConfig.ScaleSetConfig.Name,
		},
		MaxCapacity: lifecycleConfig.ScaleSetConfig.MaxRunners,
	}
	pollerDone := make(chan struct{})
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "started",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
	go func() {
		defer close(pollerDone)
		pollUntilShutdown(ctx, lifecycleConfig.Logger, lifecycleConfig.RepositoryURL, lifecycleConfig.ScaleSetConfig.Name, &poller, lifecycleConfig.RetryDelay)
	}()
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "completed",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
	return pollerDone
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
		Drain:         lifecycleConfig.Reconciler.Drain,
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
