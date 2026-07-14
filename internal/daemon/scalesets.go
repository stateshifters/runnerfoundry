package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

const pollRetryDelay = 5 * time.Second
const sustainedPollFailureThreshold = 3

var operatingSystemHostname = os.Hostname

type scaleSetClient interface {
	scaleSetManager
	scaleSetRunnerClient
}

type scaleSetManager interface {
	GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error)
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, scaleSetName string) (*upstream.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, runnerScaleSet *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error)
	DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error
}

type scaleSetRunnerClient interface {
	MessageSessionClient(ctx context.Context, runnerScaleSetID int, runnerName string) (scaleSetMessageClient, error)
	GenerateJitRunnerConfig(
		ctx context.Context,
		setting *upstream.RunnerScaleSetJitRunnerSetting,
		runnerScaleSetID int,
	) (*upstream.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(ctx context.Context, runnerName string) (*upstream.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

type scaleSetPruneClient interface {
	GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error)
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
	DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error
}

type scaleSetMessageClient struct {
	Client messageSessionClient
}

type scaleSetLifecycle struct {
	StopPolling   func()
	Drain         func(ctx context.Context) (int, error)
	DestroyActive func(ctx context.Context) error
	Shutdown      func(ctx context.Context) error
}

type messageSessionClient interface {
	scalesetpoller.MessageClient
	Close(ctx context.Context) error
}

func (client scaleSetMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	message, err := client.Client.GetMessage(ctx, lastMessageID, maxCapacity)
	if err != nil {
		return nil, fmt.Errorf("get message: %w", err)
	}
	return message, nil
}

func (client scaleSetMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	if err := client.Client.DeleteMessage(ctx, messageID); err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

func (client scaleSetMessageClient) Close(ctx context.Context) error {
	if err := client.Client.Close(ctx); err != nil {
		return fmt.Errorf("close message session: %w", err)
	}
	return nil
}

func (client scaleSetMessageClient) Acquire(ctx context.Context, requestID int64) error {
	acquirer, ok := client.Client.(interface {
		AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error)
	})
	if !ok {
		return errors.New("message session does not support job acquisition")
	}
	acquiredIDs, err := acquirer.AcquireJobs(ctx, []int64{requestID})
	if err != nil {
		return fmt.Errorf("acquire job: %w", err)
	}
	for _, acquiredID := range acquiredIDs {
		if acquiredID == requestID {
			return nil
		}
	}
	return fmt.Errorf("job %d was not acquired", requestID)
}

type scaleSetRuntime struct {
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

func productionScaleSetRuntime() scaleSetRuntime {
	return scaleSetRuntime{
		BuildReconciler: buildReconciler,
		RunnerHostname:  runnerHostname,
		PollRetryDelay:  pollRetryDelay,
	}
}

func runScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	scaleSets scaleSetRuntime,
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
	return startScaleSetLifecycle(ctx, scaleSetLifecycleConfig{
		Logger: logger, RepositoryURL: repositoryURL, ScaleSetConfig: scaleSetConfig,
		Client: client, RunnerScaleSet: runnerScaleSet, MessageClient: messageClient,
		Reconciler: reconciler, RetryDelay: scaleSets.PollRetryDelay,
	}), nil
}

func runCoordinatedScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	coordinator *scaler.Coordinator,
	scaleSets scaleSetRuntime,
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
	lifecycleConfig.Coordinator.AddRepository(lifecycleConfig.RepositoryURL, coordinatedRepositoryWorker{
		Reconciler:    lifecycleConfig.Reconciler,
		MessageClient: lifecycleConfig.MessageClient,
		Logger:        lifecycleConfig.Logger,
		Repository:    lifecycleConfig.RepositoryURL,
		ScaleSet:      lifecycleConfig.ScaleSetConfig.Name,
	})
	pollerCtx, stopPolling := context.WithCancel(ctx)
	poller := &scalesetpoller.Poller{
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
		pollUntilShutdown(pollerCtx, lifecycleConfig.Logger, lifecycleConfig.RepositoryURL, lifecycleConfig.ScaleSetConfig.Name, poller, lifecycleConfig.RetryDelay)
	}()
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "completed",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
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
	exists, err := worker.Reconciler.Exists(ctx, fmt.Sprintf("%s-%d", worker.Reconciler.ScaleSet.Name, requestID))
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
	if err := worker.Reconciler.StartNamed(ctx, fmt.Sprintf("%s-%d", worker.Reconciler.ScaleSet.Name, requestID)); err != nil {
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

func prepareScaleSetSession(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	runnerHostname string,
) (*upstream.RunnerScaleSet, scaleSetMessageClient, error) {
	logAction(ctx, logger, slog.LevelDebug, "runner_group_lookup", "started",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.String("runnerGroup", scaleSetConfig.RunnerGroup),
	)
	runnerGroup, err := client.GetRunnerGroupByName(ctx, scaleSetConfig.RunnerGroup)
	if err != nil {
		logActionFailure(ctx, logger, "runner_group_lookup", err,
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetConfig.Name),
			slog.String("runnerGroup", scaleSetConfig.RunnerGroup),
		)
		return nil, scaleSetMessageClient{}, fmt.Errorf("get runner group %q: %w", scaleSetConfig.RunnerGroup, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "runner_group_lookup", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.String("runnerGroup", scaleSetConfig.RunnerGroup),
		slog.Int("runnerGroupID", runnerGroup.ID),
	)
	logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_ensure", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
	runnerScaleSet, err := ensureRunnerScaleSet(ctx, client, runnerGroup.ID, scaleSetConfig)
	if err != nil {
		logActionFailure(ctx, logger, "runner_scale_set_ensure", err,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
		return nil, scaleSetMessageClient{}, fmt.Errorf("ensure runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_ensure", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.Int("runnerScaleSetID", runnerScaleSet.ID),
	)
	logAction(ctx, logger, slog.LevelDebug, "message_session_create", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
	messageClient, err := client.MessageSessionClient(ctx, runnerScaleSet.ID, runnerHostname)
	if err != nil {
		logActionFailure(ctx, logger, "message_session_create", err,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
		return nil, scaleSetMessageClient{}, fmt.Errorf("create message session for %q: %w", scaleSetConfig.Name, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "message_session_create", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.Int("runnerScaleSetID", runnerScaleSet.ID),
	)
	return runnerScaleSet, messageClient, nil
}

func closeMessageSessionAfterReconcilerFailure(
	ctx context.Context,
	scaleSetName string,
	messageClient scaleSetMessageClient,
	reconcilerErr error,
) error {
	if closeErr := messageClient.Close(context.WithoutCancel(ctx)); closeErr != nil {
		return errors.Join(
			fmt.Errorf("build reconciler for %q: %w", scaleSetName, reconcilerErr),
			fmt.Errorf("close message session: %w", closeErr),
		)
	}
	return fmt.Errorf("build reconciler for %q: %w", scaleSetName, reconcilerErr)
}

type scaleSetLifecycleConfig struct {
	Logger         *slog.Logger
	RepositoryURL  string
	ScaleSetConfig config.ScaleSetConfig
	Client         scaleSetManager
	RunnerScaleSet *upstream.RunnerScaleSet
	MessageClient  scaleSetMessageClient
	Reconciler     scaler.Reconciler
	RetryDelay     time.Duration
}

func startScaleSetLifecycle(ctx context.Context, lifecycleConfig scaleSetLifecycleConfig) scaleSetLifecycle {
	pollerCtx, stopPolling := context.WithCancel(ctx)
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "started",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
	pollerDone := startPoller(
		pollerCtx, lifecycleConfig.Logger, lifecycleConfig.RepositoryURL, lifecycleConfig.ScaleSetConfig,
		lifecycleConfig.MessageClient, lifecycleConfig.Reconciler, lifecycleConfig.RetryDelay,
	)
	logAction(ctx, lifecycleConfig.Logger, slog.LevelDebug, "polling_start", "completed",
		slog.String("repository", lifecycleConfig.RepositoryURL),
		slog.String("scaleSet", lifecycleConfig.ScaleSetConfig.Name),
	)
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

func shutdownScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	messageClient scaleSetMessageClient,
	client scaleSetManager,
	runnerScaleSet *upstream.RunnerScaleSet,
) error {
	logAction(ctx, logger, slog.LevelDebug, "message_session_close", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	closeErr := messageClient.Close(ctx)
	if closeErr != nil {
		logActionFailure(ctx, logger, "message_session_close", closeErr,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	} else {
		logAction(ctx, logger, slog.LevelDebug, "message_session_close", "completed",
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	}
	logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_delete", "started",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
		slog.Int("runnerScaleSetID", runnerScaleSet.ID),
	)
	deleteErr := client.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID)
	if deleteErr != nil {
		logActionFailure(ctx, logger, "runner_scale_set_delete", deleteErr,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	} else {
		logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_delete", "completed",
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetName),
			slog.Int("runnerScaleSetID", runnerScaleSet.ID),
		)
	}
	return errors.Join(closeErr, deleteErr)
}

func ensureRunnerScaleSet(
	ctx context.Context,
	client scaleSetManager,
	runnerGroupID int,
	scaleSetConfig config.ScaleSetConfig,
) (*upstream.RunnerScaleSet, error) {
	runnerScaleSet, err := client.GetRunnerScaleSet(ctx, runnerGroupID, scaleSetConfig.Name)
	if err != nil {
		return nil, fmt.Errorf("get runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	if runnerScaleSet != nil {
		return runnerScaleSet, nil
	}
	runnerScaleSet, err = client.CreateRunnerScaleSet(ctx, &upstream.RunnerScaleSet{
		Name:          scaleSetConfig.Name,
		RunnerGroupID: runnerGroupID,
		Labels: []upstream.Label{{
			Name: scaleSetConfig.Name,
			Type: "System",
		}},
		RunnerSetting: upstream.RunnerSetting{
			DisableUpdate: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	return runnerScaleSet, nil
}

func runnerHostname() string {
	hostname, err := operatingSystemHostname()
	if err != nil {
		return "github-scaler"
	}
	return hostname
}

func startPoller(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	messageClient scalesetpoller.MessageClient,
	reconciler scaler.Reconciler,
	retryDelay time.Duration,
) <-chan struct{} {
	poller := scalesetpoller.Poller{
		Client:      messageClient,
		Handler:     scaleSetHandler{Reconciler: reconciler},
		MaxCapacity: scaleSetConfig.MaxRunners,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollUntilShutdown(ctx, logger, repositoryURL, scaleSetConfig.Name, &poller, retryDelay)
	}()
	return done
}

func pollUntilShutdown(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	poller *scalesetpoller.Poller,
	retryDelay time.Duration,
) {
	consecutiveFailures := 0
	for ctx.Err() == nil {
		pollStartedAt := time.Now()
		logAction(ctx, logger, slog.LevelDebug, "message_poll", "started",
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetName),
		)
		if err := poller.PollOnce(ctx); err != nil {
			if ctx.Err() != nil {
				logAction(context.WithoutCancel(ctx), logger, slog.LevelDebug, "message_poll", "canceled",
					slog.String("repository", repositoryURL),
					slog.String("scaleSet", scaleSetName),
					slog.Duration("duration", time.Since(pollStartedAt)),
				)
				return
			}
			consecutiveFailures = logPollFailure(logger, repositoryURL, scaleSetName, err, consecutiveFailures)
			if !waitForPollRetry(ctx, retryDelay) {
				return
			}
			continue
		}
		consecutiveFailures = 0
		logAction(ctx, logger, slog.LevelDebug, "message_poll", "completed",
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetName),
			slog.Int("lastMessageID", poller.LastMessageID),
			slog.Duration("duration", time.Since(pollStartedAt)),
		)
	}
}

func logPollFailure(logger *slog.Logger, repositoryURL string, scaleSetName string, pollErr error, consecutiveFailures int) int {
	consecutiveFailures++
	logger.Error("poll failed",
		slog.String("action", "message_poll"),
		slog.String("phase", "failed"),
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
		slog.String("error", pollErr.Error()),
	)
	if consecutiveFailures == sustainedPollFailureThreshold {
		logger.Error(
			"sustained poll failures",
			slog.String("action", "message_poll"),
			slog.String("phase", "degraded"),
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetName),
			slog.Int("consecutiveFailures", consecutiveFailures),
		)
	}
	return consecutiveFailures
}

func waitForPollRetry(ctx context.Context, retryDelay time.Duration) bool {
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type scaleSetHandler struct {
	Reconciler scaler.Reconciler
}

type coordinatedScaleSetHandler struct {
	Coordinator *scaler.Coordinator
	Repository  string
	Logger      *slog.Logger
	ScaleSet    string
}

func (handler coordinatedScaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	observedDemand, removedDemand, err := handler.collectDemand(message)
	if err != nil {
		return err
	}
	if applyErr := handler.Coordinator.Apply(ctx, observedDemand, removedDemand); applyErr != nil {
		return fmt.Errorf("persist GitHub job demand: %w", applyErr)
	}
	if scheduleErr := handler.Coordinator.Schedule(ctx); scheduleErr != nil {
		return fmt.Errorf("schedule shared runner capacity: %w", scheduleErr)
	}
	reconciliation, err := handler.Coordinator.ReconcileAssigned(ctx, handler.Repository, message.Statistics.TotalAssignedJobs)
	if err != nil {
		return fmt.Errorf("reconcile assigned runner capacity: %w", err)
	}
	effectiveLogger(handler.Logger).Info(
		"assigned capacity reconciled",
		slog.String("action", "capacity_reconcile"),
		slog.String("phase", "completed"),
		slog.String("repository", handler.Repository),
		slog.String("scaleSet", handler.ScaleSet),
		slog.Int("assignedJobs", reconciliation.AssignedJobs),
		slog.Int("runningRunners", reconciliation.RunningRunners),
		slog.Int("startedRunners", reconciliation.StartedRunners),
	)
	return nil
}

func (handler coordinatedScaleSetHandler) collectDemand(message *upstream.RunnerScaleSetMessage) ([]scaler.JobDemand, []scaler.DemandRemoval, error) {
	observedDemand := make([]scaler.JobDemand, 0, len(message.JobAvailableMessages))
	removedDemand := make([]scaler.DemandRemoval, 0, len(message.JobCompletedMessages))
	for _, availableJob := range message.JobAvailableMessages {
		if availableJob == nil || availableJob.RunnerRequestID <= 0 || availableJob.QueueTime.IsZero() {
			return nil, nil, errors.New("job available message requires a positive runner request ID and queue time")
		}
		handler.logDemand("available", availableJob.RunnerRequestID, availableJob.QueueTime)
		observedDemand = append(observedDemand, scaler.JobDemand{
			RequestID:  availableJob.RunnerRequestID,
			Repository: handler.Repository,
			QueueTime:  availableJob.QueueTime,
		})
	}
	for _, completedJob := range message.JobCompletedMessages {
		if completedJob == nil || completedJob.RunnerRequestID <= 0 {
			continue
		}
		removedDemand = append(removedDemand, scaler.DemandRemoval{Repository: handler.Repository, RequestID: completedJob.RunnerRequestID})
		attributes := []any{slog.String("action", "demand_remove"), slog.String("phase", "completed")}
		attributes = append(attributes, jobLogAttributes(handler.Repository, handler.ScaleSet, completedJob.RunnerRequestID)...)
		attributes = append(attributes, slog.String("status", "completed"))
		effectiveLogger(handler.Logger).Info("job demand removed", attributes...)
	}
	return observedDemand, removedDemand, nil
}

func (handler coordinatedScaleSetHandler) logDemand(status string, requestID int64, queueTime time.Time) {
	attributes := []any{slog.String("action", "demand_observe"), slog.String("phase", "completed")}
	attributes = append(attributes, jobLogAttributes(handler.Repository, handler.ScaleSet, requestID)...)
	attributes = append(attributes, slog.String("status", status), slog.Time("queueTime", queueTime))
	effectiveLogger(handler.Logger).Info("job demand observed", attributes...)
}

func jobLogAttributes(repository string, scaleSet string, requestID int64) []any {
	return []any{slog.String("repository", repository), slog.String("scaleSet", scaleSet), slog.Int64("requestID", requestID)}
}

func effectiveLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

func (handler scaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	_, err := handler.Reconciler.Reconcile(ctx, message.Statistics.TotalAssignedJobs)
	if err != nil {
		return fmt.Errorf("reconcile assigned jobs: %w", err)
	}
	return nil
}

type scalesetJITGenerator struct {
	Client     scaleSetClient
	ScaleSetID int
}

func (generator scalesetJITGenerator) Generate(ctx context.Context, runnerName string) (scaler.JITRunnerConfig, error) {
	jitConfig, err := generator.Client.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, generator.ScaleSetID)
	if err != nil {
		if !errors.Is(err, upstream.RunnerExistsError) {
			return scaler.JITRunnerConfig{}, fmt.Errorf("generate jit runner config: %w", err)
		}
		return generator.replaceStaleRunnerAndGenerate(ctx, runnerName, err)
	}
	return scaler.JITRunnerConfig{
		Runner:    jitRunnerReference(jitConfig.Runner),
		JITConfig: jitConfig.EncodedJITConfig,
	}, nil
}

func (generator scalesetJITGenerator) replaceStaleRunnerAndGenerate(
	ctx context.Context,
	runnerName string,
	generateErr error,
) (scaler.JITRunnerConfig, error) {
	staleRunner, err := generator.Client.GetRunnerByName(ctx, runnerName)
	if err != nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("find stale runner after jit conflict for %q: %w", runnerName, err)
	}
	if staleRunner == nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("find stale runner after jit conflict for %q: %w", runnerName, generateErr)
	}
	removeErr := generator.Client.RemoveRunner(ctx, int64(staleRunner.ID))
	if removeErr != nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("remove stale runner %q: %w", runnerName, removeErr)
	}
	jitConfig, err := generator.Client.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, generator.ScaleSetID)
	if err != nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("generate jit runner config after stale runner cleanup: %w", err)
	}
	return scaler.JITRunnerConfig{
		Runner:    jitRunnerReference(jitConfig.Runner),
		JITConfig: jitConfig.EncodedJITConfig,
	}, nil
}

func (generator scalesetJITGenerator) Remove(ctx context.Context, runner scaler.JITRunner) error {
	if runner.ID == 0 {
		return nil
	}
	if err := generator.Client.RemoveRunner(ctx, runner.ID); err != nil {
		return fmt.Errorf("remove jit runner %q: %w", runner.Name, err)
	}
	return nil
}

func jitRunnerReference(runner *upstream.RunnerReference) scaler.JITRunner {
	if runner == nil {
		return scaler.JITRunner{}
	}
	return scaler.JITRunner{ID: int64(runner.ID), Name: runner.Name}
}
