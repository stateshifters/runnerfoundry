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
	StopPolling func()
	Drain       func(ctx context.Context) (int, error)
	Shutdown    func(ctx context.Context) error
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
	runnerScaleSet, messageClient, err := prepareScaleSetSession(ctx, client, scaleSetConfig, scaleSets.RunnerHostname())
	if err != nil {
		return scaleSetLifecycle{}, err
	}
	reconciler, err := scaleSets.BuildReconciler(ctx, repositoryURL, scaleSetConfig, client, runnerScaleSet.ID)
	if err != nil {
		return scaleSetLifecycle{}, closeMessageSessionAfterReconcilerFailure(ctx, scaleSetConfig.Name, messageClient, err)
	}
	return startScaleSetLifecycle(ctx, logger, scaleSetConfig, client, runnerScaleSet, messageClient, reconciler, scaleSets.PollRetryDelay), nil
}

func prepareScaleSetSession(
	ctx context.Context,
	client scaleSetClient,
	scaleSetConfig config.ScaleSetConfig,
	runnerHostname string,
) (*upstream.RunnerScaleSet, scaleSetMessageClient, error) {
	runnerGroup, err := client.GetRunnerGroupByName(ctx, scaleSetConfig.RunnerGroup)
	if err != nil {
		return nil, scaleSetMessageClient{}, fmt.Errorf("get runner group %q: %w", scaleSetConfig.RunnerGroup, err)
	}
	runnerScaleSet, err := ensureRunnerScaleSet(ctx, client, runnerGroup.ID, scaleSetConfig)
	if err != nil {
		return nil, scaleSetMessageClient{}, fmt.Errorf("ensure runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	messageClient, err := client.MessageSessionClient(ctx, runnerScaleSet.ID, runnerHostname)
	if err != nil {
		return nil, scaleSetMessageClient{}, fmt.Errorf("create message session for %q: %w", scaleSetConfig.Name, err)
	}
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

func startScaleSetLifecycle(
	ctx context.Context,
	logger *slog.Logger,
	scaleSetConfig config.ScaleSetConfig,
	client scaleSetManager,
	runnerScaleSet *upstream.RunnerScaleSet,
	messageClient scaleSetMessageClient,
	reconciler scaler.Reconciler,
	retryDelay time.Duration,
) scaleSetLifecycle {
	pollerCtx, stopPolling := context.WithCancel(ctx)
	startPoller(pollerCtx, logger, scaleSetConfig, messageClient, reconciler, retryDelay)
	return scaleSetLifecycle{
		StopPolling: stopPolling,
		Drain:       reconciler.Drain,
		Shutdown: func(ctx context.Context) error {
			return shutdownScaleSet(ctx, messageClient, client, runnerScaleSet)
		},
	}
}

func shutdownScaleSet(
	ctx context.Context,
	messageClient scaleSetMessageClient,
	client scaleSetManager,
	runnerScaleSet *upstream.RunnerScaleSet,
) error {
	closeErr := messageClient.Close(ctx)
	deleteErr := client.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID)
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
	scaleSetConfig config.ScaleSetConfig,
	messageClient scalesetpoller.MessageClient,
	reconciler scaler.Reconciler,
	retryDelay time.Duration,
) {
	poller := scalesetpoller.Poller{
		Client:      messageClient,
		Handler:     scaleSetHandler{Reconciler: reconciler},
		MaxCapacity: scaleSetConfig.MaxRunners,
	}
	go pollUntilShutdown(ctx, logger, scaleSetConfig.Name, &poller, retryDelay)
}

func pollUntilShutdown(
	ctx context.Context,
	logger *slog.Logger,
	scaleSetName string,
	poller *scalesetpoller.Poller,
	retryDelay time.Duration,
) {
	for ctx.Err() == nil {
		if err := poller.PollOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("poll failed", slog.String("scaleSet", scaleSetName), slog.String("error", err.Error()))
			time.Sleep(retryDelay)
		}
	}
}

type scaleSetHandler struct {
	Reconciler scaler.Reconciler
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

func (generator scalesetJITGenerator) Generate(ctx context.Context, runnerName string) (string, error) {
	jitConfig, err := generator.Client.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, generator.ScaleSetID)
	if err != nil {
		return "", fmt.Errorf("generate jit runner config: %w", err)
	}
	return jitConfig.EncodedJITConfig, nil
}
