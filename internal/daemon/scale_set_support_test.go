package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func runTestScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	scaleSetConfig config.ScaleSetConfig,
	operations scaleSetSessionOperations,
) (scaleSetLifecycle, error) {
	return runCoordinatedScaleSet(
		ctx, logger, client, "https://github.com/meza/repo-a", scaleSetConfig,
		scaler.NewCoordinator(scaleSetConfig.MaxRunners), operations,
	)
}

func fakeExistingScaleSetClient(
	runnerGroup *upstream.RunnerGroup,
	runnerScaleSet *upstream.RunnerScaleSet,
	messageClient scaleSetMessageClient,
) *fakeScaleSetClient {
	return &fakeScaleSetClient{fakeScaleSetSessionClient: fakeScaleSetSessionClient{
		fakeRunnerGroupClient: fakeRunnerGroupClient{getRunnerGroupByName: func(context.Context, string) (*upstream.RunnerGroup, error) {
			return runnerGroup, nil
		}},
		fakeScaleSetRegistrationClient: fakeScaleSetRegistrationClient{getRunnerScaleSet: func(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
			return runnerScaleSet, nil
		}},
		fakeScaleSetMessageSessionClient: fakeScaleSetMessageSessionClient{messageSessionClient: func(context.Context, int, string) (scaleSetMessageClient, error) {
			return messageClient, nil
		}},
	}}
}

type fakeScaleSetClient struct {
	fakeScaleSetSessionClient
	fakeScaleSetDeletionClient
	fakeScaleSetJITClient
	fakeScaleSetListClient
}

type fakeScaleSetSessionClient struct {
	fakeRunnerGroupClient
	fakeScaleSetRegistrationClient
	fakeScaleSetMessageSessionClient
}

type fakeScaleSetPruneClient struct {
	fakeRunnerGroupClient
	fakeScaleSetListClient
	fakeScaleSetDeletionClient
}

type fakeRunnerGroupClient struct {
	getRunnerGroupByName func(ctx context.Context, name string) (*upstream.RunnerGroup, error)
}

func (client fakeRunnerGroupClient) GetRunnerGroupByName(
	ctx context.Context,
	name string,
) (*upstream.RunnerGroup, error) {
	if client.getRunnerGroupByName == nil {
		return nil, errors.New("unexpected GetRunnerGroupByName call")
	}
	return client.getRunnerGroupByName(ctx, name)
}

type fakeScaleSetListClient struct {
	listRunnerScaleSets func(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
}

func (client fakeScaleSetListClient) ListRunnerScaleSets(
	ctx context.Context,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	if client.listRunnerScaleSets == nil {
		return nil, errors.New("unexpected ListRunnerScaleSets call")
	}
	return client.listRunnerScaleSets(ctx, runnerGroupID)
}

type fakeScaleSetDeletionClient struct {
	deleteRunnerScaleSet func(ctx context.Context, runnerScaleSetID int) error
}

func (client fakeScaleSetDeletionClient) DeleteRunnerScaleSet(
	ctx context.Context,
	runnerScaleSetID int,
) error {
	if client.deleteRunnerScaleSet == nil {
		return errors.New("unexpected DeleteRunnerScaleSet call")
	}
	return client.deleteRunnerScaleSet(ctx, runnerScaleSetID)
}

type fakeScaleSetMessageSessionClient struct {
	messageSessionClient func(
		ctx context.Context,
		runnerScaleSetID int,
		runnerName string,
	) (scaleSetMessageClient, error)
}

func (client fakeScaleSetMessageSessionClient) MessageSessionClient(
	ctx context.Context,
	runnerScaleSetID int,
	runnerName string,
) (scaleSetMessageClient, error) {
	if client.messageSessionClient == nil {
		return scaleSetMessageClient{}, errors.New("unexpected MessageSessionClient call")
	}
	return client.messageSessionClient(ctx, runnerScaleSetID, runnerName)
}

type fakeScaleSetJITClient struct {
	generateJitRunnerConfig func(
		ctx context.Context,
		setting *upstream.RunnerScaleSetJitRunnerSetting,
		runnerScaleSetID int,
	) (*upstream.RunnerScaleSetJitRunnerConfig, error)
	getRunnerByName func(ctx context.Context, runnerName string) (*upstream.RunnerReference, error)
	removeRunner    func(ctx context.Context, runnerID int64) error
}

type fakeScaleSetRegistrationClient struct {
	getRunnerScaleSet func(
		ctx context.Context,
		runnerGroupID int,
		scaleSetName string,
	) (*upstream.RunnerScaleSet, error)
	createRunnerScaleSet func(
		ctx context.Context,
		runnerScaleSet *upstream.RunnerScaleSet,
	) (*upstream.RunnerScaleSet, error)
}

func (client fakeScaleSetRegistrationClient) GetRunnerScaleSet(
	ctx context.Context,
	runnerGroupID int,
	scaleSetName string,
) (*upstream.RunnerScaleSet, error) {
	if client.getRunnerScaleSet == nil {
		return nil, errors.New("unexpected GetRunnerScaleSet call")
	}
	return client.getRunnerScaleSet(ctx, runnerGroupID, scaleSetName)
}

func (client fakeScaleSetRegistrationClient) CreateRunnerScaleSet(
	ctx context.Context,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	if client.createRunnerScaleSet == nil {
		return nil, errors.New("unexpected CreateRunnerScaleSet call")
	}
	return client.createRunnerScaleSet(ctx, runnerScaleSet)
}

func (client fakeScaleSetJITClient) GenerateJitRunnerConfig(
	ctx context.Context,
	setting *upstream.RunnerScaleSetJitRunnerSetting,
	runnerScaleSetID int,
) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	if client.generateJitRunnerConfig == nil {
		return nil, errors.New("unexpected GenerateJitRunnerConfig call")
	}
	return client.generateJitRunnerConfig(ctx, setting, runnerScaleSetID)
}

func (client fakeScaleSetJITClient) GetRunnerByName(
	ctx context.Context,
	runnerName string,
) (*upstream.RunnerReference, error) {
	if client.getRunnerByName == nil {
		return nil, errors.New("unexpected GetRunnerByName call")
	}
	return client.getRunnerByName(ctx, runnerName)
}

func (client fakeScaleSetJITClient) RemoveRunner(ctx context.Context, runnerID int64) error {
	if client.removeRunner == nil {
		return errors.New("unexpected RemoveRunner call")
	}
	return client.removeRunner(ctx, runnerID)
}

type fakeMessageClient struct {
	getMessageErr error
	deleteErr     error
	closeErr      error
	closed        bool
	onGetMessage  func()
	polled        chan struct{}
	pollOnce      sync.Once
	message       *upstream.RunnerScaleSetMessage
	acquireErr    error
	acquiredIDs   []int64
	session       upstream.RunnerScaleSetSession
}

type blockingMessageClient struct {
	entered   chan struct{}
	exited    chan struct{}
	contexts  chan context.Context
	enterOnce sync.Once
	exitOnce  sync.Once
}

type emptyAcquisitionClient struct {
	fakeMessageClient
}

func (*emptyAcquisitionClient) AcquireJobs(context.Context, []int64) ([]int64, error) {
	return []int64{}, nil
}

func (client *blockingMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	if client.contexts != nil {
		client.contexts <- ctx
	}
	client.enterOnce.Do(func() {
		close(client.entered)
	})
	<-ctx.Done()
	client.exitOnce.Do(func() {
		close(client.exited)
	})
	return nil, fmt.Errorf("message receive stopped: %w", ctx.Err())
}

func (client *blockingMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	return nil
}

func (client *blockingMessageClient) Close(ctx context.Context) error {
	return nil
}

func (client *blockingMessageClient) Session() upstream.RunnerScaleSetSession {
	return upstream.RunnerScaleSetSession{}
}

func (client *fakeMessageClient) AcquireJobs(_ context.Context, requestIDs []int64) ([]int64, error) {
	if client.acquireErr != nil {
		return nil, client.acquireErr
	}
	client.acquiredIDs = append(client.acquiredIDs, requestIDs...)
	return requestIDs, nil
}

func newFakeMessageClient(onGetMessage func()) *fakeMessageClient {
	return &fakeMessageClient{
		onGetMessage: onGetMessage,
		polled:       make(chan struct{}),
	}
}

func (client *fakeMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	if client.polled != nil {
		client.pollOnce.Do(func() {
			close(client.polled)
		})
	}
	if client.onGetMessage != nil {
		client.onGetMessage()
	}
	if client.getMessageErr != nil {
		return nil, client.getMessageErr
	}
	return client.message, nil
}

func (client *fakeMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	if client.deleteErr != nil {
		return client.deleteErr
	}
	return nil
}

func (client *fakeMessageClient) Close(ctx context.Context) error {
	if client.closeErr != nil {
		return client.closeErr
	}
	client.closed = true
	return nil
}

func (client *fakeMessageClient) Session() upstream.RunnerScaleSetSession {
	return client.session
}

type fakeScalerProvider struct {
	listErr      error
	instances    []scaler.RunnerInstance
	started      []scaler.RunnerStartRequest
	trackStarted bool
}

type failingStartProvider struct{}

func (failingStartProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	return nil, nil
}

func (failingStartProvider) Start(context.Context, scaler.RunnerStartRequest) error {
	return errors.New("start failed")
}

func (failingStartProvider) Destroy(context.Context, scaler.RunnerInstance) error { return nil }

func (provider *fakeScalerProvider) List(ctx context.Context, scaleSetName string) ([]scaler.RunnerInstance, error) {
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	return provider.instances, nil
}

func (provider *fakeScalerProvider) Start(ctx context.Context, request scaler.RunnerStartRequest) error {
	provider.started = append(provider.started, request)
	if provider.trackStarted {
		provider.instances = append(provider.instances, scaler.RunnerInstance{RunnerName: request.RunnerName, State: scaler.RunnerStateRunning})
	}
	return nil
}

func (provider *fakeScalerProvider) Destroy(ctx context.Context, instance scaler.RunnerInstance) error {
	return nil
}

type fakeJITConfigGenerator struct{}

func (generator fakeJITConfigGenerator) Generate(ctx context.Context, runnerName string) (scaler.JITRunnerConfig, error) {
	return scaler.JITRunnerConfig{JITConfig: "jit"}, nil
}

func (generator fakeJITConfigGenerator) Remove(ctx context.Context, runner scaler.JITRunner) error {
	return nil
}

type recordingRunnerNameJITGenerator struct {
	runnerNames []string
}

func (generator *recordingRunnerNameJITGenerator) Generate(_ context.Context, runnerName string) (scaler.JITRunnerConfig, error) {
	generator.runnerNames = append(generator.runnerNames, runnerName)
	return scaler.JITRunnerConfig{JITConfig: "jit"}, nil
}

func (generator *recordingRunnerNameJITGenerator) Remove(context.Context, scaler.JITRunner) error {
	return nil
}
