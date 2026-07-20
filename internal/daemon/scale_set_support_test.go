package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
)

func runTestScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	operations scaleSetSessionOperations,
) (scaleSetLifecycle, error) {
	return runCoordinatedScaleSet(
		ctx, logger, client, repositoryURL, scaleSetConfig,
		scaler.NewCoordinator(scaleSetConfig.MaxRunners), operations,
	)
}

type fakeScaleSetClient struct {
	runnerGroup            *upstream.RunnerGroup
	runnerGroupErr         error
	runnerScaleSet         *upstream.RunnerScaleSet
	runnerScaleSetErr      error
	runnerScaleSets        []upstream.RunnerScaleSet
	listScaleSetsErr       error
	createdScaleSet        *upstream.RunnerScaleSet
	createdScaleSetRequest *upstream.RunnerScaleSet
	createScaleSetErr      error
	deletedScaleSetIDs     []int
	deleteScaleSetErr      error
	messageClient          scaleSetMessageClient
	messageRunnerName      string
	messageSessionErr      error
	jitConfig              string
	jitRunner              *upstream.RunnerReference
	jitErr                 error
	jitRunnerAlreadyExists bool
	jitRetryErr            error
	jitAttempts            int
	generatedRunnerNames   []string
	runnerByName           *upstream.RunnerReference
	runnerByNameErr        error
	runnerNameLookups      []string
	removedRunnerIDs       []int64
	removeRunnerErr        error
}

func (client *fakeScaleSetClient) GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error) {
	if client.runnerGroupErr != nil {
		return nil, client.runnerGroupErr
	}
	return client.runnerGroup, nil
}

func (client *fakeScaleSetClient) GetRunnerScaleSet(ctx context.Context, runnerGroupID int, scaleSetName string) (*upstream.RunnerScaleSet, error) {
	if client.runnerScaleSetErr != nil {
		return nil, client.runnerScaleSetErr
	}
	return client.runnerScaleSet, nil
}

func (client *fakeScaleSetClient) ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error) {
	if client.listScaleSetsErr != nil {
		return nil, client.listScaleSetsErr
	}
	return client.runnerScaleSets, nil
}

func (client *fakeScaleSetClient) CreateRunnerScaleSet(
	ctx context.Context,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	client.createdScaleSetRequest = runnerScaleSet
	if client.createScaleSetErr != nil {
		return nil, client.createScaleSetErr
	}
	return client.createdScaleSet, nil
}

func (client *fakeScaleSetClient) DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error {
	if client.deleteScaleSetErr != nil {
		return client.deleteScaleSetErr
	}
	client.deletedScaleSetIDs = append(client.deletedScaleSetIDs, runnerScaleSetID)
	return nil
}

func (client *fakeScaleSetClient) MessageSessionClient(
	ctx context.Context,
	runnerScaleSetID int,
	runnerName string,
) (scaleSetMessageClient, error) {
	client.messageRunnerName = runnerName
	if client.messageSessionErr != nil {
		return scaleSetMessageClient{}, client.messageSessionErr
	}
	return client.messageClient, nil
}

func (client *fakeScaleSetClient) GenerateJitRunnerConfig(
	ctx context.Context,
	setting *upstream.RunnerScaleSetJitRunnerSetting,
	runnerScaleSetID int,
) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	client.generatedRunnerNames = append(client.generatedRunnerNames, setting.Name)
	client.jitAttempts++
	if client.jitRunnerAlreadyExists && client.jitAttempts == 1 {
		return nil, upstream.RunnerExistsError
	}
	if client.jitAttempts > 1 && client.jitRetryErr != nil {
		return nil, client.jitRetryErr
	}
	if client.jitErr != nil {
		return nil, client.jitErr
	}
	return &upstream.RunnerScaleSetJitRunnerConfig{
		Runner:           client.jitRunner,
		EncodedJITConfig: client.jitConfig,
	}, nil
}

func (client *fakeScaleSetClient) GetRunnerByName(ctx context.Context, runnerName string) (*upstream.RunnerReference, error) {
	client.runnerNameLookups = append(client.runnerNameLookups, runnerName)
	if client.runnerByNameErr != nil {
		return nil, client.runnerByNameErr
	}
	return client.runnerByName, nil
}

func (client *fakeScaleSetClient) RemoveRunner(ctx context.Context, runnerID int64) error {
	if client.removeRunnerErr != nil {
		return client.removeRunnerErr
	}
	client.removedRunnerIDs = append(client.removedRunnerIDs, runnerID)
	return nil
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
