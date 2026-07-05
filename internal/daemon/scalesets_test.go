package daemon

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

func TestRunScaleSetPassesRepositoryToReconciler(t *testing.T) {
	scaleSets := scaleSetRuntime{
		RunnerHostname: func() string {
			return "host"
		},
		BuildReconciler: func(
			ctx context.Context,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			client scaleSetClient,
			scaleSetID int,
		) (scaler.Reconciler, error) {
			if repositoryURL != "https://github.com/meza/repo-a" {
				t.Fatalf("expected repository url, got %q", repositoryURL)
			}
			return scaler.Reconciler{}, errors.New("build failed")
		},
	}
	messageClient := &fakeMessageClient{}
	client := &fakeScaleSetClient{
		runnerGroup:    &upstream.RunnerGroup{ID: 1},
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
		messageClient:  scaleSetMessageClient{Client: messageClient},
	}
	_, err := runScaleSet(context.Background(), slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	}, scaleSets)
	if err == nil || !strings.Contains(err.Error(), "build reconciler") {
		t.Fatalf("expected reconciler error, got %v", err)
	}
	if !messageClient.closed {
		t.Fatal("expected message session to close after reconciler failure")
	}
}

func TestRunScaleSetSurfacesCloseFailureAfterReconcilerFailure(t *testing.T) {
	scaleSets := scaleSetRuntime{
		RunnerHostname: func() string {
			return "host"
		},
		BuildReconciler: func(
			ctx context.Context,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			client scaleSetClient,
			scaleSetID int,
		) (scaler.Reconciler, error) {
			return scaler.Reconciler{}, errors.New("build failed")
		},
	}
	client := &fakeScaleSetClient{
		runnerGroup:    &upstream.RunnerGroup{ID: 1},
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
		messageClient:  scaleSetMessageClient{Client: &fakeMessageClient{closeErr: errors.New("close failed")}},
	}
	_, err := runScaleSet(context.Background(), slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	}, scaleSets)
	if err == nil || !strings.Contains(err.Error(), "close message session") {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestStartScaleSetUsesProductionRuntime(t *testing.T) {
	_, err := startScaleSet(context.Background(), slog.Default(), &fakeScaleSetClient{
		runnerGroupErr: errors.New("group failed"),
	}, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	})
	if err == nil {
		t.Fatal("expected runner group error")
	}
}

func TestRunScaleSetStartsPollerForExistingScaleSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messageClient := newFakeMessageClient(cancel)
	var capturedScaleSetID int
	scaleSets := scaleSetRuntime{
		RunnerHostname: func() string {
			return "host"
		},
		BuildReconciler: func(
			ctx context.Context,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			client scaleSetClient,
			scaleSetID int,
		) (scaler.Reconciler, error) {
			capturedScaleSetID = scaleSetID
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
				Name:       scaleSetConfig.Name,
				MaxRunners: scaleSetConfig.MaxRunners,
				Provider:   &fakeScalerProvider{},
				JITConfig:  fakeJITConfigGenerator{},
			}}, nil
		},
		PollRetryDelay: time.Nanosecond,
	}
	client := &fakeScaleSetClient{
		runnerGroup:    &upstream.RunnerGroup{ID: 10},
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		messageClient:  scaleSetMessageClient{Client: messageClient},
	}

	scaleSet, err := runScaleSet(ctx, slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
		MaxRunners:  2,
	}, scaleSets)
	if err != nil {
		t.Fatalf("expected scale set success, got %v", err)
	}
	if scaleSet.StopPolling == nil || scaleSet.Drain == nil || scaleSet.Shutdown == nil {
		t.Fatalf("expected complete scale set lifecycle, got %#v", scaleSet)
	}
	waitForPoll(t, messageClient)
	scaleSet.StopPolling()
	if err := scaleSet.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected shutdown success, got %v", err)
	}
	if !messageClient.closed {
		t.Fatal("expected message session to close")
	}
	if capturedScaleSetID != 20 {
		t.Fatalf("expected scale set ID 20, got %d", capturedScaleSetID)
	}
	if client.messageRunnerName != "host" {
		t.Fatalf("expected runner hostname, got %q", client.messageRunnerName)
	}
}

func TestRunScaleSetCreatesMissingScaleSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messageClient := newFakeMessageClient(cancel)
	scaleSets := scaleSetRuntime{
		RunnerHostname: func() string {
			return "host"
		},
		BuildReconciler: func(
			ctx context.Context,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			client scaleSetClient,
			scaleSetID int,
		) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
				Name:       scaleSetConfig.Name,
				MaxRunners: scaleSetConfig.MaxRunners,
				Provider:   &fakeScalerProvider{},
				JITConfig:  fakeJITConfigGenerator{},
			}}, nil
		},
		PollRetryDelay: time.Nanosecond,
	}
	client := &fakeScaleSetClient{
		runnerGroup:     &upstream.RunnerGroup{ID: 10},
		createdScaleSet: &upstream.RunnerScaleSet{ID: 30},
		messageClient:   scaleSetMessageClient{Client: messageClient},
	}

	_, err := runScaleSet(ctx, slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
		MaxRunners:  2,
	}, scaleSets)
	if err != nil {
		t.Fatalf("expected scale set success, got %v", err)
	}
	waitForPoll(t, messageClient)
	if client.createdScaleSetRequest.Name != "linux" {
		t.Fatalf("expected created scale set request, got %#v", client.createdScaleSetRequest)
	}
}

func TestRunScaleSetSurfacesClientAndRuntimeFailures(t *testing.T) {
	tests := map[string]struct {
		client    *fakeScaleSetClient
		scaleSets scaleSetRuntime
		expected  string
	}{
		"runner group": {
			client:   &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")},
			expected: "get runner group",
		},
		"get scale set": {
			client: &fakeScaleSetClient{
				runnerGroup:       &upstream.RunnerGroup{ID: 1},
				runnerScaleSetErr: errors.New("get failed"),
			},
			expected: "ensure runner scale set",
		},
		"create scale set": {
			client: &fakeScaleSetClient{
				runnerGroup:       &upstream.RunnerGroup{ID: 1},
				createScaleSetErr: errors.New("create failed"),
			},
			expected: "ensure runner scale set",
		},
		"message session": {
			client: &fakeScaleSetClient{
				runnerGroup:       &upstream.RunnerGroup{ID: 1},
				runnerScaleSet:    &upstream.RunnerScaleSet{ID: 2},
				messageSessionErr: errors.New("message failed"),
			},
			expected: "create message session",
		},
		"reconciler": {
			client: &fakeScaleSetClient{
				runnerGroup:    &upstream.RunnerGroup{ID: 1},
				runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
				messageClient:  scaleSetMessageClient{Client: &fakeMessageClient{}},
			},
			scaleSets: scaleSetRuntime{
				RunnerHostname: func() string {
					return "host"
				},
				BuildReconciler: func(
					ctx context.Context,
					repositoryURL string,
					scaleSetConfig config.ScaleSetConfig,
					client scaleSetClient,
					scaleSetID int,
				) (scaler.Reconciler, error) {
					return scaler.Reconciler{}, errors.New("build failed")
				},
			},
			expected: "build reconciler",
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			if testCase.scaleSets.RunnerHostname == nil {
				testCase.scaleSets.RunnerHostname = func() string {
					return "host"
				}
			}
			_, err := runScaleSet(context.Background(), slog.Default(), testCase.client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
				Name:        "linux",
				RunnerGroup: "default",
			}, testCase.scaleSets)
			if err == nil || !strings.Contains(err.Error(), testCase.expected) {
				t.Fatalf("expected %q error, got %v", testCase.expected, err)
			}
		})
	}
}

func TestScaleSetMessageClientForwardsMessages(t *testing.T) {
	message := &upstream.RunnerScaleSetMessage{MessageID: 7}
	client := scaleSetMessageClient{Client: &fakeMessageClient{message: message}}
	receivedMessage, err := client.GetMessage(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("expected message success, got %v", err)
	}
	if receivedMessage.MessageID != 7 {
		t.Fatalf("expected forwarded message, got %#v", receivedMessage)
	}
	if err := client.DeleteMessage(context.Background(), 7); err != nil {
		t.Fatalf("expected delete success, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected close success, got %v", err)
	}
}

func TestScaleSetMessageClientWrapsErrors(t *testing.T) {
	client := scaleSetMessageClient{Client: &fakeMessageClient{
		getMessageErr: errors.New("get failed"),
		deleteErr:     errors.New("delete failed"),
		closeErr:      errors.New("close failed"),
	}}
	if _, err := client.GetMessage(context.Background(), 1, 2); err == nil {
		t.Fatal("expected get error")
	}
	if err := client.DeleteMessage(context.Background(), 7); err == nil {
		t.Fatal("expected delete error")
	}
	if err := client.Close(context.Background()); err == nil {
		t.Fatal("expected close error")
	}
}

func TestEnsureRunnerScaleSetReturnsExistingOrCreatesNew(t *testing.T) {
	existing, err := ensureRunnerScaleSet(context.Background(), &fakeScaleSetClient{
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
	}, 10, "linux")
	if err != nil {
		t.Fatalf("expected existing success, got %v", err)
	}
	if existing.ID != 1 {
		t.Fatalf("expected existing scale set, got %#v", existing)
	}

	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	created, err := ensureRunnerScaleSet(context.Background(), client, 10, "linux")
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	if created.ID != 2 {
		t.Fatalf("expected created scale set, got %#v", created)
	}
}

func TestRunnerHostnameReturnsHostName(t *testing.T) {
	if runnerHostname() == "" {
		t.Fatal("expected hostname")
	}
}

func TestRunnerHostnameFallsBackWhenHostNameFails(t *testing.T) {
	originalHostname := operatingSystemHostname
	defer func() {
		operatingSystemHostname = originalHostname
	}()
	operatingSystemHostname = func() (string, error) {
		return "", errors.New("hostname failed")
	}
	if runnerHostname() != "github-scaler" {
		t.Fatal("expected fallback hostname")
	}
}

func TestPollUntilShutdownHandlesPollErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}
	pollUntilShutdown(ctx, slog.Default(), "linux", poller, time.Nanosecond)
}

func TestScaleSetHandlerReconcilesAssignedJobs(t *testing.T) {
	provider := &fakeScalerProvider{}
	handler := scaleSetHandler{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name:       "linux",
		MaxRunners: 3,
		Provider:   provider,
		JITConfig:  fakeJITConfigGenerator{},
	}}}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
	})
	if err != nil {
		t.Fatalf("expected handler success, got %v", err)
	}
	if len(provider.started) != 2 {
		t.Fatalf("expected two starts, got %#v", provider.started)
	}
}

func TestScaleSetHandlerSurfacesReconcileFailure(t *testing.T) {
	handler := scaleSetHandler{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name:       "linux",
		MaxRunners: 1,
		Provider:   &fakeScalerProvider{listErr: errors.New("list failed")},
		JITConfig:  fakeJITConfigGenerator{},
	}}}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	})
	if err == nil {
		t.Fatal("expected handler error")
	}
}

func TestJITGeneratorUsesScaleSetClient(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{jitConfig: "encoded"}, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success, got %v", err)
	}
	if jitConfig != "encoded" {
		t.Fatalf("expected encoded jit, got %q", jitConfig)
	}
}

func TestJITGeneratorSurfacesClientFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{jitErr: errors.New("jit failed")}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected jit error")
	}
}

func waitForPoll(t *testing.T, messageClient *fakeMessageClient) {
	t.Helper()
	select {
	case <-messageClient.polled:
	case <-time.After(time.Second):
		t.Fatal("expected poll")
	}
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
	jitErr                 error
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
	if client.jitErr != nil {
		return nil, client.jitErr
	}
	return &upstream.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: client.jitConfig}, nil
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

type fakeScalerProvider struct {
	listErr error
	started []scaler.RunnerStartRequest
}

func (provider *fakeScalerProvider) List(ctx context.Context, scaleSetName string) ([]scaler.RunnerInstance, error) {
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	return nil, nil
}

func (provider *fakeScalerProvider) Start(ctx context.Context, request scaler.RunnerStartRequest) error {
	provider.started = append(provider.started, request)
	return nil
}

func (provider *fakeScalerProvider) Destroy(ctx context.Context, instance scaler.RunnerInstance) error {
	return nil
}

type fakeJITConfigGenerator struct{}

func (generator fakeJITConfigGenerator) Generate(ctx context.Context, runnerName string) (string, error) {
	return "jit", nil
}
