package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestRunScaleSetPassesRepositoryToReconciler(t *testing.T) {
	scaleSets := scaleSetSessionOperations{
		RunnerHostname: func() string {
			return "host"
		},
		BuildReconciler: func(
			ctx context.Context,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			client scaleSetJITClient,
			scaleSetID int,
		) (scaler.Reconciler, error) {
			if repositoryURL != "https://github.com/meza/repo-a" {
				t.Fatalf("expected repository url, got %q", repositoryURL)
			}
			return scaler.Reconciler{}, errors.New("build failed")
		},
	}
	messageClient := &fakeMessageClient{}
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2},
		scaleSetMessageClient{Client: messageClient},
	)
	_, err := runTestScaleSet(context.Background(), slog.Default(), client, config.ScaleSetConfig{
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
	scaleSets := scaleSetSessionOperations{
		RunnerHostname: func() string {
			return "host"
		},
		BuildReconciler: func(
			ctx context.Context,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			client scaleSetJITClient,
			scaleSetID int,
		) (scaler.Reconciler, error) {
			return scaler.Reconciler{}, errors.New("build failed")
		},
	}
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2},
		scaleSetMessageClient{Client: &fakeMessageClient{closeErr: errors.New("close failed")}},
	)
	_, err := runTestScaleSet(context.Background(), slog.Default(), client, config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	}, scaleSets)
	if err == nil || !strings.Contains(err.Error(), "close message session") {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestStartCoordinatedScaleSetSurfacesRunnerGroupFailure(t *testing.T) {
	_, err := startCoordinatedScaleSet(context.Background(), slog.Default(), &fakeScaleSetClient{
		fakeScaleSetSessionClient: fakeScaleSetSessionClient{
			fakeRunnerGroupClient: fakeRunnerGroupClient{getRunnerGroupByName: func(context.Context, string) (*upstream.RunnerGroup, error) {
				return nil, errors.New("group failed")
			}},
		},
	}, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	}, scaler.NewCoordinator(1))
	if err == nil {
		t.Fatal("expected runner group error")
	}
}

func TestRunScaleSetStartsPollerForExistingScaleSet(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	scaleSet, err := runTestScaleSet(ctx, slog.Default(), fixture.client, fixture.config, fixture.operations)
	if err != nil {
		t.Fatalf("expected scale set success, got %v", err)
	}
	if scaleSet.StopPolling == nil || scaleSet.Running == nil || scaleSet.Shutdown == nil {
		t.Fatalf("expected complete scale set lifecycle, got %#v", scaleSet)
	}
	waitForPoll(t, fixture.messageClient)
	scaleSet.StopPolling()
	if err := scaleSet.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected shutdown success, got %v", err)
	}
	if !fixture.messageClient.closed {
		t.Fatal("expected message session to close")
	}
	if len(fixture.deletedScaleSetIDs) != 1 || fixture.deletedScaleSetIDs[0] != 20 {
		t.Fatalf("expected scale set deletion, got %#v", fixture.deletedScaleSetIDs)
	}
	if fixture.capturedScaleSetID != 20 {
		t.Fatalf("expected scale set ID 20, got %d", fixture.capturedScaleSetID)
	}
	if fixture.messageRunnerName != "host" {
		t.Fatalf("expected runner hostname, got %q", fixture.messageRunnerName)
	}
}

type existingScaleSetSessionFixture struct {
	messageClient      *fakeMessageClient
	client             *fakeScaleSetClient
	config             config.ScaleSetConfig
	operations         scaleSetSessionOperations
	capturedScaleSetID int
	deletedScaleSetIDs []int
	messageRunnerName  string
}

func newExistingScaleSetSessionFixture(t *testing.T) (*existingScaleSetSessionFixture, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fixture := &existingScaleSetSessionFixture{config: config.ScaleSetConfig{
		Name: "linux", RunnerGroup: "default", MaxRunners: 2,
	}}
	fixture.messageClient = newFakeMessageClient(cancel)
	fixture.messageClient.session.Statistics = &upstream.RunnerScaleSetStatistic{}
	fixture.operations = scaleSetSessionOperations{
		RunnerHostname:  func() string { return "host" },
		BuildReconciler: fixture.buildReconciler,
		PollRetryDelay:  time.Nanosecond,
	}
	fixture.client = fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 10}, &upstream.RunnerScaleSet{ID: 20},
		scaleSetMessageClient{Client: fixture.messageClient},
	)
	fixture.client.fakeScaleSetMessageSessionClient = fakeScaleSetMessageSessionClient{messageSessionClient: fixture.messageSession}
	fixture.client.fakeScaleSetDeletionClient = fakeScaleSetDeletionClient{deleteRunnerScaleSet: fixture.deleteScaleSet}
	return fixture, ctx
}

func (fixture *existingScaleSetSessionFixture) buildReconciler(
	_ context.Context,
	_ string,
	scaleSetConfig config.ScaleSetConfig,
	_ scaleSetJITClient,
	scaleSetID int,
) (scaler.Reconciler, error) {
	fixture.capturedScaleSetID = scaleSetID
	return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: scaleSetConfig.Name, MaxRunners: scaleSetConfig.MaxRunners,
		Provider: &fakeScalerProvider{}, JITConfig: fakeJITConfigGenerator{},
	}}, nil
}

func (fixture *existingScaleSetSessionFixture) messageSession(
	_ context.Context,
	_ int,
	runnerName string,
) (scaleSetMessageClient, error) {
	fixture.messageRunnerName = runnerName
	return scaleSetMessageClient{Client: fixture.messageClient}, nil
}

func (fixture *existingScaleSetSessionFixture) deleteScaleSet(_ context.Context, scaleSetID int) error {
	fixture.deletedScaleSetIDs = append(fixture.deletedScaleSetIDs, scaleSetID)
	return nil
}

func TestRunCoordinatedScaleSetStartsSharedSchedulerPoller(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	lifecycle, err := runCoordinatedScaleSet(
		ctx, logger, fixture.client, "https://github.com/acme/repo", fixture.config,
		scaler.NewCoordinator(2), fixture.operations,
	)
	if err != nil {
		t.Fatalf("expected coordinated lifecycle, got %v", err)
	}
	waitForPoll(t, fixture.messageClient)
	lifecycle.StopPolling()
	if lifecycle.DestroyActive == nil {
		t.Fatal("expected immediate cleanup capability")
	}
	if err := lifecycle.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected coordinated shutdown, got %v", err)
	}
	for _, action := range []string{
		"runner_group_lookup",
		"runner_scale_set_ensure",
		"message_session_create",
		"reconciler_build",
		"polling_start",
		"message_session_close",
		"runner_scale_set_delete",
	} {
		assertStructuredAction(t, logs.Bytes(), action, "started")
		assertStructuredAction(t, logs.Bytes(), action, "completed")
	}
}

func TestRunCoordinatedScaleSetSeedsAssignedDemandFromSessionStatistics(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	fixture.messageClient.session.Statistics = &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}
	provider := &fakeScalerProvider{trackStarted: true}
	fixture.operations.BuildReconciler = reconcilerBuilderForProvider(provider)
	coordinator := scaler.NewCoordinator(1)
	lifecycle, err := runCoordinatedScaleSet(
		ctx, slog.Default(), fixture.client, "https://github.com/acme/repo",
		fixture.config, coordinator, fixture.operations,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lifecycle.StopPolling()
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(provider.started) != 1 {
		t.Fatalf("expected initial session statistics to restore assigned demand, got %#v", provider.started)
	}
}

func TestRunCoordinatedScaleSetRejectsMissingInitialSessionStatistics(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	fixture.messageClient.session.Statistics = nil
	fixture.messageClient.missingSessionStatistics = true

	_, err := runCoordinatedScaleSet(
		ctx, slog.Default(), fixture.client, "https://github.com/acme/repo",
		fixture.config, scaler.NewCoordinator(1), fixture.operations,
	)
	if err == nil || !strings.Contains(err.Error(), "initial scale set session statistics are required") {
		t.Fatalf("startup error = %v", err)
	}
	if !fixture.messageClient.closed {
		t.Fatal("missing statistics did not close message session")
	}
}

func TestRunCoordinatedScaleSetReportsMissingStatisticsSessionCloseFailure(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	fixture.messageClient.session.Statistics = nil
	fixture.messageClient.missingSessionStatistics = true
	closeErr := errors.New("close failed")
	fixture.messageClient.closeErr = closeErr

	_, err := runCoordinatedScaleSet(
		ctx, slog.Default(), fixture.client, "https://github.com/acme/repo",
		fixture.config, scaler.NewCoordinator(1), fixture.operations,
	)
	if !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "initial scale set session statistics are required") {
		t.Fatalf("startup error = %v", err)
	}
}

func TestRunCoordinatedScaleSetTreatsInitialZeroAssignedJobsAsAuthoritative(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	fixture.messageClient.session.Statistics = &upstream.RunnerScaleSetStatistic{}
	provider := &fakeScalerProvider{trackStarted: true}
	fixture.operations.BuildReconciler = reconcilerBuilderForProvider(provider)
	coordinator := scaler.NewCoordinator(1)
	coordinator.ObserveAssigned("https://github.com/acme/repo", 1)

	lifecycle, err := runCoordinatedScaleSet(
		ctx, slog.Default(), fixture.client, "https://github.com/acme/repo",
		fixture.config, coordinator, fixture.operations,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lifecycle.StopPolling()
	if err = coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(provider.started) != 0 {
		t.Fatalf("initial zero statistics started runners: %#v", provider.started)
	}
}

func TestRunCoordinatedScaleSetLogsInitialStatisticsBeforePolling(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	fixture.messageClient.session.Statistics = &upstream.RunnerScaleSetStatistic{
		TotalAvailableJobs: 1, TotalAcquiredJobs: 2, TotalAssignedJobs: 3, TotalRunningJobs: 4,
		TotalRegisteredRunners: 5, TotalBusyRunners: 6, TotalIdleRunners: 7,
	}
	logs := &bytes.Buffer{}
	loggedBeforePoll := make(chan bool, 1)
	var observeLog sync.Once
	fixture.messageClient.onGetMessage = func() {
		observeLog.Do(func() {
			loggedBeforePoll <- containsStandaloneSessionStatistics(logs.String())
		})
	}

	lifecycle, err := runCoordinatedScaleSet(
		ctx, slog.New(slog.NewTextHandler(logs, nil)), fixture.client, "https://github.com/acme/repo",
		fixture.config, scaler.NewCoordinator(3), fixture.operations,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lifecycle.StopPolling()
	if logged := <-loggedBeforePoll; !logged {
		t.Fatalf("session statistics were not logged before polling: %q", logs.String())
	}
}

func containsStandaloneSessionStatistics(output string) bool {
	for _, expected := range []string{
		"action=session_statistics", "assignedJobs=3", "availableJobs=1", "acquiredJobs=2",
		"runningJobs=4", "registeredRunners=5", "busyRunners=6", "idleRunners=7",
	} {
		if !strings.Contains(output, expected) {
			return false
		}
	}
	return true
}

func reconcilerBuilderForProvider(provider *fakeScalerProvider) func(
	context.Context,
	string,
	config.ScaleSetConfig,
	scaleSetJITClient,
	int,
) (scaler.Reconciler, error) {
	return func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
}

func TestReconcilerPreparationFailureClosesMessageSession(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	messageClient := &fakeMessageClient{}
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2},
		scaleSetMessageClient{Client: messageClient},
	)
	_, err := runCoordinatedScaleSet(
		context.Background(), slog.Default(), client,
		"repo", config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, coordinator, scaleSetSessionOperations{
			RunnerHostname: func() string { return "host" },
			BuildReconciler: func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
				return scaler.Reconciler{}, errors.New("build failed")
			},
		},
	)
	if err == nil || !messageClient.closed {
		t.Fatalf("expected reconciler failure and session cleanup, got %v", err)
	}
}

func TestRunScaleSetCreatesMissingScaleSet(t *testing.T) {
	fixture, ctx := newExistingScaleSetSessionFixture(t)
	var missingScaleSet *upstream.RunnerScaleSet
	var createdScaleSetRequest *upstream.RunnerScaleSet
	fixture.client.fakeScaleSetRegistrationClient = fakeScaleSetRegistrationClient{
		getRunnerScaleSet: func(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
			return missingScaleSet, nil
		},
		createRunnerScaleSet: func(_ context.Context, runnerScaleSet *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error) {
			createdScaleSetRequest = runnerScaleSet
			return &upstream.RunnerScaleSet{ID: 30}, nil
		},
	}
	_, err := runTestScaleSet(ctx, slog.Default(), fixture.client, fixture.config, fixture.operations)
	if err != nil {
		t.Fatalf("expected scale set success, got %v", err)
	}
	waitForPoll(t, fixture.messageClient)
	if createdScaleSetRequest.Name != "linux" {
		t.Fatalf("expected created scale set request, got %#v", createdScaleSetRequest)
	}
}

func TestScaleSetShutdownReportsCloseAndDeleteFailures(t *testing.T) {
	deleteCalled := false
	client := fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(context.Context, int) error {
		deleteCalled = true
		return errors.New("delete failed")
	}}
	messageClient := scaleSetMessageClient{Client: &fakeMessageClient{closeErr: errors.New("close failed")}}

	err := shutdownScaleSet(
		context.Background(), slog.Default(), "repo", "linux",
		messageClient, client, &upstream.RunnerScaleSet{ID: 20},
	)
	if err == nil || !strings.Contains(err.Error(), "close message session") || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected joined close and delete errors, got %v", err)
	}
	if !deleteCalled {
		t.Fatal("expected scale set deletion attempt")
	}
}

func TestScaleSetSessionReportsRunnerGroupFailure(t *testing.T) {
	client := &fakeScaleSetClient{fakeScaleSetSessionClient: fakeScaleSetSessionClient{
		fakeRunnerGroupClient: fakeRunnerGroupClient{getRunnerGroupByName: func(context.Context, string) (*upstream.RunnerGroup, error) {
			return nil, errors.New("group failed")
		}},
	}}
	assertScaleSetSessionFailure(t, client, scaleSetSessionOperations{}, "get runner group")
}

func TestScaleSetSessionReportsScaleSetLookupFailure(t *testing.T) {
	client := &fakeScaleSetClient{fakeScaleSetSessionClient: fakeScaleSetSessionClient{
		fakeRunnerGroupClient: fakeRunnerGroupClient{getRunnerGroupByName: func(context.Context, string) (*upstream.RunnerGroup, error) {
			return &upstream.RunnerGroup{ID: 1}, nil
		}},
		fakeScaleSetRegistrationClient: fakeScaleSetRegistrationClient{getRunnerScaleSet: func(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
			return nil, errors.New("get failed")
		}},
	}}
	assertScaleSetSessionFailure(t, client, scaleSetSessionOperations{}, "ensure runner scale set")
}

func TestScaleSetSessionReportsScaleSetCreationFailure(t *testing.T) {
	var missingScaleSet *upstream.RunnerScaleSet
	client := &fakeScaleSetClient{fakeScaleSetSessionClient: fakeScaleSetSessionClient{
		fakeRunnerGroupClient: fakeRunnerGroupClient{getRunnerGroupByName: func(context.Context, string) (*upstream.RunnerGroup, error) {
			return &upstream.RunnerGroup{ID: 1}, nil
		}},
		fakeScaleSetRegistrationClient: fakeScaleSetRegistrationClient{
			getRunnerScaleSet: func(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
				return missingScaleSet, nil
			},
			createRunnerScaleSet: func(context.Context, *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error) {
				return nil, errors.New("create failed")
			},
		},
	}}
	assertScaleSetSessionFailure(t, client, scaleSetSessionOperations{}, "ensure runner scale set")
}

func TestScaleSetSessionReportsMessageSessionFailure(t *testing.T) {
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2},
		scaleSetMessageClient{},
	)
	client.fakeScaleSetMessageSessionClient = fakeScaleSetMessageSessionClient{messageSessionClient: func(context.Context, int, string) (scaleSetMessageClient, error) {
		return scaleSetMessageClient{}, errors.New("message failed")
	}}
	assertScaleSetSessionFailure(t, client, scaleSetSessionOperations{}, "create message session")
}

func TestScaleSetSessionReportsReconcilerConstructionFailure(t *testing.T) {
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2},
		scaleSetMessageClient{Client: &fakeMessageClient{}},
	)
	operations := scaleSetSessionOperations{BuildReconciler: func(
		context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int,
	) (scaler.Reconciler, error) {
		return scaler.Reconciler{}, errors.New("build failed")
	}}
	assertScaleSetSessionFailure(t, client, operations, "build reconciler")
}

func TestStartConfirmedCoordinatedScaleSetUsesConfirmedIdentityAndClosesOnBuildFailure(t *testing.T) {
	messageClient := &fakeMessageClient{}
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1}, &upstream.RunnerScaleSet{ID: 7, Name: "linux"},
		scaleSetMessageClient{Client: messageClient},
	)
	_, err := startConfirmedCoordinatedScaleSet(
		context.Background(), slog.Default(), client, "repo", config.ScaleSetConfig{Name: "linux", MaxRunners: 1},
		scaler.NewCoordinator(1), &upstream.RunnerScaleSet{ID: 7, Name: "linux"},
	)
	if err == nil || !messageClient.closed {
		t.Fatalf("error=%v closed=%t", err, messageClient.closed)
	}
}

func TestStartConfirmedCoordinatedScaleSetReportsMessageSessionFailure(t *testing.T) {
	client := &fakeScaleSetClient{}
	client.messageSessionClient = func(context.Context, int, string) (scaleSetMessageClient, error) {
		return scaleSetMessageClient{}, errors.New("session failed")
	}
	_, err := startConfirmedCoordinatedScaleSet(
		context.Background(), slog.Default(), client, "repo", config.ScaleSetConfig{Name: "linux"},
		scaler.NewCoordinator(1), &upstream.RunnerScaleSet{ID: 7},
	)
	if err == nil {
		t.Fatal("expected message session failure")
	}
}

func TestCoordinatedLifecycleCanCloseOnlyItsMessageSession(t *testing.T) {
	messageClient := &fakeMessageClient{}
	lifecycle := coordinatedScaleSetLifecycle(coordinatedLifecycleConfig{
		Logger: slog.Default(), RepositoryURL: "repo", ScaleSetConfig: config.ScaleSetConfig{Name: "linux"},
		MessageClient: scaleSetMessageClient{Client: messageClient},
	}, func() {}, closedChannel())
	if err := lifecycle.CloseSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !messageClient.closed {
		t.Fatal("message session was not closed")
	}
}

func assertScaleSetSessionFailure(
	t *testing.T,
	client scaleSetClient,
	operations scaleSetSessionOperations,
	expected string,
) {
	t.Helper()
	operations.RunnerHostname = func() string { return "host" }
	_, err := runTestScaleSet(
		context.Background(), slog.Default(), client,
		config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, operations,
	)
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected %q error, got %v", expected, err)
	}
}
