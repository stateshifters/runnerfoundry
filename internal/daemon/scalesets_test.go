package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	if len(client.deletedScaleSetIDs) != 1 || client.deletedScaleSetIDs[0] != 20 {
		t.Fatalf("expected scale set deletion, got %#v", client.deletedScaleSetIDs)
	}
	if capturedScaleSetID != 20 {
		t.Fatalf("expected scale set ID 20, got %d", capturedScaleSetID)
	}
	if client.messageRunnerName != "host" {
		t.Fatalf("expected runner hostname, got %q", client.messageRunnerName)
	}
}

func TestRunCoordinatedScaleSetStartsSharedSchedulerPoller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	messageClient := newFakeMessageClient(cancel)
	scaleSets := scaleSetRuntime{
		RunnerHostname: func() string { return "host" },
		BuildReconciler: func(
			context.Context,
			string,
			config.ScaleSetConfig,
			scaleSetClient,
			int,
		) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
				Name: "local", MaxRunners: 2, Provider: &fakeScalerProvider{}, JITConfig: fakeJITConfigGenerator{},
			}}, nil
		},
		PollRetryDelay: time.Nanosecond,
	}
	client := &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 10}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		messageClient: scaleSetMessageClient{Client: messageClient},
	}
	lifecycle, err := runCoordinatedScaleSet(
		ctx, logger, client, "https://github.com/acme/repo", config.ScaleSetConfig{
			Name: "linux", RunnerGroup: "default", MaxRunners: 2,
		}, scaler.NewCoordinator(2), scaleSets,
	)
	if err != nil {
		t.Fatalf("expected coordinated lifecycle, got %v", err)
	}
	waitForPoll(t, messageClient)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messageClient := newFakeMessageClient(cancel)
	messageClient.session.Statistics = &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(1)
	client := &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 10}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		messageClient: scaleSetMessageClient{Client: messageClient},
	}
	lifecycle, err := runCoordinatedScaleSet(
		ctx, slog.Default(), client, "https://github.com/acme/repo", config.ScaleSetConfig{
			Name: "linux", RunnerGroup: "default", MaxRunners: 1,
		}, coordinator, scaleSetRuntime{
			RunnerHostname: func() string { return "host" },
			BuildReconciler: func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
				return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
					Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
				}}, nil
			},
			PollRetryDelay: time.Nanosecond,
		},
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

func TestRunCoordinatedScaleSetSurfacesPreparationAndReconcilerFailures(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	_, err := runCoordinatedScaleSet(
		context.Background(), slog.Default(), &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")},
		"repo", config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, coordinator, scaleSetRuntime{
			RunnerHostname: func() string { return "host" },
		},
	)
	if err == nil {
		t.Fatal("expected preparation failure")
	}
	messageClient := &fakeMessageClient{}
	_, err = runCoordinatedScaleSet(
		context.Background(), slog.Default(), &fakeScaleSetClient{
			runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
			messageClient: scaleSetMessageClient{Client: messageClient},
		}, "repo", config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, coordinator, scaleSetRuntime{
			RunnerHostname: func() string { return "host" },
			BuildReconciler: func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
				return scaler.Reconciler{}, errors.New("build failed")
			},
		},
	)
	if err == nil || !messageClient.closed {
		t.Fatalf("expected reconciler failure and session cleanup, got %v", err)
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

func TestScaleSetShutdownReportsCloseAndDeleteFailures(t *testing.T) {
	client := &fakeScaleSetClient{deleteScaleSetErr: errors.New("delete failed")}
	messageClient := scaleSetMessageClient{Client: &fakeMessageClient{closeErr: errors.New("close failed")}}

	err := shutdownScaleSet(
		context.Background(), slog.Default(), "repo", "linux",
		messageClient, client, &upstream.RunnerScaleSet{ID: 20},
	)
	if err == nil || !strings.Contains(err.Error(), "close message session") || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected joined close and delete errors, got %v", err)
	}
	if len(client.deletedScaleSetIDs) != 0 {
		t.Fatalf("expected failed delete not to record success, got %#v", client.deletedScaleSetIDs)
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
	}, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected existing success, got %v", err)
	}
	if existing.ID != 1 {
		t.Fatalf("expected existing scale set, got %#v", existing)
	}

	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	created, err := ensureRunnerScaleSet(context.Background(), client, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	if created.ID != 2 {
		t.Fatalf("expected created scale set, got %#v", created)
	}
}

func TestEnsureRunnerScaleSetUsesConfiguredLabels(t *testing.T) {
	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	_, err := ensureRunnerScaleSet(context.Background(), client, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	assertScaleSetLabels(t, client.createdScaleSetRequest.Labels, []string{"linux"})
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
	pollCount := 0
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage: func() {
			pollCount++
			if pollCount > 1 {
				cancel()
			}
		},
	}, Handler: scaleSetHandler{}}
	pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, time.Nanosecond)
}

func TestPollUntilShutdownExitsDuringRetryDelayWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}
	returned := make(chan struct{})

	go func() {
		pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, time.Hour)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("expected poller to exit promptly after cancellation")
	}
}

func TestPollUntilShutdownExitsWhenCanceledDuringRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	firstPoll := make(chan struct{})
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage: func() {
			select {
			case <-firstPoll:
			default:
				close(firstPoll)
			}
		},
	}, Handler: scaleSetHandler{}}
	returned := make(chan struct{})

	go func() {
		pollUntilShutdown(ctx, slog.Default(), "repo", "linux", poller, time.Hour)
		close(returned)
	}()

	select {
	case <-firstPoll:
	case <-time.After(time.Second):
		t.Fatal("expected first poll")
	}
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("expected poller to exit during retry delay")
	}
}

func TestPollUntilShutdownDoesNotLogShutdownCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: context.Canceled,
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "repo", "linux", poller, time.Nanosecond)

	if logs.Len() != 0 {
		t.Fatalf("expected no shutdown cancellation log, got %q", logs.String())
	}
}

func TestPollUntilShutdownLogsPollActionsAtDebugLevel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: context.Canceled,
		onGetMessage:  cancel,
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "https://github.com/acme/repo", "linux", poller, time.Nanosecond)

	assertStructuredAction(t, logs.Bytes(), "message_poll", "started")
	assertStructuredAction(t, logs.Bytes(), "message_poll", "canceled")
}

func TestPollUntilShutdownLogsSustainedFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pollCount := 0
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	poller := &scalesetpoller.Poller{Client: &fakeMessageClient{
		getMessageErr: errors.New("poll failed"),
		onGetMessage: func() {
			pollCount++
			if pollCount > sustainedPollFailureThreshold {
				cancel()
			}
		},
	}, Handler: scaleSetHandler{}}

	pollUntilShutdown(ctx, logger, "https://github.com/acme/repo", "linux", poller, time.Nanosecond)

	if !strings.Contains(logs.String(), "sustained poll failures") || !strings.Contains(logs.String(), "repository=https://github.com/acme/repo") {
		t.Fatalf("expected sustained failure log, got %q", logs.String())
	}
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

func TestCoordinatedScaleSetHandlerQueuesAvailableAndAssignedJobsAndRemovesCompleted(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(2)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	queueTime := time.Now()
	message := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: queueTime}}},
		JobAssignedMessages:  []*upstream.JobAssigned{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2, QueueTime: queueTime.Add(time.Second)}}},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2}}},
	}
	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatalf("expected message scheduling, got %v", err)
	}
	if len(provider.started) != 1 {
		t.Fatalf("expected only non-completed demand to start, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerUsesStatisticsForZeroIdentityAssignedJobs(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(2)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
	})
	handler := coordinatedScaleSetHandler{Logger: logger, ScaleSet: "self-hosted", Coordinator: coordinator, Repository: "repo"}
	message := &upstream.RunnerScaleSetMessage{
		Statistics:          &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
		JobAssignedMessages: []*upstream.JobAssigned{{}, {}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatalf("expected assigned capacity reconciliation, got %v", err)
	}
	if len(provider.started) != 2 {
		t.Fatalf("expected two assigned runners, got %#v", provider.started)
	}
	for _, startedRunner := range provider.started {
		if strings.HasSuffix(startedRunner.RunnerName, "-0") {
			t.Fatalf("expected assigned runner without zero request identity, got %q", startedRunner.RunnerName)
		}
	}
	if !strings.Contains(logs.String(), "assigned capacity reconciled") || !strings.Contains(logs.String(), "startedRunners=2") || strings.Contains(logs.String(), "requestID=0") {
		t.Fatalf("expected aggregate assigned-capacity log without fabricated identity, got %q", logs.String())
	}
}

func TestCoordinatedScaleSetHandlerAppliesLowerAssignedCountBeforeScheduling(t *testing.T) {
	capacityHolder := &recordingRecoveryWorker{running: 1}
	waitingWorker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("capacity-holder", capacityHolder)
	coordinator.AddRepository("waiting", waitingWorker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "waiting"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}

	capacityHolder.running = 0
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
	}); err != nil {
		t.Fatal(err)
	}
	if waitingWorker.assigned != 0 {
		t.Fatalf("expected current zero assigned count to prevent stale provisioning, got %d", waitingWorker.assigned)
	}
}

func TestCoordinatedScaleSetHandlerRejectsMalformedAvailableDemand(t *testing.T) {
	worker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}

	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{}},
	})
	if err == nil || !strings.Contains(err.Error(), "positive runner request ID and queue time") {
		t.Fatalf("expected malformed available demand rejection, got %v", err)
	}
	if len(worker.acquired) != 0 || len(worker.started) != 0 {
		t.Fatalf("expected no malformed demand side effects, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRequiresStatisticsAndStartsAssignedWorker(t *testing.T) {
	worker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{}); err == nil {
		t.Fatal("expected missing statistics failure")
	}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if worker.assigned != 1 {
		t.Fatalf("expected assigned worker start, got %d", worker.assigned)
	}
}

func TestCoordinatedScaleSetHandlerIgnoresZeroIdentityCompletion(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 48, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobStartedMessages:   []*upstream.JobStarted{{}},
		JobCompletedMessages: []*upstream.JobCompleted{{}},
	}); err != nil {
		t.Fatal(err)
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 1 || worker.started[0] != 48 {
		t.Fatalf("expected valid demand to survive zero-ID completion, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRemovesRepeatedPositiveIdentityCompletionIdempotently(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 49, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	completion := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 49}}},
	}
	for range 2 {
		if err := handler.HandleMessage(context.Background(), completion); err != nil {
			t.Fatal(err)
		}
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 0 {
		t.Fatalf("expected positive completion identity to remove demand, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRetainsDemandUntilJobStarted(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 50, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:          &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAssignedMessages: []*upstream.JobAssigned{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 50}}},
	}); err != nil {
		t.Fatal(err)
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 1 || worker.started[0] != 50 {
		t.Fatalf("expected JobAssigned demand to remain provisionable, got %#v", worker.started)
	}

	worker.running = 1
	started := &upstream.RunnerScaleSetMessage{
		Statistics:         &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 50}}},
	}
	for range 2 {
		if err := handler.HandleMessage(context.Background(), started); err != nil {
			t.Fatal(err)
		}
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 1 {
		t.Fatalf("expected JobStarted to complete durable demand, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerLeavesAssignedReconciliationRetryable(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 1, Provider: failingStartProvider{}, JITConfig: fakeJITConfigGenerator{},
	}}})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:          &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAssignedMessages: []*upstream.JobAssigned{{}},
	})
	if err == nil || !strings.Contains(err.Error(), "schedule shared runner capacity") {
		t.Fatalf("expected retryable assigned reconciliation failure, got %v", err)
	}
}

func TestCoordinatedScaleSetLogsJobServingLifecycle(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	provider := &fakeScalerProvider{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Logger: logger, Repository: "repo", ScaleSet: "linux",
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Logger: logger, ScaleSet: "linux", Coordinator: coordinator, Repository: "repo"}
	queueTime := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: queueTime}}},
	}); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	for _, expected := range []string{
		"job demand observed", "status=available", "requestID=42", "repository=repo", "scaleSet=linux",
		"job acquired", "runner started",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected log %q in %q", expected, output)
		}
	}
}

func TestCoordinatedScaleSetHandlerLeavesMessageRetryableWhenSchedulingFails(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: &fakeScalerProvider{listErr: errors.New("list failed")},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: time.Now()}}},
	})
	if err == nil || !strings.Contains(err.Error(), "schedule shared runner capacity") {
		t.Fatalf("expected scheduling failure, got %v", err)
	}
}

func TestCoordinatedScaleSetHandlerDoesNotDuplicateRunnerWhenAvailableDemandIsRedelivered(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	message := &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 50, QueueTime: time.Now(),
		}}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(provider.started) != 1 {
		t.Fatalf("expected redelivery to reuse the provider-visible runner, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerLeavesMessageRetryableWhenPersistenceFails(t *testing.T) {
	coordinator := scaler.NewDurableCoordinator("linux", 1, failingDaemonDemandStore{})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: time.Now()}}},
	})
	if err == nil || !strings.Contains(err.Error(), "persist GitHub job demand") {
		t.Fatalf("expected persistence failure, got %v", err)
	}
}

type failingDaemonDemandStore struct{}

func (failingDaemonDemandStore) Apply(context.Context, string, []scaler.JobDemand, []scaler.DemandRemoval) error {
	return errors.New("persist failed")
}
func (failingDaemonDemandStore) RemoveRepository(context.Context, string, string) error { return nil }
func (failingDaemonDemandStore) RemoveScaleSet(context.Context, string) error           { return nil }
func (failingDaemonDemandStore) Ordered(context.Context, string) ([]scaler.QueuedDemand, error) {
	return nil, nil
}
func (failingDaemonDemandStore) Delete(context.Context, string, string, int64) error { return nil }

func TestScaleSetMessageClientRequiresConfirmedJobAcquisition(t *testing.T) {
	if err := (scaleSetMessageClient{Client: &blockingMessageClient{}}).Acquire(context.Background(), 1); err == nil {
		t.Fatal("expected unsupported acquisition error")
	}
	if err := (scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: errors.New("acquire failed")}}).Acquire(context.Background(), 1); err == nil {
		t.Fatal("expected acquisition request error")
	}
	if err := (scaleSetMessageClient{Client: &emptyAcquisitionClient{}}).Acquire(context.Background(), 1); err == nil {
		t.Fatal("expected unconfirmed acquisition error")
	}
}

func TestUnconfirmedJobAcquisitionPreservesDemandWithoutStartingRunner(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &emptyAcquisitionClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 51, QueueTime: time.Now(),
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "job 51 was not acquired") {
		t.Fatalf("expected unconfirmed acquisition failure, got %v", err)
	}
	if len(provider.started) != 0 {
		t.Fatalf("expected no runner for unconfirmed acquisition, got %#v", provider.started)
	}

	recoveryWorker := &recordingRecoveryWorker{}
	coordinator.AddRepository("repo", recoveryWorker)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(recoveryWorker.acquired) != 1 || recoveryWorker.acquired[0] != 51 || len(recoveryWorker.started) != 1 {
		t.Fatalf("expected preserved demand to remain recoverable, acquired=%#v started=%#v", recoveryWorker.acquired, recoveryWorker.started)
	}
}

func TestCoordinatedRepositoryWorkerWrapsRunnerStartFailure(t *testing.T) {
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 1, Provider: failingStartProvider{}, JITConfig: fakeJITConfigGenerator{},
	}}}
	if err := worker.StartOne(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "start repository runner") {
		t.Fatalf("expected wrapped runner start failure, got %v", err)
	}
	worker.MessageClient = scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: errors.New("acquire failed")}}
	if err := worker.Acquire(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "acquire failed") {
		t.Fatalf("expected acquisition failure, got %v", err)
	}
	worker.Reconciler.ScaleSet.Provider = &fakeScalerProvider{listErr: errors.New("list failed")}
	if _, err := worker.Exists(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "inspect runner") {
		t.Fatalf("expected wrapped runner inspection failure, got %v", err)
	}
}

func TestCoordinatedRepositoryWorkerUsesRepositoryQualifiedRunnerName(t *testing.T) {
	provider := &fakeScalerProvider{instances: []scaler.RunnerInstance{{
		RunnerName: "github-com-meza-repo-a-self-hosted-42",
		State:      scaler.RunnerStateRunning,
	}}}
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "github-com-meza-repo-a-self-hosted", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
	}}}

	if err := worker.StartOne(context.Background(), 42); err != nil {
		t.Fatalf("expected runner start success, got %v", err)
	}
	if len(provider.started) != 1 || provider.started[0].RunnerName != "github-com-meza-repo-a-self-hosted-42" {
		t.Fatalf("expected repository-qualified runner name, got %#v", provider.started)
	}
	exists, err := worker.Exists(context.Background(), 42)
	if err != nil || !exists {
		t.Fatalf("expected repository-qualified runner lookup to find the runner, got exists=%t err=%v", exists, err)
	}
}

func TestCoordinatedRepositoryWorkerUsesCompactedRunnerNameForStartAndRecovery(t *testing.T) {
	baseName := "github-com-meza-mezas-conventional-changelog-meza-ubuntu-latest"
	expectedRunnerName := "github-com-meza-mezas-conventional-changelog-ee540047238cb46a-42"
	provider := &fakeScalerProvider{instances: []scaler.RunnerInstance{{
		RunnerName: expectedRunnerName,
		State:      scaler.RunnerStateRunning,
	}}}
	jitGenerator := &recordingRunnerNameJITGenerator{}
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: baseName, MaxRunners: 2, Provider: provider, JITConfig: jitGenerator,
	}}}

	if err := worker.StartOne(context.Background(), 42); err != nil {
		t.Fatalf("expected runner start success, got %v", err)
	}
	if len(provider.started) != 1 || provider.started[0].RunnerName != expectedRunnerName {
		t.Fatalf("expected compacted runner name, got %#v", provider.started)
	}
	if len(jitGenerator.runnerNames) != 1 || jitGenerator.runnerNames[0] != expectedRunnerName {
		t.Fatalf("expected compacted JIT runner name, got %#v", jitGenerator.runnerNames)
	}
	exists, err := worker.Exists(context.Background(), 42)
	if err != nil || !exists {
		t.Fatalf("expected compacted runner lookup to find the runner, got exists=%t err=%v", exists, err)
	}
}

func TestScaleSetStopPollingWaitsForPollerBeforeShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messageClient := &blockingMessageClient{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
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

	select {
	case <-messageClient.entered:
	case <-time.After(time.Second):
		t.Fatal("expected poller to enter message receive")
	}
	scaleSet.StopPolling()
	select {
	case <-messageClient.exited:
	default:
		t.Fatal("expected StopPolling to wait for poller exit")
	}
	if err := scaleSet.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected shutdown success, got %v", err)
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
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitConfig: "encoded",
		jitRunner: &upstream.RunnerReference{ID: 99, Name: "linux-1"},
	}, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success, got %v", err)
	}
	if jitConfig.JITConfig != "encoded" {
		t.Fatalf("expected encoded jit, got %q", jitConfig.JITConfig)
	}
	if jitConfig.Runner.ID != 99 || jitConfig.Runner.Name != "linux-1" {
		t.Fatalf("expected runner reference, got %#v", jitConfig.Runner)
	}
}

func TestJITGeneratorSurfacesClientFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{jitErr: errors.New("jit failed")}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected jit error")
	}
}

func TestJITGeneratorRemovesStaleRunnerAfterNameConflict(t *testing.T) {
	client := &fakeScaleSetClient{
		jitErrors:    []error{upstream.RunnerExistsError},
		jitConfig:    "encoded",
		jitRunner:    &upstream.RunnerReference{ID: 100, Name: "linux-1"},
		runnerByName: &upstream.RunnerReference{ID: 99, Name: "linux-1"},
	}
	generator := scalesetJITGenerator{Client: client, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success after stale runner cleanup, got %v", err)
	}
	if jitConfig.Runner.ID != 100 || jitConfig.JITConfig != "encoded" {
		t.Fatalf("expected retried jit config, got %#v", jitConfig)
	}
	if len(client.runnerNameLookups) != 1 || client.runnerNameLookups[0] != "linux-1" {
		t.Fatalf("expected runner name lookup, got %#v", client.runnerNameLookups)
	}
	if len(client.removedRunnerIDs) != 1 || client.removedRunnerIDs[0] != 99 {
		t.Fatalf("expected stale runner removal, got %#v", client.removedRunnerIDs)
	}
	if len(client.generatedRunnerNames) != 2 {
		t.Fatalf("expected one retry after stale runner cleanup, got %#v", client.generatedRunnerNames)
	}
}

func TestJITGeneratorSurfacesStaleRunnerLookupFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitErrors:        []error{upstream.RunnerExistsError},
		runnerByNameErr:  errors.New("lookup failed"),
		runnerByName:     &upstream.RunnerReference{ID: 99, Name: "linux-1"},
		removeRunnerErr:  nil,
		removedRunnerIDs: nil,
	}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected lookup error")
	}
}

func TestJITGeneratorSurfacesMissingStaleRunner(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitErrors: []error{upstream.RunnerExistsError},
	}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected missing stale runner error")
	}
}

func TestJITGeneratorSurfacesStaleRunnerRemovalFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitErrors:       []error{upstream.RunnerExistsError},
		runnerByName:    &upstream.RunnerReference{ID: 99, Name: "linux-1"},
		removeRunnerErr: errors.New("remove failed"),
	}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected remove error")
	}
}

func TestJITGeneratorSurfacesRetryFailureAfterStaleRunnerCleanup(t *testing.T) {
	client := &fakeScaleSetClient{
		jitErrors:    []error{upstream.RunnerExistsError, errors.New("retry failed")},
		runnerByName: &upstream.RunnerReference{ID: 99, Name: "linux-1"},
	}
	generator := scalesetJITGenerator{Client: client}

	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected retry error")
	}
	if len(client.removedRunnerIDs) != 1 || client.removedRunnerIDs[0] != 99 {
		t.Fatalf("expected stale runner removal, got %#v", client.removedRunnerIDs)
	}
	if len(client.generatedRunnerNames) != 2 {
		t.Fatalf("expected one retry after stale runner cleanup, got %#v", client.generatedRunnerNames)
	}
}

func TestJITGeneratorHandlesMissingRunnerReference(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{jitConfig: "encoded"}, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success, got %v", err)
	}
	if jitConfig.Runner.ID != 0 || jitConfig.Runner.Name != "" {
		t.Fatalf("expected empty runner reference, got %#v", jitConfig.Runner)
	}
}

func TestJITGeneratorRemovesGeneratedRunner(t *testing.T) {
	client := &fakeScaleSetClient{}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{ID: 99, Name: "linux-1"}); err != nil {
		t.Fatalf("expected remove success, got %v", err)
	}
	if len(client.removedRunnerIDs) != 1 || client.removedRunnerIDs[0] != 99 {
		t.Fatalf("expected runner removal, got %#v", client.removedRunnerIDs)
	}
}

func TestJITGeneratorIgnoresMissingRunnerReference(t *testing.T) {
	client := &fakeScaleSetClient{}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{}); err != nil {
		t.Fatalf("expected missing runner reference success, got %v", err)
	}
	if len(client.removedRunnerIDs) != 0 {
		t.Fatalf("expected no runner removal, got %#v", client.removedRunnerIDs)
	}
}

func TestJITGeneratorSurfacesRemoveFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{removeRunnerErr: errors.New("remove failed")}}
	if err := generator.Remove(context.Background(), scaler.JITRunner{ID: 99, Name: "linux-1"}); err == nil {
		t.Fatal("expected remove error")
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

func assertScaleSetLabels(t *testing.T, labels []upstream.Label, expectedNames []string) {
	t.Helper()
	if len(labels) != len(expectedNames) {
		t.Fatalf("expected labels %v, got %#v", expectedNames, labels)
	}
	for labelIndex, expectedName := range expectedNames {
		if labels[labelIndex].Name != expectedName || labels[labelIndex].Type != "System" {
			t.Fatalf("expected label %q at index %d, got %#v", expectedName, labelIndex, labels[labelIndex])
		}
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
	jitRunner              *upstream.RunnerReference
	jitErr                 error
	jitErrors              []error
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
	if len(client.jitErrors) > 0 {
		jitErr := client.jitErrors[0]
		client.jitErrors = client.jitErrors[1:]
		if jitErr != nil {
			return nil, jitErr
		}
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
	client.enterOnce.Do(func() {
		close(client.entered)
	})
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)
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
