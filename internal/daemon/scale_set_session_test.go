package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
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
	_, err := runTestScaleSet(context.Background(), slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
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
	_, err := runTestScaleSet(context.Background(), slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	}, scaleSets)
	if err == nil || !strings.Contains(err.Error(), "close message session") {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestStartCoordinatedScaleSetSurfacesRunnerGroupFailure(t *testing.T) {
	_, err := startCoordinatedScaleSet(context.Background(), slog.Default(), &fakeScaleSetClient{
		runnerGroupErr: errors.New("group failed"),
	}, "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
	}, scaler.NewCoordinator(1))
	if err == nil {
		t.Fatal("expected runner group error")
	}
}

func TestRunScaleSetStartsPollerForExistingScaleSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messageClient := newFakeMessageClient(cancel)
	var capturedScaleSetID int
	scaleSets := scaleSetSessionOperations{
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

	scaleSet, err := runTestScaleSet(ctx, slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
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
	scaleSets := scaleSetSessionOperations{
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
		}, coordinator, scaleSetSessionOperations{
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

func TestReconcilerPreparationFailureClosesMessageSession(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	messageClient := &fakeMessageClient{}
	_, err := runCoordinatedScaleSet(
		context.Background(), slog.Default(), &fakeScaleSetClient{
			runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
			messageClient: scaleSetMessageClient{Client: messageClient},
		}, "repo", config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, coordinator, scaleSetSessionOperations{
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
	scaleSets := scaleSetSessionOperations{
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

	_, err := runTestScaleSet(ctx, slog.Default(), client, "https://github.com/meza/repo-a", config.ScaleSetConfig{
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

func TestScaleSetSessionReportsRunnerGroupFailure(t *testing.T) {
	assertScaleSetSessionFailure(t, &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")}, scaleSetSessionOperations{}, "get runner group")
}

func TestScaleSetSessionReportsScaleSetLookupFailure(t *testing.T) {
	assertScaleSetSessionFailure(t, &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSetErr: errors.New("get failed"),
	}, scaleSetSessionOperations{}, "ensure runner scale set")
}

func TestScaleSetSessionReportsScaleSetCreationFailure(t *testing.T) {
	assertScaleSetSessionFailure(t, &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 1}, createScaleSetErr: errors.New("create failed"),
	}, scaleSetSessionOperations{}, "ensure runner scale set")
}

func TestScaleSetSessionReportsMessageSessionFailure(t *testing.T) {
	assertScaleSetSessionFailure(t, &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
		messageSessionErr: errors.New("message failed"),
	}, scaleSetSessionOperations{}, "create message session")
}

func TestScaleSetSessionReportsReconcilerConstructionFailure(t *testing.T) {
	client := &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
		messageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	}
	operations := scaleSetSessionOperations{BuildReconciler: func(
		context.Context, string, config.ScaleSetConfig, scaleSetClient, int,
	) (scaler.Reconciler, error) {
		return scaler.Reconciler{}, errors.New("build failed")
	}}
	assertScaleSetSessionFailure(t, client, operations, "build reconciler")
}

func assertScaleSetSessionFailure(
	t *testing.T,
	client *fakeScaleSetClient,
	operations scaleSetSessionOperations,
	expected string,
) {
	t.Helper()
	operations.RunnerHostname = func() string { return "host" }
	_, err := runTestScaleSet(
		context.Background(), slog.Default(), client, "https://github.com/meza/repo-a",
		config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, operations,
	)
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected %q error, got %v", expected, err)
	}
}
