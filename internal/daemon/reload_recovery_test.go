package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestRejectedProviderChangeSchedulesDemandObservedDuringSuspension(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fixture := newRejectedReloadRecoveryFixture(t, cancel)
	if err := runActiveConfiguration(
		ctx, fixture.options, fixture.daemon, fixture.activeConfig,
	); err != nil {
		t.Fatalf("run active configuration: %v", err)
	}
	if len(fixture.worker.started) != 1 || fixture.worker.started[0] != 42 {
		t.Fatalf("retained demand was not scheduled after rejection: %#v", fixture.worker.started)
	}
}

type rejectedReloadRecoveryFixture struct {
	options      Options
	daemon       standaloneDaemon
	activeConfig activeConfiguration
	worker       *recordingRecoveryWorker
}

func newRejectedReloadRecoveryFixture(t *testing.T, cancel context.CancelFunc) rejectedReloadRecoveryFixture {
	t.Helper()
	discoverer := &recordingDiscoverer{result: githubapp.DiscoveryResult{
		InstallationFailures: map[githubapp.InstallationID]error{1: errors.New("discovery unavailable")},
	}, afterDiscover: func(call int) {
		if call == 2 {
			cancel()
		}
	}}
	activeConfig, worker := rejectedReloadActiveConfiguration(t, discoverer)
	daemonUnderTest := rejectedReloadDaemon(t, discoverer)
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	return rejectedReloadRecoveryFixture{
		options: Options{ConfigPath: "config", ReloadRequests: reloads},
		daemon:  daemonUnderTest, activeConfig: activeConfig, worker: worker,
	}
}

func rejectedReloadActiveConfiguration(
	t *testing.T,
	discoverer repositoryDiscoverer,
) (activeConfiguration, *recordingRecoveryWorker) {
	t.Helper()
	coordinator := scaler.NewCoordinator(1)
	worker := &recordingRecoveryWorker{}
	coordinator.AddRepository("repo", worker)
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		Running: func(ctx context.Context) (int, error) {
			return 1, coordinator.Apply(ctx, []scaler.JobDemand{{
				Repository: "repo", RequestID: 42,
			}}, nil)
		},
	})
	activeConfig.Config = runnableConfig(t)
	activeConfig.Coordinators["linux"] = coordinator
	activeConfig.Discoverer = discoverer
	repository := activeConfig.Repositories["repo"]
	repository.InstallationID = 1
	activeConfig.Repositories["repo"] = repository
	return activeConfig, worker
}

func rejectedReloadDaemon(t *testing.T, discoverer repositoryDiscoverer) standaloneDaemon {
	t.Helper()
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Docker.Service = "replacement"
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	daemonUnderTest.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return discoverer, nil
	}
	daemonUnderTest.reconciliationRetryDelay = time.Nanosecond
	return daemonUnderTest
}

func TestProviderChangeValidationRejectsBlockedProvisioningFence(t *testing.T) {
	worker := &blockingCoordinatorWorker{entered: make(chan struct{}), release: make(chan struct{})}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	scheduleDone := make(chan error, 1)
	go func() { scheduleDone <- coordinator.Schedule(context.Background()) }()
	<-worker.entered
	activeConfig := testActiveConfiguration()
	activeConfig.Config = runnableConfig(t)
	activeConfig.Coordinators["linux"] = coordinator
	reloadedConfig := runnableConfig(t)
	reloadedConfig.ScaleSets[0].Docker.Service = "replacement"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := validateReloadProviderChanges(ctx, activeConfig, reloadedConfig); !errors.Is(err, context.Canceled) {
		t.Fatalf("provider validation error = %v, want context cancellation", err)
	}
	close(worker.release)
	if err := <-scheduleDone; err != nil {
		t.Fatalf("in-flight schedule: %v", err)
	}
}

type blockingCoordinatorWorker struct {
	entered chan struct{}
	release chan struct{}
}

func (worker *blockingCoordinatorWorker) Running(context.Context) (int, error) {
	close(worker.entered)
	<-worker.release
	return 0, nil
}

func (*blockingCoordinatorWorker) Exists(context.Context, int64) (bool, error) { return false, nil }
func (*blockingCoordinatorWorker) Acquire(context.Context, int64) error        { return nil }
func (*blockingCoordinatorWorker) StartOne(context.Context, int64) error       { return nil }
func (*blockingCoordinatorWorker) StartAssigned(context.Context) error         { return nil }
