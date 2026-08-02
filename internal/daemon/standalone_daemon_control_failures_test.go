package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestRunDaemonReportsRepositoryControlStartupFailure(t *testing.T) {
	expectedErr := errors.New("controller failed")
	loadedConfig := runnableConfig(t)
	err := runDaemon(context.Background(), Options{}, standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{}, nil
		},
		newLogger: func(string, string) *slog.Logger {
			return slog.New(slog.NewTextHandler(io.Discard, nil))
		},
		openState: func(string) (standaloneState, error) {
			return &faultStandaloneState{standaloneState: newMemoryStandaloneState(), startErr: expectedErr}, nil
		},
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("run error = %v, want %v", err, expectedErr)
	}
}

func TestRunActiveConfigurationReportsRepositoryControlStartupFailure(t *testing.T) {
	expectedErr := errors.New("controller failed")
	active := testActiveConfiguration()
	active.RepositoryState = &faultStandaloneState{standaloneState: newMemoryStandaloneState(), startErr: expectedErr}
	if err := runActiveConfiguration(context.Background(), Options{}, standaloneDaemon{}, active); !errors.Is(err, expectedErr) {
		t.Fatalf("run error = %v, want %v", err, expectedErr)
	}
}

func TestRejectedReloadReportsRepositoryControlResumeFailure(t *testing.T) {
	expectedErr := errors.New("controller failed")
	active := testActiveConfiguration()
	active.Config = runnableConfig(t)
	active.Discoverer = staticDiscoverer{}
	active.RepositoryState = &sequencedStartStandaloneState{
		standaloneState: newMemoryStandaloneState(), failAt: 2, err: expectedErr,
	}
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	daemon := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("reload rejected") },
		notifier:   noopNotifier{},
	}
	err := runActiveConfiguration(context.Background(), Options{ReloadRequests: reloads}, daemon, active)
	if !errors.Is(err, expectedErr) || !strings.Contains(err.Error(), "resume standalone repository control") {
		t.Fatalf("run error = %v, want resume failure", err)
	}
}

func TestAcceptedReloadReportsReplacementRepositoryControlFailure(t *testing.T) {
	expectedErr := errors.New("controller failed")
	loadedConfig := runnableConfig(t)
	active := testActiveConfiguration()
	active.Config = loadedConfig
	active.Discoverer = staticDiscoverer{}
	active.RepositoryState = &sequencedStartStandaloneState{
		standaloneState: newMemoryStandaloneState(), failAt: 2, err: expectedErr,
	}
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	err := runActiveConfiguration(
		context.Background(), Options{ReloadRequests: reloads}, successfulReloadDaemon(loadedConfig), active,
	)
	if !errors.Is(err, expectedErr) || !strings.Contains(err.Error(), "start reloaded standalone repository control") {
		t.Fatalf("run error = %v, want replacement failure", err)
	}
}

func TestRejectedReloadReportsDemandRecoveryFailureBeforeCancelledShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	active := testActiveConfiguration()
	active.Config = runnableConfig(t)
	active.Discoverer = staticDiscoverer{}
	active.RepositoryState = newMemoryStandaloneState()
	active.Coordinators["linux"] = scaler.NewDurableCoordinator("linux", 1, cancelingOrderedFailureStore{cancel: cancel})
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	daemon := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("reload rejected") },
		notifier:   noopNotifier{},
	}
	if err := runActiveConfiguration(ctx, Options{ReloadRequests: reloads}, daemon, active); err != nil {
		t.Fatalf("run error = %v", err)
	}
}

type sequencedStartStandaloneState struct {
	standaloneState
	starts int
	failAt int
	err    error
}

func (state *sequencedStartStandaloneState) StartController(
	ctx context.Context, appID int64,
) (repositorystate.Snapshot, error) {
	state.starts++
	if state.starts == state.failAt {
		return repositorystate.Snapshot{}, state.err
	}
	snapshot, err := state.standaloneState.StartController(ctx, appID)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("delegate sequenced controller start: %w", err)
	}
	return snapshot, nil
}

type cancelingOrderedFailureStore struct {
	cancel context.CancelFunc
}

func (cancelingOrderedFailureStore) Apply(context.Context, string, []scaler.JobDemand, []scaler.DemandRemoval) error {
	return nil
}

func (cancelingOrderedFailureStore) RemoveRepository(context.Context, string, string) error {
	return nil
}
func (cancelingOrderedFailureStore) RemoveScaleSet(context.Context, string) error { return nil }
func (store cancelingOrderedFailureStore) Ordered(context.Context, string) ([]scaler.QueuedDemand, error) {
	store.cancel()
	return nil, errors.New("recovery failed")
}
