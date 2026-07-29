package daemon

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestClusteredStartupReportsLogDestinationFailure(t *testing.T) {
	assertClusteredStartupFailure(t, func(_ *config.Config, daemon *clusteredDaemon, _ *fakeClusterState) {
		daemon.newLogging = func(config.LogConfig) (*loggingManager, error) {
			return nil, errors.New("log destination unavailable")
		}
	})
}

func TestClusteredShutdownReportsActiveLogDestinationCloseFailure(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	destination := &recordingLogDestination{closeErr: errors.New("log close failed")}
	manager := newTestLoggingManager(t, destination)
	daemon.newLogging = func(config.LogConfig) (*loggingManager, error) { return manager, nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runClusteredDaemon(ctx, Options{}, daemon, configured)
	if err == nil || !strings.Contains(err.Error(), "log close failed") || destination.closeCalls != 1 {
		t.Fatalf("shutdown error=%v destination close calls=%d", err, destination.closeCalls)
	}
}

func TestClusterReloadKeepsCurrentLoggingWhenReplacementPreparationFails(t *testing.T) {
	configured := clusteredConfig(t)
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: configured, loaded: configured})
	initial := &recordingLogDestination{}
	factoryCalls := 0
	manager, err := newLoggingManager(config.LogConfig{}, logDestinationFactoryFunc(func(config.LogConfig) (io.WriteCloser, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return initial, nil
		}
		return nil, errors.New("replacement unavailable")
	}))
	if err != nil {
		t.Fatal(err)
	}
	daemon.logging = manager
	if err = daemon.reload(context.Background(), "config.yaml"); err == nil || !strings.Contains(err.Error(), "prepare reloaded logging") {
		t.Fatalf("reload error = %v", err)
	}
	manager.Logger().Info("current remains active")
	if !strings.Contains(initial.String(), "current remains active") || initial.closeCalls != 0 {
		t.Fatalf("current output=%q close calls=%d", initial.String(), initial.closeCalls)
	}
}

func TestClusterReloadReportsReplacementActivationFailure(t *testing.T) {
	configured := clusteredConfig(t)
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: configured, loaded: configured})
	manager := newTestLoggingManager(t,
		&recordingLogDestination{closeErr: errors.New("current close failed")},
		&recordingLogDestination{},
	)
	daemon.logging = manager
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil || !strings.Contains(err.Error(), "activate reloaded logging") {
		t.Fatalf("reload error = %v", err)
	}
}
