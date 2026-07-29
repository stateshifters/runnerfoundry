package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestLoggingStandaloneReloadActivatesPreparedDestination(t *testing.T) {
	initialDestination := &recordingLogDestination{}
	replacementDestination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, initialDestination, replacementDestination)
	loadedConfig := mockedStandaloneLoggingConfig()
	loadedConfig.Log = config.LogConfig{Format: "json", Level: "debug"}
	active := testActiveConfiguration()
	active.Config.State.Path = loadedConfig.State.Path
	active.Logger = manager.Logger()
	active.Logging = manager

	daemon := successfulReloadDaemon(loadedConfig)
	daemon.loadCredentials = mockedLoggingCredentials
	reloaded, err := reloadConfiguration(context.Background(), "config", daemon, active)
	if err != nil {
		t.Fatalf("reload standalone logging: %v", err)
	}
	reloaded.Logger.Debug("standalone replacement")
	if !strings.Contains(replacementDestination.String(), `"msg":"standalone replacement"`) {
		t.Fatalf("replacement destination = %q", replacementDestination.String())
	}
	if initialDestination.closeCalls != 1 || reloaded.Logging != manager {
		t.Fatalf("initial closes=%d logging manager preserved=%t", initialDestination.closeCalls, reloaded.Logging == manager)
	}
}

func TestLoggingClusterReloadActivatesPreparedDestination(t *testing.T) {
	configured := mockedClusteredLoggingConfig()
	loaded := configured
	loaded.Log = config.LogConfig{Format: "json", Level: "debug"}
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	daemon.config = configured
	daemon.credentials = config.GitHubAppCredentials{AppID: 1, PrivateKey: "mocked"}
	daemon.loadConfig = func(string) (config.Config, error) { return loaded, nil }
	daemon.loadCredentials = mockedLoggingCredentials
	daemon.nodes = state
	initialDestination := &recordingLogDestination{}
	replacementDestination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, initialDestination, replacementDestination)
	daemon.logger = manager.Logger()
	daemon.logging = manager

	if err := daemon.reload(context.Background(), "config.yaml"); err != nil {
		t.Fatalf("reload clustered logging: %v", err)
	}
	daemon.logger.Debug("cluster replacement")
	if !strings.Contains(replacementDestination.String(), `"msg":"cluster replacement"`) {
		t.Fatalf("replacement destination = %q", replacementDestination.String())
	}
	if initialDestination.closeCalls != 1 {
		t.Fatalf("initial destination close calls = %d", initialDestination.closeCalls)
	}
}

func TestLoggingStandaloneShutdownClosesActiveDestination(t *testing.T) {
	destination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, destination)
	ctx, cancel := context.WithCancel(context.Background())
	daemon := standaloneDaemon{
		loadConfig:      func(string) (config.Config, error) { return mockedStandaloneLoggingConfig(), nil },
		loadCredentials: mockedLoggingCredentials,
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{}, nil
		},
		newLogging: func(config.LogConfig) (*loggingManager, error) { return manager, nil },
		notifier:   cancellingNotifier{cancel: cancel},
	}
	if err := runDaemon(ctx, Options{}, daemon); err != nil {
		t.Fatalf("run mocked daemon lifecycle: %v", err)
	}
	if destination.closeCalls != 1 {
		t.Fatalf("destination close calls = %d", destination.closeCalls)
	}
}

func TestLoggingStandaloneReloadClosesUnactivatedCandidate(t *testing.T) {
	initialDestination := &recordingLogDestination{}
	candidateDestination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, initialDestination, candidateDestination)
	loadedConfig := mockedStandaloneLoggingConfig()
	active := testActiveConfiguration()
	active.Config.State.Path = loadedConfig.State.Path
	active.Logger = manager.Logger()
	active.Logging = manager
	daemon := successfulReloadDaemon(loadedConfig)
	daemon.loadCredentials = mockedLoggingCredentials
	daemon.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return nil, errors.New("candidate dependency failed")
	}

	if _, err := reloadConfiguration(context.Background(), "config", daemon, active); err == nil {
		t.Fatal("expected replacement validation failure")
	}
	manager.Logger().Info("initial remains active")
	if !strings.Contains(initialDestination.String(), "initial remains active") || initialDestination.closeCalls != 0 {
		t.Fatalf("initial destination = %q, closes=%d", initialDestination.String(), initialDestination.closeCalls)
	}
	if candidateDestination.closeCalls != 1 {
		t.Fatalf("candidate destination close calls = %d", candidateDestination.closeCalls)
	}
}

func mockedStandaloneLoggingConfig() config.Config {
	return config.Config{
		GitHub: config.GitHubConfig{
			App: config.GitHubAppConfig{AppID: 1, PrivateKeyFile: "mocked"}, DiscoveryInterval: "5m",
		},
		Log:   config.LogConfig{Format: "text", Level: "info"},
		State: config.StateConfig{Path: "mocked-state"},
		ScaleSets: []config.ScaleSetConfig{{
			Name: "linux", RunnerGroup: "Default", MaxRunners: 1,
			Docker: &config.DockerConfig{ComposeFile: "mocked-compose", Service: "runner"},
		}},
	}
}

func mockedClusteredLoggingConfig() config.Config {
	configured := mockedStandaloneLoggingConfig()
	configured.State.Postgres = &postgresconfig.Settings{DSN: "postgres://mocked", Role: "runnerfoundry"}
	return configured
}

func mockedLoggingCredentials(config.GitHubConfig) (config.GitHubAppCredentials, error) {
	return config.GitHubAppCredentials{AppID: 1, PrivateKey: "mocked"}, nil
}

func TestLoggingManagerHotReloadsExclusiveOutput(t *testing.T) {
	initialDestination := &recordingLogDestination{}
	replacementDestination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, initialDestination, replacementDestination)

	logger := manager.Logger()
	logger.Info("before reload")
	prepared, err := manager.Prepare(config.LogConfig{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatalf("prepare replacement logging: %v", err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatalf("activate replacement logging: %v", err)
	}
	logger.Debug("after reload")

	if !strings.Contains(initialDestination.String(), "before reload") || strings.Contains(initialDestination.String(), "after reload") {
		t.Fatalf("initial destination = %q", initialDestination.String())
	}
	if !strings.Contains(replacementDestination.String(), `"msg":"after reload"`) {
		t.Fatalf("replacement destination = %q", replacementDestination.String())
	}
	if initialDestination.closeCalls != 1 {
		t.Fatalf("initial destination close calls = %d", initialDestination.closeCalls)
	}
}

func TestLoggingManagerKeepsCurrentOutputWhenPreparationFails(t *testing.T) {
	initialDestination := &recordingLogDestination{}
	factoryCalls := 0
	manager, err := newLoggingManager(config.LogConfig{Format: "text", Level: "info"}, logDestinationFactoryFunc(
		func(config.LogConfig) (io.WriteCloser, error) {
			factoryCalls++
			if factoryCalls == 1 {
				return initialDestination, nil
			}
			return nil, errors.New("open failed")
		},
	))
	if err != nil {
		t.Fatalf("create logging manager: %v", err)
	}

	if _, err = manager.Prepare(config.LogConfig{Format: "json", Level: "debug"}); err == nil {
		t.Fatal("expected replacement preparation failure")
	}
	manager.Logger().Info("still active")
	if !strings.Contains(initialDestination.String(), "still active") || initialDestination.closeCalls != 0 {
		t.Fatalf("initial destination after failed prepare = %q, closes=%d", initialDestination.String(), initialDestination.closeCalls)
	}
}

func TestLoggingManagerReportsReplacedDestinationCloseFailure(t *testing.T) {
	initialDestination := &recordingLogDestination{closeErr: errors.New("close failed")}
	replacementDestination := &recordingLogDestination{}
	destinations := []io.WriteCloser{initialDestination, replacementDestination}
	manager, err := newLoggingManager(config.LogConfig{Format: "text", Level: "info"}, logDestinationFactoryFunc(
		func(config.LogConfig) (io.WriteCloser, error) {
			destination := destinations[0]
			destinations = destinations[1:]
			return destination, nil
		},
	))
	if err != nil {
		t.Fatalf("create logging manager: %v", err)
	}
	prepared, err := manager.Prepare(config.LogConfig{Format: "text", Level: "info"})
	if err != nil {
		t.Fatalf("prepare replacement logging: %v", err)
	}
	if err = prepared.Activate(); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("activation error = %v", err)
	}
	manager.Logger().Info("replacement remains active")
	if !strings.Contains(replacementDestination.String(), "replacement remains active") {
		t.Fatalf("replacement destination = %q", replacementDestination.String())
	}
}

func TestLoggingManagerSerializesWritesBeforeDestinationClosure(t *testing.T) {
	initialDestination := newBlockingLogDestination()
	replacementDestination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, initialDestination, replacementDestination)
	writeDone := make(chan struct{})
	go func() {
		manager.Logger().Info("in flight")
		close(writeDone)
	}()
	<-initialDestination.writeStarted

	prepared, err := manager.Prepare(config.LogConfig{Format: "text", Level: "info"})
	if err != nil {
		t.Fatalf("prepare replacement logging: %v", err)
	}
	activationDone := make(chan error, 1)
	go func() { activationDone <- prepared.Activate() }()
	select {
	case err = <-activationDone:
		t.Fatalf("activation completed before in-flight write: %v", err)
	default:
	}
	close(initialDestination.allowWrite)
	<-writeDone
	if err = <-activationDone; err != nil {
		t.Fatalf("activate replacement logging: %v", err)
	}
	if initialDestination.closeCalls != 1 || initialDestination.closedBeforeWrite {
		t.Fatalf("initial closes=%d closed before write=%t", initialDestination.closeCalls, initialDestination.closedBeforeWrite)
	}
}

func TestLoggingManagerClosesActiveDestination(t *testing.T) {
	destination := &recordingLogDestination{}
	manager := newTestLoggingManager(t, destination)
	if err := manager.Close(); err != nil {
		t.Fatalf("close logging manager: %v", err)
	}
	if destination.closeCalls != 1 {
		t.Fatalf("destination close calls = %d", destination.closeCalls)
	}
}

func newTestLoggingManager(t *testing.T, destinations ...io.WriteCloser) *loggingManager {
	t.Helper()
	manager, err := newLoggingManager(config.LogConfig{Format: "text", Level: "info"}, logDestinationFactoryFunc(
		func(config.LogConfig) (io.WriteCloser, error) {
			destination := destinations[0]
			destinations = destinations[1:]
			return destination, nil
		},
	))
	if err != nil {
		t.Fatalf("create logging manager: %v", err)
	}
	return manager
}

type recordingLogDestination struct {
	bytes.Buffer
	closeCalls int
	closeErr   error
}

func (destination *recordingLogDestination) Close() error {
	destination.closeCalls++
	return destination.closeErr
}

type blockingLogDestination struct {
	writeStarted      chan struct{}
	allowWrite        chan struct{}
	mutex             sync.Mutex
	writeCompleted    bool
	closedBeforeWrite bool
	closeCalls        int
}

func newBlockingLogDestination() *blockingLogDestination {
	return &blockingLogDestination{writeStarted: make(chan struct{}), allowWrite: make(chan struct{})}
}

func (destination *blockingLogDestination) Write(contents []byte) (int, error) {
	close(destination.writeStarted)
	<-destination.allowWrite
	destination.mutex.Lock()
	destination.writeCompleted = true
	destination.mutex.Unlock()
	return len(contents), nil
}

func (destination *blockingLogDestination) Close() error {
	destination.mutex.Lock()
	defer destination.mutex.Unlock()
	destination.closeCalls++
	destination.closedBeforeWrite = !destination.writeCompleted
	return nil
}
