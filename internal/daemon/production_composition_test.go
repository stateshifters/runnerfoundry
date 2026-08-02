package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/libtnb/logrotate"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusteredProductionCompositionBuildsWithoutExternalIO(t *testing.T) {
	credentials := testAppCredentials(t)
	daemon := newClusteredDaemon()
	loaded, err := daemon.loadCredentials(config.GitHubConfig{App: config.GitHubAppConfig{
		AppID: credentials.AppID, PrivateKey: credentials.PrivateKey,
	}})
	if err != nil || loaded.AppID != credentials.AppID {
		t.Fatalf("credentials = %#v, %v", loaded, err)
	}
	logging, err := daemon.newLogging(config.LogConfig{Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	if err = logging.Close(); err != nil {
		t.Fatal(err)
	}
	connection, err := openClusterStore(postgresconfig.Settings{})
	if err != nil || connection.state == nil {
		t.Fatalf("connection = %#v, %v", connection, err)
	}
	lease := &fakeClusterLease{alive: true}
	result, err := clusterLeadershipResult("leadership", lease, nil)
	if err != nil || result.lease != lease {
		t.Fatalf("leadership = %#v, %v", result, err)
	}
}

func TestStandaloneProductionCompositionBuildsWithoutExternalIO(t *testing.T) {
	credentials := testAppCredentials(t)
	daemon := newStandaloneDaemon()
	loaded, err := daemon.loadCredentials(config.GitHubConfig{App: config.GitHubAppConfig{
		AppID: credentials.AppID, PrivateKey: credentials.PrivateKey,
	}})
	if err != nil || loaded.AppID != credentials.AppID {
		t.Fatalf("credentials = %#v, %v", loaded, err)
	}
	if _, err = daemon.newDiscoverer(credentials); err != nil {
		t.Fatal(err)
	}
	logging, err := daemon.newLogging(config.LogConfig{Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	if err = logging.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedLoggingCloseAndNoOpDestinationSucceed(t *testing.T) {
	logging, err := newLoggingManager(config.LogConfig{Format: "text"}, productionLogDestinationFactory{
		standardOutput: noOpWriteCloser{},
	})
	if err != nil {
		t.Fatal(err)
	}
	preparationErr := errors.New("prepare")
	if err = closePreparedDaemonLogging(preparationErr, logging); !errors.Is(err, preparationErr) {
		t.Fatalf("close preparation = %v", err)
	}
	if err = (noOpWriteCloser{}).Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = newGitHubClient("https://github.com/meza/RunnerFoundry", 1, testAppCredentials(t)); err != nil {
		t.Fatal(err)
	}
}

func TestStandalonePreparationFailuresCloseInMemoryLogging(t *testing.T) {
	failure := errors.New("failed")
	if err := runDaemon(context.Background(), Options{}, standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, failure },
	}); !errors.Is(err, failure) {
		t.Fatalf("run error = %v", err)
	}
	logging, err := newLoggingManager(config.LogConfig{Format: "text"}, productionLogDestinationFactory{
		standardOutput: noOpWriteCloser{},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return runnableConfig(t), nil },
		loadCredentials: func(config.GitHubConfig) (config.GitHubAppCredentials, error) {
			return config.GitHubAppCredentials{}, failure
		},
		newLogging: func(config.LogConfig) (*loggingManager, error) { return logging, nil },
	}
	if _, err = loadConfiguration("config.yaml", daemon); !errors.Is(err, failure) {
		t.Fatalf("configuration error = %v", err)
	}
}

func TestOperatingSystemAdaptersDelegateToInjectedBoundaries(t *testing.T) {
	originalRotatingLog := newRotatingLogFile
	originalSQLiteStore := openSQLiteDemandStore
	t.Cleanup(func() {
		newRotatingLogFile = originalRotatingLog
		openSQLiteDemandStore = originalSQLiteStore
	})
	newRotatingLogFile = func(string, ...logrotate.Option) (*logrotate.Writer, error) {
		return &logrotate.Writer{}, nil
	}
	factory := newProductionLogDestinationFactory()
	if destination, err := factory.openRotatingFile(rotatingFileSettings{}, func(error) {}); err != nil || destination == nil {
		t.Fatalf("destination = %#v, %v", destination, err)
	}
	openSQLiteDemandStore = func(string) (*scaler.SQLiteDemandStore, error) {
		return &scaler.SQLiteDemandStore{}, nil
	}
	daemon := newStandaloneDaemon()
	store, err := daemon.openState("state.db")
	if err != nil || store == nil {
		t.Fatalf("store = %#v, %v", store, err)
	}
	failure := errors.New("failed")
	openSQLiteDemandStore = func(string) (*scaler.SQLiteDemandStore, error) { return nil, failure }
	if _, err = daemon.openState("state.db"); err == nil {
		t.Fatal("expected SQLite open failure")
	}
}
