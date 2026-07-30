package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestRunUsesInjectedRuntimeWithoutExternalIO(t *testing.T) {
	original := productionDaemonRuntime
	t.Cleanup(func() { productionDaemonRuntime = original })
	failure := errors.New("failed")
	productionDaemonRuntime = daemonRuntime{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, failure },
	}
	if err := Run(context.Background(), Options{}); !errors.Is(err, failure) {
		t.Fatalf("load error = %v", err)
	}

	clusteredCalled := false
	productionDaemonRuntime = testDaemonRuntime(config.Config{State: config.StateConfig{
		Postgres: &postgresconfig.Settings{DSN: "postgres://database"},
	}}, func() { clusteredCalled = true }, func() {})
	if err := Run(context.Background(), Options{}); err != nil || !clusteredCalled {
		t.Fatalf("clustered call = %t, %v", clusteredCalled, err)
	}

	standaloneCalled := false
	productionDaemonRuntime = testDaemonRuntime(config.Config{}, func() {}, func() { standaloneCalled = true })
	if err := Run(context.Background(), Options{}); err != nil || !standaloneCalled {
		t.Fatalf("standalone call = %t, %v", standaloneCalled, err)
	}
}

func testDaemonRuntime(configured config.Config, clustered, standalone func()) daemonRuntime {
	return daemonRuntime{
		loadConfig: func(string) (config.Config, error) { return configured, nil },
		clustered: func(context.Context, Options, *clusteredDaemon, config.Config) error {
			clustered()
			return nil
		},
		standalone: func(context.Context, Options, standaloneDaemon) error {
			standalone()
			return nil
		},
		newClustered:  func() *clusteredDaemon { return &clusteredDaemon{} },
		newStandalone: func() standaloneDaemon { return standaloneDaemon{} },
	}
}
