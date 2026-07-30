package daemon

import (
	"context"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

type Options struct {
	ConfigPath     string
	ReloadRequests <-chan struct{}
}

type repositoryDiscoverer interface {
	Discover(ctx context.Context) (githubapp.DiscoveryResult, error)
}

type serviceNotifier interface {
	Ready(ctx context.Context) error
	Reloading(ctx context.Context) error
}

type daemonRuntime struct {
	loadConfig    func(string) (config.Config, error)
	clustered     func(context.Context, Options, *clusteredDaemon, config.Config) error
	standalone    func(context.Context, Options, standaloneDaemon) error
	newClustered  func() *clusteredDaemon
	newStandalone func() standaloneDaemon
}

var productionDaemonRuntime = daemonRuntime{
	loadConfig: loadConfig, clustered: runClusteredDaemon, standalone: runDaemon,
	newClustered: newClusteredDaemon, newStandalone: newStandaloneDaemon,
}

func Run(ctx context.Context, options Options) error {
	runtime := productionDaemonRuntime
	configured, err := runtime.loadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	if configured.State.Postgres != nil {
		return runtime.clustered(ctx, options, runtime.newClustered(), configured)
	}
	return runtime.standalone(ctx, options, runtime.newStandalone())
}
