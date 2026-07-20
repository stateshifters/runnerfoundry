package daemon

import (
	"context"

	"github.com/meza/github-scaler/internal/githubapp"
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

func Run(ctx context.Context, options Options) error {
	configured, err := loadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	if configured.State.Postgres != nil {
		return runClusteredDaemon(ctx, options, newClusteredDaemon(), configured)
	}
	return runDaemon(ctx, options, newStandaloneDaemon())
}
