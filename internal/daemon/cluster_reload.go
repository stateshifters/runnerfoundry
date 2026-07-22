package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
)

func (daemon *clusteredDaemon) reload(ctx context.Context, configPath string) error {
	loaded, err := daemon.loadConfig(configPath)
	if err != nil {
		return err
	}
	configuration := daemon.configurationSnapshot()
	if loaded.State.Postgres == nil || *loaded.State.Postgres != *configuration.config.State.Postgres {
		return errors.New("clustered reload cannot change PostgreSQL state configuration or switch state backends")
	}
	credentials, err := loaded.GitHub.AppCredentials()
	if err != nil {
		return fmt.Errorf("load reloaded GitHub App credentials: %w", err)
	}
	discoverer, err := daemon.newDiscoverer(credentials)
	if err != nil {
		return fmt.Errorf("create reloaded repository discoverer: %w", err)
	}
	daemon.configurationMutex.Lock()
	daemon.config, daemon.credentials, daemon.discoverer = loaded, credentials, discoverer
	daemon.configurationMutex.Unlock()
	if err = daemon.publish(ctx, cluster.NodeHealthy); err != nil {
		return err
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "reload", "completed")
	return nil
}

type clusteredConfigurationSnapshot struct {
	config      config.Config
	credentials config.GitHubAppCredentials
	discoverer  repositoryDiscoverer
}

func (daemon *clusteredDaemon) configurationSnapshot() clusteredConfigurationSnapshot {
	daemon.configurationMutex.RLock()
	defer daemon.configurationMutex.RUnlock()
	return clusteredConfigurationSnapshot{
		config: daemon.config, credentials: daemon.credentials, discoverer: daemon.discoverer,
	}
}

func (daemon *clusteredDaemon) shutdown(ctx context.Context) {
	daemon.waitForWorkers()
	daemon.stopLeadershipContender(ctx)
	if leader := daemon.currentLeader(); leader != nil {
		if daemon.clearLeadership(leader) {
			leader.stop(ctx)
		}
	}
	if err := daemon.publish(ctx, cluster.NodeUnhealthy); err != nil {
		daemon.logger.Warn("publish node shutdown failed", slog.String("error", err.Error()))
	}
}
