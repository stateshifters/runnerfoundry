package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
)

func (daemon *clusteredDaemon) reload(ctx context.Context, configPath string) error {
	candidate, err := daemon.prepareClusterReload(configPath)
	if err != nil {
		return err
	}
	daemon.configurationMutex.Lock()
	daemon.config, daemon.credentials, daemon.discoverer = candidate.config, candidate.credentials, candidate.discoverer
	daemon.configurationMutex.Unlock()
	if err = daemon.publish(ctx, cluster.NodeHealthy); err != nil {
		return err
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "reload", "completed")
	return nil
}

type clusterReloadCandidate struct {
	config      config.Config
	credentials config.GitHubAppCredentials
	discoverer  repositoryDiscoverer
}

func (daemon *clusteredDaemon) prepareClusterReload(configPath string) (clusterReloadCandidate, error) {
	loaded, err := daemon.loadConfig(configPath)
	if err != nil {
		return clusterReloadCandidate{}, err
	}
	configuration := daemon.configurationSnapshot()
	if loaded.State.Postgres == nil || *loaded.State.Postgres != *configuration.config.State.Postgres {
		return clusterReloadCandidate{}, errors.New("clustered reload cannot change PostgreSQL state configuration or switch state backends")
	}
	credentials, err := daemon.credentialsFor(loaded.GitHub)
	if err != nil {
		return clusterReloadCandidate{}, fmt.Errorf("load reloaded GitHub App credentials: %w", err)
	}
	discoverer, err := daemon.newDiscoverer(credentials)
	if err != nil {
		return clusterReloadCandidate{}, fmt.Errorf("create reloaded repository discoverer: %w", err)
	}
	if err = daemon.activateReloadedLogging(loaded.Log); err != nil {
		return clusterReloadCandidate{}, err
	}
	return clusterReloadCandidate{config: loaded, credentials: credentials, discoverer: discoverer}, nil
}

func (daemon *clusteredDaemon) activateReloadedLogging(logConfig config.LogConfig) error {
	if daemon.logging == nil {
		return nil
	}
	prepared, err := daemon.logging.Prepare(logConfig)
	if err != nil {
		return fmt.Errorf("prepare reloaded logging: %w", err)
	}
	if err = prepared.Activate(); err != nil {
		return fmt.Errorf("activate reloaded logging: %w", err)
	}
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
