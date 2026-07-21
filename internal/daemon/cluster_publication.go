package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
)

func (daemon *clusteredDaemon) publish(ctx context.Context, nodeHealth cluster.NodeHealth) error {
	configuration := daemon.configurationSnapshot()
	contributions := make([]cluster.Contribution, 0, len(configuration.config.ScaleSets))
	for _, scaleSet := range configuration.config.ScaleSets {
		healthCtx, cancelHealth := context.WithTimeout(ctx, clusterCoordinationTimeout)
		healthErr := daemon.providerHealthy(healthCtx, scaleSet)
		cancelHealth()
		healthy := healthErr == nil
		if !healthy {
			daemon.logger.Error("scale-set provider is unhealthy",
				slog.String("scaleSet", scaleSet.Name), slog.String("hostname", string(daemon.nodeID)),
				slog.String("error", healthErr.Error()))
		}
		contributions = append(contributions, cluster.Contribution{
			ScaleSet: scaleSet.Name, RunnerGroup: scaleSet.RunnerGroup,
			MaxRunners: scaleSet.MaxRunners, Healthy: healthy,
		})
	}
	publishCtx, cancelPublish := context.WithTimeout(ctx, clusterCoordinationTimeout)
	err := daemon.nodes.Publish(publishCtx, daemon.nodeID, nodeHealth, contributions, time.Now())
	cancelPublish()
	if err != nil {
		return fmt.Errorf("publish clustered node contributions: %w", err)
	}
	return nil
}
