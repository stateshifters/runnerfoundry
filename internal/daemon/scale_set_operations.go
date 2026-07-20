package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
)

func startCoordinatedScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	coordinator *scaler.Coordinator,
) (scaleSetLifecycle, error) {
	return runCoordinatedScaleSet(
		ctx, logger, client, repositoryURL, scaleSetConfig, coordinator, productionScaleSetSessionOperations(),
	)
}

type pruneTarget struct {
	RepositoryURL string
	RunnerGroup   string
}

func pruneScaleSetTarget(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetPruneClient,
	target pruneTarget,
	desiredNames map[string]bool,
) error {
	runnerGroup, err := client.GetRunnerGroupByName(ctx, target.RunnerGroup)
	if err != nil {
		return fmt.Errorf("get runner group %q for prune: %w", target.RunnerGroup, err)
	}
	runnerScaleSets, err := client.ListRunnerScaleSets(ctx, runnerGroup.ID)
	if err != nil {
		return fmt.Errorf("list runner scale sets for prune: %w", err)
	}
	for _, runnerScaleSet := range runnerScaleSets {
		if desiredNames[runnerScaleSet.Name] {
			continue
		}
		if err := deleteRunnerScaleSet(
			ctx, logger, target.RepositoryURL, runnerScaleSet.Name, client, &runnerScaleSet,
		); err != nil {
			return fmt.Errorf("delete runner scale set %q: %w", runnerScaleSet.Name, err)
		}
	}
	return nil
}
