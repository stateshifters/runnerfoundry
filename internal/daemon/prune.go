package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/meza/RunnerFoundry/internal/config"
)

func pruneRepository(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetPruneClient,
	repositoryURL string,
	scaleSets []config.ScaleSetConfig,
) error {
	byGroup := map[string]map[string]bool{}
	for _, scaleSetConfig := range scaleSets {
		if byGroup[scaleSetConfig.RunnerGroup] == nil {
			byGroup[scaleSetConfig.RunnerGroup] = map[string]bool{}
		}
		byGroup[scaleSetConfig.RunnerGroup][scaleSetConfig.Name] = true
	}
	for runnerGroup, names := range byGroup {
		if err := pruneScaleSetTarget(ctx, logger, client, pruneTarget{RepositoryURL: repositoryURL, RunnerGroup: runnerGroup}, names); err != nil {
			return err
		}
	}
	return nil
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
