package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/meza/github-scaler/internal/config"
)

func pruneRepository(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSets []config.ScaleSetConfig,
) error {
	pruneClient, ok := client.(scaleSetPruneClient)
	if !ok {
		return fmt.Errorf("GitHub client for %q does not support pruning", repositoryURL)
	}
	byGroup := map[string]map[string]bool{}
	for _, scaleSetConfig := range scaleSets {
		if byGroup[scaleSetConfig.RunnerGroup] == nil {
			byGroup[scaleSetConfig.RunnerGroup] = map[string]bool{}
		}
		byGroup[scaleSetConfig.RunnerGroup][scaleSetConfig.Name] = true
	}
	for runnerGroup, names := range byGroup {
		if err := pruneScaleSetTarget(ctx, logger, pruneClient, pruneTarget{RepositoryURL: repositoryURL, RunnerGroup: runnerGroup}, names); err != nil {
			return err
		}
	}
	return nil
}
