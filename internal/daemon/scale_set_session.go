package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func prepareScaleSetSession(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	runnerHostname string,
) (*upstream.RunnerScaleSet, scaleSetMessageClient, error) {
	runnerGroup, err := lookupRunnerGroup(ctx, logger, client, repositoryURL, scaleSetConfig)
	if err != nil {
		return nil, scaleSetMessageClient{}, err
	}
	runnerScaleSet, err := ensureConfiguredRunnerScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig, runnerGroup.ID)
	if err != nil {
		return nil, scaleSetMessageClient{}, err
	}
	messageClient, err := createMessageSession(ctx, logger, client, repositoryURL, scaleSetConfig.Name, runnerHostname, runnerScaleSet.ID)
	if err != nil {
		return nil, scaleSetMessageClient{}, err
	}
	return runnerScaleSet, messageClient, nil
}

func lookupRunnerGroup(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
) (*upstream.RunnerGroup, error) {
	logAction(ctx, logger, slog.LevelDebug, "runner_group_lookup", "started",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.String("runnerGroup", scaleSetConfig.RunnerGroup),
	)
	runnerGroup, err := client.GetRunnerGroupByName(ctx, scaleSetConfig.RunnerGroup)
	if err != nil {
		logActionFailure(ctx, logger, "runner_group_lookup", err,
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetConfig.Name),
			slog.String("runnerGroup", scaleSetConfig.RunnerGroup),
		)
		return nil, fmt.Errorf("get runner group %q: %w", scaleSetConfig.RunnerGroup, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "runner_group_lookup", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.String("runnerGroup", scaleSetConfig.RunnerGroup),
		slog.Int("runnerGroupID", runnerGroup.ID),
	)
	return runnerGroup, nil
}

func ensureConfiguredRunnerScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	runnerGroupID int,
) (*upstream.RunnerScaleSet, error) {
	logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_ensure", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
	runnerScaleSet, err := ensureRunnerScaleSet(ctx, client, runnerGroupID, scaleSetConfig)
	if err != nil {
		logActionFailure(ctx, logger, "runner_scale_set_ensure", err,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
		return nil, fmt.Errorf("ensure runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_ensure", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
		slog.Int("runnerScaleSetID", runnerScaleSet.ID),
	)
	return runnerScaleSet, nil
}

func createMessageSession(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetName string,
	runnerHostname string,
	runnerScaleSetID int,
) (scaleSetMessageClient, error) {
	logAction(ctx, logger, slog.LevelDebug, "message_session_create", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	messageClient, err := client.MessageSessionClient(ctx, runnerScaleSetID, runnerHostname)
	if err != nil {
		logActionFailure(ctx, logger, "message_session_create", err,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
		return scaleSetMessageClient{}, fmt.Errorf("create message session for %q: %w", scaleSetName, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "message_session_create", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
		slog.Int("runnerScaleSetID", runnerScaleSetID),
	)
	return messageClient, nil
}

func closeMessageSessionAfterReconcilerFailure(
	ctx context.Context,
	scaleSetName string,
	messageClient scaleSetMessageClient,
	reconcilerErr error,
) error {
	if closeErr := messageClient.Close(context.WithoutCancel(ctx)); closeErr != nil {
		return errors.Join(
			fmt.Errorf("build reconciler for %q: %w", scaleSetName, reconcilerErr),
			fmt.Errorf("close message session: %w", closeErr),
		)
	}
	return fmt.Errorf("build reconciler for %q: %w", scaleSetName, reconcilerErr)
}
