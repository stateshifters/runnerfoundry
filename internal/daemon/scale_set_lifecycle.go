package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func shutdownScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	messageClient scaleSetMessageClient,
	client scaleSetAdministrationClient,
	runnerScaleSet *upstream.RunnerScaleSet,
) error {
	closeErr := closeScaleSetMessageSession(ctx, logger, repositoryURL, scaleSetName, messageClient)
	deleteErr := deleteRunnerScaleSet(ctx, logger, repositoryURL, scaleSetName, client, runnerScaleSet)
	return errors.Join(closeErr, deleteErr)
}

func closeScaleSetMessageSession(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	messageClient scaleSetMessageClient,
) error {
	logAction(ctx, logger, slog.LevelDebug, "message_session_close", "started",
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	closeErr := messageClient.Close(ctx)
	if closeErr != nil {
		logActionFailure(ctx, logger, "message_session_close", closeErr,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	} else {
		logAction(ctx, logger, slog.LevelDebug, "message_session_close", "completed",
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	}
	return closeErr
}

func deleteRunnerScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	client scaleSetDeletionClient,
	runnerScaleSet *upstream.RunnerScaleSet,
) error {
	logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_delete", "started",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
		slog.Int("runnerScaleSetID", runnerScaleSet.ID),
	)
	deleteErr := client.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID)
	if deleteErr != nil {
		logActionFailure(ctx, logger, "runner_scale_set_delete", deleteErr,
			slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
	} else {
		logAction(ctx, logger, slog.LevelDebug, "runner_scale_set_delete", "completed",
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetName),
			slog.Int("runnerScaleSetID", runnerScaleSet.ID),
		)
	}
	if deleteErr != nil {
		return fmt.Errorf("delete runner scale set: %w", deleteErr)
	}
	return nil
}

func ensureRunnerScaleSet(
	ctx context.Context,
	client scaleSetAdministrationClient,
	runnerGroupID int,
	scaleSetConfig config.ScaleSetConfig,
) (*upstream.RunnerScaleSet, error) {
	runnerScaleSet, err := client.GetRunnerScaleSet(ctx, runnerGroupID, scaleSetConfig.Name)
	if err != nil {
		return nil, fmt.Errorf("get runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	if runnerScaleSet != nil {
		return runnerScaleSet, nil
	}
	runnerScaleSet, err = client.CreateRunnerScaleSet(ctx, &upstream.RunnerScaleSet{
		Name:          scaleSetConfig.Name,
		RunnerGroupID: runnerGroupID,
		Labels: []upstream.Label{{
			Name: scaleSetConfig.Name,
			Type: "System",
		}},
		RunnerSetting: upstream.RunnerSetting{
			DisableUpdate: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	return runnerScaleSet, nil
}

func runnerHostname() string {
	hostname, err := operatingSystemHostname()
	if err != nil {
		return "github-scaler"
	}
	return hostname
}
