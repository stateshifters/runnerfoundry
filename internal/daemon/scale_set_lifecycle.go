package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
)

var operatingSystemHostname = os.Hostname

type scaleSetLifecycle struct {
	RunnerScaleSetID int
	StopPolling      func()
	CloseSession     func(ctx context.Context) error
	Drain            func(ctx context.Context) (int, error)
	DestroyActive    func(ctx context.Context) error
	Shutdown         func(ctx context.Context) error
}

func shutdownScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	messageClient scaleSetMessageClient,
	client scaleSetDeletionClient,
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
	client scaleSetRegistrationClient,
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
	runnerScaleSet, err = client.CreateRunnerScaleSet(ctx, configuredRunnerScaleSet(runnerGroupID, scaleSetConfig))
	if err != nil {
		return nil, fmt.Errorf("create runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	return runnerScaleSet, nil
}

type scaleSetRegistrationAuditClient interface {
	runnerGroupClient
	scaleSetRegistrationClient
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
}

func ensureRepositoryScaleSetRegistrations(
	ctx context.Context,
	logger *slog.Logger,
	repository string,
	client scaleSetRegistrationAuditClient,
	configuredScaleSets []config.ScaleSetConfig,
) (map[string]*upstream.RunnerScaleSet, error) {
	registrations := make(map[string]*upstream.RunnerScaleSet, len(configuredScaleSets))
	var resultErr error
	for runnerGroupName, groupScaleSets := range scaleSetsByRunnerGroup(configuredScaleSets) {
		groupRegistrations, err := reconcileRunnerGroupRegistrations(
			ctx, logger, repository, runnerGroupName, client, groupScaleSets,
		)
		if err != nil {
			resultErr = errors.Join(resultErr, err)
		}
		for name, registration := range groupRegistrations {
			registrations[name] = registration
		}
	}
	return registrations, resultErr
}

func scaleSetsByRunnerGroup(configured []config.ScaleSetConfig) map[string][]config.ScaleSetConfig {
	result := make(map[string][]config.ScaleSetConfig)
	for _, scaleSetConfig := range configured {
		result[scaleSetConfig.RunnerGroup] = append(result[scaleSetConfig.RunnerGroup], scaleSetConfig)
	}
	return result
}

func reconcileRunnerGroupRegistrations(
	ctx context.Context,
	logger *slog.Logger,
	repository string,
	runnerGroupName string,
	client scaleSetRegistrationAuditClient,
	configured []config.ScaleSetConfig,
) (map[string]*upstream.RunnerScaleSet, error) {
	runnerGroup, err := client.GetRunnerGroupByName(ctx, runnerGroupName)
	if err != nil {
		return nil, fmt.Errorf("get runner group %q for registration audit: %w", runnerGroupName, err)
	}
	observed, err := client.ListRunnerScaleSets(ctx, runnerGroup.ID)
	if err != nil {
		return nil, fmt.Errorf("list runner scale sets for group %q: %w", runnerGroupName, err)
	}
	logAction(ctx, logger, slog.LevelDebug, "repository_registration_audit", "completed",
		slog.String("repository", repository), slog.String("runnerGroup", runnerGroupName),
		slog.Int("observedCount", len(observed)), slog.Int("expectedCount", len(configured)))
	return ensureRunnerGroupRegistrations(ctx, runnerGroupRegistrationAudit{
		logger: logger, repository: repository, runnerGroupName: runnerGroupName, runnerGroupID: runnerGroup.ID,
		client: client, configured: configured, observed: observed,
	})
}

type runnerGroupRegistrationAudit struct {
	logger          *slog.Logger
	repository      string
	runnerGroupName string
	runnerGroupID   int
	client          scaleSetRegistrationAuditClient
	configured      []config.ScaleSetConfig
	observed        []upstream.RunnerScaleSet
}

func ensureRunnerGroupRegistrations(
	ctx context.Context,
	audit runnerGroupRegistrationAudit,
) (map[string]*upstream.RunnerScaleSet, error) {
	registrations := make(map[string]*upstream.RunnerScaleSet, len(audit.configured))
	observedByName := make(map[string]*upstream.RunnerScaleSet, len(audit.observed))
	for index := range audit.observed {
		observedByName[audit.observed[index].Name] = &audit.observed[index]
	}
	var resultErr error
	for _, scaleSetConfig := range audit.configured {
		registration := observedByName[scaleSetConfig.Name]
		if registration == nil {
			var err error
			registration, err = createMissingRunnerScaleSetRegistration(
				ctx, audit.logger, audit.repository, audit.runnerGroupName, audit.runnerGroupID, audit.client, scaleSetConfig,
			)
			if err != nil {
				resultErr = errors.Join(resultErr, err)
				continue
			}
		}
		registrations[scaleSetConfig.Name] = registration
	}
	return registrations, resultErr
}

func createMissingRunnerScaleSetRegistration(
	ctx context.Context,
	logger *slog.Logger,
	repository string,
	runnerGroupName string,
	runnerGroupID int,
	client scaleSetRegistrationAuditClient,
	scaleSetConfig config.ScaleSetConfig,
) (*upstream.RunnerScaleSet, error) {
	attributes := make([]slog.Attr, 0, 4)
	attributes = append(attributes, slog.String("repository", repository), slog.String("runnerGroup", runnerGroupName),
		slog.String("scaleSet", scaleSetConfig.Name))
	logAction(ctx, logger, slog.LevelWarn, "runner_scale_set_registration", "missing", attributes...)
	registration, err := client.CreateRunnerScaleSet(ctx, configuredRunnerScaleSet(runnerGroupID, scaleSetConfig))
	if err != nil {
		logActionFailure(ctx, logger, "runner_scale_set_registration", err, attributes...)
		return nil, fmt.Errorf("create missing runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	logAction(ctx, logger, slog.LevelInfo, "runner_scale_set_registration", "created",
		append(attributes, slog.Int("runnerScaleSetID", registration.ID))...)
	return registration, nil
}

func configuredRunnerScaleSet(runnerGroupID int, scaleSetConfig config.ScaleSetConfig) *upstream.RunnerScaleSet {
	return &upstream.RunnerScaleSet{
		Name:          scaleSetConfig.Name,
		RunnerGroupID: runnerGroupID,
		Labels: []upstream.Label{{
			Name: scaleSetConfig.Name,
			Type: "System",
		}},
		RunnerSetting: upstream.RunnerSetting{
			DisableUpdate: true,
		},
	}
}

func runnerHostname() string {
	hostname, err := operatingSystemHostname()
	if err != nil {
		return "runnerfoundry"
	}
	return hostname
}
