package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/scaler"
)

type assignmentExecutionPhase string

const (
	assignmentExecutionPhaseStart  assignmentExecutionPhase = "start"
	assignmentExecutionPhaseRepair assignmentExecutionPhase = "repair"
)

func (daemon *clusteredDaemon) executeAssignments(ctx context.Context) {
	starting, err := daemon.assignments.Starting(ctx, daemon.nodeID)
	if err != nil {
		daemon.logger.Error("load runner assignments awaiting repair failed", slog.String("error", err.Error()))
		return
	}
	for index := range starting {
		if !daemon.executeClaimedAssignment(ctx, &starting[index], assignmentExecutionPhaseRepair) {
			return
		}
	}
	for {
		if !daemon.executeNextAssignment(ctx) {
			return
		}
	}
}

func (daemon *clusteredDaemon) executeNextAssignment(ctx context.Context) bool {
	assignment, err := daemon.assignments.ClaimForStart(ctx, daemon.nodeID)
	if errors.Is(err, cluster.ErrNoReadyAssignment) {
		return false
	}
	if err != nil {
		daemon.logger.Error("claim runner assignment failed", slog.String("error", err.Error()))
		return false
	}
	return daemon.executeClaimedAssignment(ctx, assignment, assignmentExecutionPhaseStart)
}

func (daemon *clusteredDaemon) executeClaimedAssignment(
	ctx context.Context,
	assignment *cluster.Assignment,
	phase assignmentExecutionPhase,
) bool {
	err := daemon.executeAssignment(ctx, assignment)
	if err == nil {
		logAction(ctx, daemon.logger, slog.LevelInfo, "runner_assignment", "completed",
			slog.String("phase", string(phase)), slog.Int64("assignmentID", int64(assignment.ID)),
			slog.String("scaleSet", assignment.ScaleSet), slog.String("repository", assignment.Repository),
			slog.Int64("requestID", assignment.RequestID))
		return true
	}
	daemon.logger.Error("runner assignment failed",
		slog.String("phase", string(phase)), slog.Int64("assignmentID", int64(assignment.ID)),
		slog.String("scaleSet", assignment.ScaleSet), slog.String("repository", assignment.Repository),
		slog.Int64("requestID", assignment.RequestID), slog.String("error", err.Error()))
	return false
}

func (daemon *clusteredDaemon) executeAssignment(ctx context.Context, assignment *cluster.Assignment) error {
	configuration := daemon.configurationSnapshot()
	scaleSet, exists := localScaleSet(configuration.config.ScaleSets, assignment.ScaleSet)
	if !exists {
		return daemon.completeRemovedScaleSetAssignment(ctx, assignment)
	}
	client, err := daemon.newGitHubClient(assignment.Repository, assignment.InstallationID, configuration.credentials)
	if err != nil {
		return fmt.Errorf("create assignment GitHub client: %w", err)
	}
	reconciler, err := daemon.buildReconciler(ctx, assignment.Repository, scaleSet, client, assignment.GitHubScaleSetID)
	if err != nil {
		return err
	}
	if err = ensureAssignedRunner(ctx, reconciler, assignment.RequestID); err != nil {
		return err
	}
	if err = daemon.assignments.ConfirmRunning(ctx, assignment.ID, assignment.NodeID); err != nil {
		return fmt.Errorf("record started runner assignment: %w", err)
	}
	return nil
}

func (daemon *clusteredDaemon) completeRemovedScaleSetAssignment(
	ctx context.Context,
	assignment *cluster.Assignment,
) error {
	if err := daemon.assignments.CompleteAssignment(
		ctx, assignment.ID, assignment.NodeID, cluster.AssignmentStarting,
	); err != nil {
		return fmt.Errorf("release assignment for removed local scale set: %w", err)
	}
	return nil
}

func ensureAssignedRunner(ctx context.Context, reconciler scaler.Reconciler, requestID int64) error {
	exists, err := assignedRunnerExists(ctx, reconciler, requestID)
	if err != nil {
		return fmt.Errorf("verify assigned runner: %w", err)
	}
	if exists {
		return nil
	}
	startCtx, cancelStart := context.WithTimeout(ctx, clusterProviderStartTimeout)
	startErr := reconciler.StartNamed(startCtx, requestID)
	cancelStart()
	return verifyAssignedRunnerAfterStart(ctx, reconciler, requestID, startErr)
}

func assignedRunnerExists(ctx context.Context, reconciler scaler.Reconciler, requestID int64) (bool, error) {
	inspectCtx, cancelInspect := context.WithTimeout(ctx, clusterProviderInspectTimeout)
	exists, err := reconciler.Exists(inspectCtx, requestID)
	cancelInspect()
	if err != nil {
		return false, fmt.Errorf("inspect assigned runner: %w", err)
	}
	return exists, nil
}

func verifyAssignedRunnerAfterStart(
	ctx context.Context,
	reconciler scaler.Reconciler,
	requestID int64,
	startErr error,
) error {
	exists, inspectErr := assignedRunnerExists(ctx, reconciler, requestID)
	if inspectErr != nil {
		return errors.Join(startErr, fmt.Errorf("verify assigned runner after start: %w", inspectErr))
	}
	if exists {
		return nil
	}
	if startErr != nil {
		return fmt.Errorf("start assigned runner: %w", startErr)
	}
	return errors.New("provider did not start the assigned runner")
}

func localScaleSet(scaleSets []config.ScaleSetConfig, name string) (config.ScaleSetConfig, bool) {
	for _, scaleSet := range scaleSets {
		if scaleSet.Name == name {
			return scaleSet, true
		}
	}
	return config.ScaleSetConfig{}, false
}
