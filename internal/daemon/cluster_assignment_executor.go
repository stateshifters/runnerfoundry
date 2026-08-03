package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type assignmentExecutionPhase string

const (
	assignmentExecutionPhaseStart  assignmentExecutionPhase = "start"
	assignmentExecutionPhaseRepair assignmentExecutionPhase = "repair"
)

func (daemon *clusteredDaemon) executeAssignments(ctx context.Context) {
	if !daemon.executePendingRunnerReaps(ctx) {
		return
	}
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

type clusteredRunnerLifecycleState interface {
	scaler.RunnerLifecycleStore
	PendingNodeRunnerReaps(ctx context.Context, nodeID cluster.NodeID) ([]scaler.RunnerLifecycle, error)
}

func (daemon *clusteredDaemon) executePendingRunnerReaps(ctx context.Context) bool {
	daemon.ensureRunnerReapRetries()
	lifecycleStore, supported := daemon.assignments.(clusteredRunnerLifecycleState)
	if !supported {
		return daemon.executePendingLibvirtReaps(ctx)
	}
	configuration := daemon.configurationSnapshot()
	now := time.Now().UTC()
	for _, scaleSet := range configuration.config.ScaleSets {
		timeout, err := scaleSet.JobStartDuration()
		if err != nil {
			daemon.logger.Error("parse clustered runner waiting lease failed",
				slog.String("scaleSet", scaleSet.Name), slog.String("error", err.Error()))
			return false
		}
		if _, err = lifecycleStore.RequestExpiredRunnerReaps(ctx, scaleSet.Name, "", now.Add(-timeout), now); err != nil {
			daemon.logger.Error("expire clustered waiting runners failed",
				slog.String("scaleSet", scaleSet.Name), slog.String("error", err.Error()))
			return false
		}
	}
	pending, err := lifecycleStore.PendingNodeRunnerReaps(ctx, daemon.nodeID)
	if err != nil {
		daemon.logger.Error("load pending runner reaps failed", slog.String("error", err.Error()))
		return false
	}
	daemon.runnerReapRetries.prune(pending)
	for _, runner := range pending {
		daemon.executePendingRunnerReap(ctx, configuration, lifecycleStore, runner)
	}
	return true
}

func (daemon *clusteredDaemon) executePendingRunnerReap(
	ctx context.Context,
	configuration clusteredConfigurationSnapshot,
	lifecycleStore clusteredRunnerLifecycleState,
	runner scaler.RunnerLifecycle,
) {
	reconciler, ready := daemon.pendingRunnerReconciler(ctx, configuration, runner)
	if !ready {
		return
	}
	daemon.reapPendingRunnerLifecycle(ctx, lifecycleStore, reconciler, runner)
}

func (daemon *clusteredDaemon) pendingRunnerReconciler(
	ctx context.Context,
	configuration clusteredConfigurationSnapshot,
	runner scaler.RunnerLifecycle,
) (scaler.Reconciler, bool) {
	scaleSet, exists := localScaleSet(configuration.config.ScaleSets, runner.ScaleSet)
	if !exists {
		daemon.logger.Warn("pending runner reap retained because local configuration is unavailable",
			slog.String("scaleSet", runner.ScaleSet), slog.String("repository", runner.Repository),
			slog.String("runnerName", runner.RunnerName))
		return scaler.Reconciler{}, false
	}
	client, err := daemon.newGitHubClient(
		runner.Repository, githubapp.InstallationID(runner.InstallationID), configuration.credentials,
	)
	if err != nil {
		daemon.logger.Error("create pending runner cleanup GitHub client failed",
			slog.String("runnerName", runner.RunnerName), slog.String("error", err.Error()))
		return scaler.Reconciler{}, false
	}
	reconciler, err := daemon.buildReconciler(ctx, runner.Repository, scaleSet, client, runner.GitHubScaleSetID)
	if err != nil {
		daemon.logger.Error("build pending runner reaper failed",
			slog.String("runnerName", runner.RunnerName), slog.String("error", err.Error()))
		return scaler.Reconciler{}, false
	}
	return reconciler, true
}

func (daemon *clusteredDaemon) reapPendingRunnerLifecycle(
	ctx context.Context,
	lifecycleStore clusteredRunnerLifecycleState,
	reconciler scaler.Reconciler,
	runner scaler.RunnerLifecycle,
) {
	daemon.ensureRunnerReapRetries()
	if !daemon.runnerReapRetries.ready(runner) {
		return
	}
	absent, err := reapRunnerLifecycleWithTimeout(ctx, reconciler, runner)
	if daemon.deferBusyRunnerReap(ctx, runner, err) {
		return
	}
	if err != nil {
		daemon.runnerReapRetries.clear(runner)
		logActionFailure(ctx, daemon.logger, "runner_reap", err, runnerLogAttributes(runner)...)
		return
	}
	if !absent {
		daemon.runnerReapRetries.clear(runner)
		return
	}
	daemon.confirmPendingRunnerReap(ctx, lifecycleStore, runner)
}

func reapRunnerLifecycleWithTimeout(
	ctx context.Context,
	reconciler scaler.Reconciler,
	runner scaler.RunnerLifecycle,
) (bool, error) {
	reapCtx, cancelReap := context.WithTimeout(ctx, libvirtReapTimeout)
	defer cancelReap()
	absent, err := reconciler.ReapRunnerLifecycle(reapCtx, runner)
	if err != nil {
		return false, fmt.Errorf("reap runner lifecycle: %w", err)
	}
	return absent, nil
}

func (daemon *clusteredDaemon) deferBusyRunnerReap(
	ctx context.Context,
	runner scaler.RunnerLifecycle,
	err error,
) bool {
	if !errors.Is(err, scaler.ErrRunnerStillBusy) {
		return false
	}
	retryAfter := daemon.runnerReapRetries.deferRunner(runner)
	logAction(ctx, daemon.logger, slog.LevelDebug, "runner_reap", "deferred",
		append(runnerLogAttributes(runner), slog.Duration("retryAfter", retryAfter))...)
	return true
}

func (daemon *clusteredDaemon) confirmPendingRunnerReap(
	ctx context.Context,
	lifecycleStore clusteredRunnerLifecycleState,
	runner scaler.RunnerLifecycle,
) {
	if err := lifecycleStore.ConfirmRunnerReaped(ctx, runner); err != nil {
		logActionFailure(ctx, daemon.logger, "runner_reap_confirm", err, runnerLogAttributes(runner)...)
		return
	}
	daemon.runnerReapRetries.clear(runner)
	logAction(ctx, daemon.logger, slog.LevelInfo, "runner_reap", "completed", runnerLogAttributes(runner)...)
}

func (daemon *clusteredDaemon) ensureRunnerReapRetries() {
	if daemon.runnerReapRetries == nil {
		daemon.runnerReapRetries = newRunnerReapRetryTracker(time.Now)
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
	if lifecycleStore, supported := daemon.assignments.(scaler.RunnerLifecycleStore); supported {
		configureAssignmentRunnerLifecycle(&reconciler, lifecycleStore, scaleSet, assignment)
	} else if scaleSet.Libvirt != nil {
		configureLegacyAssignmentLifecycle(daemon, &reconciler, assignment)
	}
	if assignment.IdentityFree {
		err = ensureIdentityFreeAssignedRunner(ctx, reconciler)
	} else {
		err = ensureAssignedRunner(ctx, reconciler, assignment.RequestID)
	}
	if err != nil {
		return err
	}
	if err = daemon.assignments.ConfirmRunning(ctx, assignment.ID, assignment.NodeID); err != nil {
		return fmt.Errorf("record started runner assignment: %w", err)
	}
	return nil
}

func ensureIdentityFreeAssignedRunner(ctx context.Context, reconciler scaler.Reconciler) error {
	startCtx, cancelStart := context.WithTimeout(ctx, clusterProviderStartTimeout)
	startErr := reconciler.StartOne(startCtx)
	cancelStart()
	inspectCtx, cancelInspect := context.WithTimeout(ctx, clusterProviderInspectTimeout)
	exists, inspectErr := reconciler.AssignedRunnerPresent(inspectCtx)
	cancelInspect()
	if inspectErr != nil {
		return errors.Join(startErr, fmt.Errorf("verify identity-free assigned runner: %w", inspectErr))
	}
	if exists {
		return nil
	}
	if startErr != nil {
		return fmt.Errorf("start identity-free assigned runner: %w", startErr)
	}
	return errors.New("provider did not start the identity-free assigned runner")
}

func configureAssignmentRunnerLifecycle(
	reconciler *scaler.Reconciler,
	lifecycleStore scaler.RunnerLifecycleStore,
	scaleSet config.ScaleSetConfig,
	assignment *cluster.Assignment,
) {
	provider := scaler.RunnerProviderDocker
	if scaleSet.Libvirt != nil {
		provider = scaler.RunnerProviderLibvirt
	}
	reconciler.ScaleSet.Lifecycle = lifecycleStore
	reconciler.ScaleSet.LifecycleScaleSet = assignment.ScaleSet
	reconciler.ScaleSet.Repository = assignment.Repository
	reconciler.ScaleSet.ProviderKind = provider
	reconciler.ScaleSet.NodeID = string(assignment.NodeID)
	reconciler.ScaleSet.AssignmentID = int64(assignment.ID)
	reconciler.ScaleSet.InstallationID = int64(assignment.InstallationID)
	reconciler.ScaleSet.GitHubScaleSetID = assignment.GitHubScaleSetID
}

func configureLegacyAssignmentLifecycle(
	daemon *clusteredDaemon,
	reconciler *scaler.Reconciler,
	assignment *cluster.Assignment,
) {
	reconciler.ScaleSet.RegisterRunner = func(registerCtx context.Context, runnerName string, requestID int64) error {
		return daemon.assignments.RegisterLibvirtRunner(registerCtx, assignment.ID, cluster.LibvirtRunnerLifecycle{
			NodeID: assignment.NodeID, ScaleSet: assignment.ScaleSet, Repository: assignment.Repository,
			RequestID: requestID, RunnerName: runnerName,
		})
	}
}

func (daemon *clusteredDaemon) executePendingLibvirtReaps(ctx context.Context) bool {
	pending, err := daemon.assignments.PendingLibvirtReaps(ctx, daemon.nodeID)
	if err != nil {
		daemon.logger.Error("load pending libvirt runner reaps failed", slog.String("error", err.Error()))
		return false
	}
	configuration := daemon.configurationSnapshot()
	for _, runner := range pending {
		daemon.executePendingLibvirtReap(ctx, configuration.config.ScaleSets, runner)
	}
	return true
}

func (daemon *clusteredDaemon) executePendingLibvirtReap(
	ctx context.Context,
	scaleSets []config.ScaleSetConfig,
	runner cluster.LibvirtRunnerLifecycle,
) {
	scaleSet, exists := localScaleSet(scaleSets, runner.ScaleSet)
	if !exists || scaleSet.Libvirt == nil {
		daemon.logger.Warn("pending libvirt runner reap retained because local configuration is unavailable",
			slog.String("scaleSet", runner.ScaleSet), slog.String("repository", runner.Repository),
			slog.String("runnerName", runner.RunnerName))
		return
	}
	absent, err := daemon.reapClusterLibvirtRunner(ctx, scaleSet, runner)
	if err != nil {
		daemon.logger.Error("pending libvirt runner reap failed",
			slog.String("runnerName", runner.RunnerName), slog.String("error", err.Error()))
		return
	}
	if !absent {
		return
	}
	if err := daemon.assignments.ConfirmLibvirtReaped(ctx, runner); err != nil {
		daemon.logger.Error("confirm pending libvirt runner absence failed",
			slog.String("runnerName", runner.RunnerName), slog.String("error", err.Error()))
	}
}

func (daemon *clusteredDaemon) reapClusterLibvirtRunner(
	ctx context.Context,
	scaleSet config.ScaleSetConfig,
	runner cluster.LibvirtRunnerLifecycle,
) (bool, error) {
	reconciler, err := daemon.buildReconciler(ctx, runner.Repository, scaleSet, nil, 0)
	if err != nil {
		return false, fmt.Errorf("build pending libvirt runner reaper: %w", err)
	}
	reapCtx, cancelReap := context.WithTimeout(ctx, libvirtReapTimeout)
	defer cancelReap()
	absent, err := reconciler.ReapRunner(reapCtx, runner.RunnerName)
	if err != nil {
		return false, fmt.Errorf("reap runner %q: %w", runner.RunnerName, err)
	}
	return absent, nil
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
