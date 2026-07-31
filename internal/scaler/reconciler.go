package scaler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RunnerState string

const (
	RunnerStateRunning   RunnerState = "running"
	RunnerStateCompleted RunnerState = "completed"
	RunnerStateUnknown   RunnerState = "unknown"
)

type RunnerInstance struct {
	ID         string
	RunnerName string
	State      RunnerState
}

type RunnerStartRequest struct {
	ScaleSetName string
	RunnerName   string
	JITConfig    string
}

type Provider interface {
	List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error)
	Start(ctx context.Context, request RunnerStartRequest) error
	Destroy(ctx context.Context, instance RunnerInstance) error
}

type terminalRunnerDestroyer interface {
	DestroyTerminal(ctx context.Context, instance RunnerInstance) error
}

type JITConfigGenerator interface {
	Generate(ctx context.Context, runnerName string) (JITRunnerConfig, error)
	Remove(ctx context.Context, runner JITRunner) error
}

type jitRunnerRegistry interface {
	Find(ctx context.Context, runnerName string) (JITRunner, bool, error)
}

type JITRunnerConfig struct {
	Runner    JITRunner
	JITConfig string
}

type JITRunner struct {
	ID   int64
	Name string
}

type ScaleSetRuntime struct {
	Name              string
	LifecycleScaleSet string
	Repository        string
	MaxRunners        int
	ProviderKind      RunnerProvider
	Provider          Provider
	JITConfig         JITConfigGenerator
	Lifecycle         RunnerLifecycleStore
	NodeID            string
	AssignmentID      int64
	InstallationID    int64
	GitHubScaleSetID  int
	Now               func() time.Time
	RegisterRunner    func(ctx context.Context, runnerName string, requestID int64) error
}

type Reconciler struct {
	ScaleSet ScaleSetRuntime
}

func (reconciler Reconciler) Running(ctx context.Context) (int, error) {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return 0, fmt.Errorf("list runner instances: %w", err)
	}
	runningCount, _, err := reconciler.cleanupCompletedRunners(ctx, instances)
	return runningCount, err
}

func (reconciler Reconciler) StartOne(ctx context.Context) error {
	return reconciler.startRequestedRunner(ctx, "", 0)
}

// StartNamed starts the runner identified within this scale set by identifier.
func (reconciler Reconciler) StartNamed(ctx context.Context, identifier int64) error {
	return reconciler.startRequestedRunner(ctx, runnerName(reconciler.ScaleSet.Name, identifier), identifier)
}

func (reconciler Reconciler) startRequestedRunner(ctx context.Context, runnerName string, requestID int64) error {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return fmt.Errorf("list runner instances: %w", err)
	}
	runningCount, usedRunnerNames, err := reconciler.cleanupCompletedRunners(ctx, instances)
	if err != nil {
		return err
	}
	if runningCount >= reconciler.ScaleSet.MaxRunners {
		return reconciler.reconcileBlockedCapacity(ctx)
	}
	if runnerName != "" {
		usedRunnerNames[runnerName] = true
		return reconciler.startRunner(ctx, runnerName, requestID)
	}
	if err = reconciler.reserveLifecycleRunnerNames(ctx, usedRunnerNames); err != nil {
		return err
	}
	return reconciler.startMissingRunners(ctx, runningCount, runningCount+1, usedRunnerNames)
}

func (reconciler Reconciler) reconcileBlockedCapacity(ctx context.Context) error {
	if reconciler.ScaleSet.Lifecycle == nil {
		return nil
	}
	_, err := reconciler.ReconcileRunnerUsability(ctx)
	return err
}

// Exists reports whether the identified runner is currently running in this scale set.
func (reconciler Reconciler) Exists(ctx context.Context, identifier int64) (bool, error) {
	expectedRunnerName := runnerName(reconciler.ScaleSet.Name, identifier)
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return false, fmt.Errorf("list runner instances: %w", err)
	}
	for _, instance := range instances {
		if instance.RunnerName == expectedRunnerName && instance.State == RunnerStateRunning {
			return true, nil
		}
	}
	return false, nil
}

// ReapRunner force-removes one terminal runner and confirms that its owned
// provider instance is absent. A false result is successful but incomplete;
// callers must retain and retry their durable cleanup obligation.
func (reconciler Reconciler) DestroyActive(ctx context.Context) error {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return fmt.Errorf("list runner instances: %w", err)
	}
	for _, instance := range instances {
		if instance.State == RunnerStateUnknown {
			continue
		}
		if err := reconciler.ScaleSet.Provider.Destroy(ctx, instance); err != nil {
			return fmt.Errorf("destroy runner %q: %w", instance.RunnerName, err)
		}
	}
	return nil
}

func DesiredRunnerCount(assignedJobs int, maxRunners int) int {
	if assignedJobs < 0 {
		return 0
	}
	if assignedJobs > maxRunners {
		return maxRunners
	}
	return assignedJobs
}

func (reconciler Reconciler) Reconcile(ctx context.Context, assignedJobs int) (int, error) {
	desiredCount := DesiredRunnerCount(assignedJobs, reconciler.ScaleSet.MaxRunners)
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return 0, fmt.Errorf("list runner instances: %w", err)
	}
	runningCount, usedRunnerNames, err := reconciler.cleanupCompletedRunners(ctx, instances)
	if err != nil {
		return 0, err
	}
	if err = reconciler.reserveLifecycleRunnerNames(ctx, usedRunnerNames); err != nil {
		return 0, err
	}
	if err := reconciler.startMissingRunners(ctx, runningCount, desiredCount, usedRunnerNames); err != nil {
		return 0, err
	}
	return desiredCount, nil
}

func (reconciler Reconciler) reserveLifecycleRunnerNames(ctx context.Context, usedRunnerNames map[string]bool) error {
	inventory, supported := reconciler.ScaleSet.Lifecycle.(RunnerLifecycleInventory)
	if !supported {
		return nil
	}
	lifecycles, err := inventory.RunnerLifecycles(ctx, reconciler.lifecycleScaleSet(), "")
	if err != nil {
		return fmt.Errorf("load reserved runner names: %w", err)
	}
	for _, runner := range lifecycles {
		if runner.RunnerName != "" {
			usedRunnerNames[runner.RunnerName] = true
		}
	}
	return nil
}

func (reconciler Reconciler) Drain(ctx context.Context) (int, error) {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return 0, fmt.Errorf("list runner instances: %w", err)
	}
	runningCount, _, err := reconciler.cleanupCompletedRunners(ctx, instances)
	if err != nil {
		return 0, err
	}
	return runningCount, nil
}

func (reconciler Reconciler) cleanupCompletedRunners(ctx context.Context, instances []RunnerInstance) (int, map[string]bool, error) {
	runningCount := 0
	usedRunnerNames := map[string]bool{}
	for _, instance := range instances {
		if instance.RunnerName != "" {
			usedRunnerNames[instance.RunnerName] = true
		}
		if instance.State == RunnerStateCompleted {
			if err := reconciler.ScaleSet.Provider.Destroy(ctx, instance); err != nil {
				return 0, nil, fmt.Errorf("destroy completed runner %q: %w", instance.RunnerName, err)
			}
			continue
		}
		if instance.State == RunnerStateRunning {
			runningCount++
		}
	}
	return runningCount, usedRunnerNames, nil
}

func (reconciler Reconciler) startMissingRunners(ctx context.Context, runningCount int, desiredCount int, usedRunnerNames map[string]bool) error {
	nextRunnerIndex := 1
	for runnerIndex := runningCount; runnerIndex < desiredCount; runnerIndex++ {
		runnerName := reconciler.nextRunnerName(usedRunnerNames, &nextRunnerIndex)
		if err := reconciler.startRunner(ctx, runnerName, 0); err != nil {
			return err
		}
	}
	return nil
}

func (reconciler Reconciler) startRunner(ctx context.Context, runnerName string, requestID int64) error {
	jitConfig, err := reconciler.ScaleSet.JITConfig.Generate(ctx, runnerName)
	if err != nil {
		return fmt.Errorf("generate jit config for %q: %w", runnerName, err)
	}
	if err = reconciler.registerRunnerLifecycle(ctx, runnerName, requestID, jitConfig.Runner); err != nil {
		return err
	}
	if err = reconciler.startProviderRunner(ctx, runnerName, requestID, jitConfig); err != nil {
		return err
	}
	return reconciler.confirmRunnerWaiting(ctx, runnerName, jitConfig.Runner.ID)
}

func (reconciler Reconciler) startProviderRunner(
	ctx context.Context,
	runnerName string,
	requestID int64,
	jitConfig JITRunnerConfig,
) error {
	startErr := reconciler.ScaleSet.Provider.Start(ctx, RunnerStartRequest{
		ScaleSetName: reconciler.ScaleSet.Name,
		RunnerName:   runnerName,
		JITConfig:    jitConfig.JITConfig,
	})
	if startErr == nil {
		return nil
	}
	if reconciler.ScaleSet.Lifecycle != nil {
		return reconciler.retainFailedStart(ctx, runnerName, requestID, startErr)
	}
	removeErr := reconciler.ScaleSet.JITConfig.Remove(ctx, jitConfig.Runner)
	if removeErr != nil {
		startErr = errors.Join(startErr, fmt.Errorf("remove github runner: %w", removeErr))
	}
	return fmt.Errorf("start runner %q: %w", runnerName, startErr)
}

func (reconciler Reconciler) confirmRunnerWaiting(ctx context.Context, runnerName string, githubRunnerID int64) error {
	if reconciler.ScaleSet.Lifecycle == nil {
		return nil
	}
	confirmedAt := reconciler.now()
	err := reconciler.ScaleSet.Lifecycle.ConfirmRunnerWaiting(ctx, RunnerLifecycle{
		NodeID: reconciler.ScaleSet.NodeID, ScaleSet: reconciler.lifecycleScaleSet(), Repository: reconciler.ScaleSet.Repository,
		RunnerName: runnerName, GitHubRunnerID: githubRunnerID,
	}, confirmedAt)
	if err != nil {
		return fmt.Errorf("confirm runner lifecycle %q: %w", runnerName, err)
	}
	return nil
}

func (reconciler Reconciler) registerRunnerLifecycle(
	ctx context.Context,
	runnerName string,
	requestID int64,
	githubRunner JITRunner,
) error {
	if reconciler.ScaleSet.Lifecycle != nil {
		if err := reconciler.registerDurableRunner(ctx, runnerName, requestID, githubRunner); err != nil {
			return reconciler.cleanupFailedRegistration(ctx, runnerName, githubRunner, err)
		}
	}
	if reconciler.ScaleSet.RegisterRunner == nil {
		return nil
	}
	if err := reconciler.ScaleSet.RegisterRunner(ctx, runnerName, requestID); err != nil {
		return reconciler.cleanupFailedRegistration(ctx, runnerName, githubRunner, err)
	}
	return nil
}

func (reconciler Reconciler) registerDurableRunner(
	ctx context.Context,
	runnerName string,
	requestID int64,
	githubRunner JITRunner,
) error {
	if err := reconciler.ScaleSet.Lifecycle.RegisterRunner(ctx, RunnerLifecycle{
		NodeID: reconciler.ScaleSet.NodeID, ScaleSet: reconciler.lifecycleScaleSet(), Repository: reconciler.ScaleSet.Repository,
		Provider: reconciler.ScaleSet.ProviderKind, RunnerName: runnerName,
		RequestID: requestID, AssignmentID: reconciler.ScaleSet.AssignmentID,
		InstallationID: reconciler.ScaleSet.InstallationID, GitHubScaleSetID: reconciler.ScaleSet.GitHubScaleSetID,
		GitHubRunnerID: githubRunner.ID, Phase: RunnerProvisioning, CreatedAt: reconciler.now(),
	}); err != nil {
		return fmt.Errorf("persist runner lifecycle: %w", err)
	}
	return nil
}

func (reconciler Reconciler) cleanupFailedRegistration(
	ctx context.Context,
	runnerName string,
	githubRunner JITRunner,
	registrationErr error,
) error {
	removeErr := reconciler.ScaleSet.JITConfig.Remove(ctx, githubRunner)
	if removeErr != nil {
		registrationErr = errors.Join(registrationErr, fmt.Errorf("remove github runner: %w", removeErr))
	}
	return fmt.Errorf("register runner lifecycle %q: %w", runnerName, registrationErr)
}

func (reconciler Reconciler) retainFailedStart(ctx context.Context, runnerName string, requestID int64, startErr error) error {
	reapErr := reconciler.ScaleSet.Lifecycle.RequestRunnerReap(
		ctx, reconciler.lifecycleScaleSet(), reconciler.ScaleSet.Repository, requestID, runnerName,
		"provider_start_failed", reconciler.now(),
	)
	if reapErr != nil {
		return fmt.Errorf("start runner %q: %w", runnerName, errors.Join(startErr, fmt.Errorf("retain cleanup obligation: %w", reapErr)))
	}
	return fmt.Errorf("start runner %q: %w", runnerName, startErr)
}

func (reconciler Reconciler) lifecycleScaleSet() string {
	if reconciler.ScaleSet.LifecycleScaleSet != "" {
		return reconciler.ScaleSet.LifecycleScaleSet
	}
	return reconciler.ScaleSet.Name
}

func (reconciler Reconciler) now() time.Time {
	if reconciler.ScaleSet.Now != nil {
		return reconciler.ScaleSet.Now().UTC()
	}
	return time.Now().UTC()
}

func (reconciler Reconciler) nextRunnerName(usedRunnerNames map[string]bool, nextRunnerIndex *int) string {
	for {
		candidateRunnerName := runnerName(reconciler.ScaleSet.Name, int64(*nextRunnerIndex))
		(*nextRunnerIndex)++
		if !usedRunnerNames[candidateRunnerName] {
			usedRunnerNames[candidateRunnerName] = true
			return candidateRunnerName
		}
	}
}
