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
func (reconciler Reconciler) ReapRunner(ctx context.Context, runnerName string) (bool, error) {
	terminalDestroyer, supported := reconciler.ScaleSet.Provider.(terminalRunnerDestroyer)
	if !supported {
		return false, errors.New("provider does not support terminal runner reaping")
	}
	instance, exists, err := reconciler.runnerInstance(ctx, runnerName)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	if err = terminalDestroyer.DestroyTerminal(ctx, instance); err != nil {
		return false, fmt.Errorf("destroy terminal runner %q: %w", runnerName, err)
	}
	_, exists, err = reconciler.runnerInstance(ctx, runnerName)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

// ReapRunnerLifecycle removes the exact provider instance and GitHub
// registration owned by a durable lifecycle. It reports success only after
// both resources are confirmed absent, so callers can safely retain and retry
// the lifecycle otherwise.
func (reconciler Reconciler) ReapRunnerLifecycle(ctx context.Context, runner RunnerLifecycle) (bool, error) {
	providerAbsent, err := reconciler.removeProviderRunner(ctx, runner.RunnerName)
	if err != nil || !providerAbsent {
		return false, err
	}
	return reconciler.removeGitHubRunner(ctx, runner)
}

func (reconciler Reconciler) removeProviderRunner(ctx context.Context, runnerName string) (bool, error) {
	instance, exists, err := reconciler.runnerInstance(ctx, runnerName)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	if err = reconciler.ScaleSet.Provider.Destroy(ctx, instance); err != nil {
		return false, fmt.Errorf("destroy runner %q: %w", runnerName, err)
	}
	_, exists, err = reconciler.runnerInstance(ctx, runnerName)
	return !exists, err
}

func (reconciler Reconciler) removeGitHubRunner(ctx context.Context, runner RunnerLifecycle) (bool, error) {
	githubRunner := JITRunner{ID: runner.GitHubRunnerID, Name: runner.RunnerName}
	registry, inspectable := reconciler.ScaleSet.JITConfig.(jitRunnerRegistry)
	if inspectable {
		return reconciler.removeInspectableGitHubRunner(ctx, registry, githubRunner)
	}
	if githubRunner.ID == 0 {
		return false, errors.New("github runner absence cannot be confirmed")
	}
	if err := reconciler.ScaleSet.JITConfig.Remove(ctx, githubRunner); err != nil {
		return false, fmt.Errorf("remove github runner %q: %w", runner.RunnerName, err)
	}
	return true, nil
}

func (reconciler Reconciler) removeInspectableGitHubRunner(
	ctx context.Context,
	registry jitRunnerRegistry,
	runner JITRunner,
) (bool, error) {
	registered, found, err := registry.Find(ctx, runner.Name)
	if err != nil {
		return false, fmt.Errorf("inspect github runner %q: %w", runner.Name, err)
	}
	if !found {
		return true, nil
	}
	if err = reconciler.ScaleSet.JITConfig.Remove(ctx, registered); err != nil {
		return false, fmt.Errorf("remove github runner %q: %w", runner.Name, err)
	}
	_, found, err = registry.Find(ctx, runner.Name)
	if err != nil {
		return false, fmt.Errorf("confirm github runner %q absence: %w", runner.Name, err)
	}
	return !found, nil
}

// ReconcileRunnerUsability verifies that waiting capacity still has a GitHub
// registration and conservatively adopts provider instances created before
// lifecycle tracking was introduced.
func (reconciler Reconciler) ReconcileRunnerUsability(ctx context.Context) (int, error) {
	inventory, inventorySupported := reconciler.ScaleSet.Lifecycle.(RunnerLifecycleInventory)
	registry, registrySupported := reconciler.ScaleSet.JITConfig.(jitRunnerRegistry)
	if !inventorySupported || !registrySupported {
		return 0, nil
	}
	lifecycles, err := inventory.RunnerLifecycles(ctx, reconciler.lifecycleScaleSet(), reconciler.ScaleSet.Repository)
	if err != nil {
		return 0, fmt.Errorf("load runner lifecycle inventory: %w", err)
	}
	tracked, reapRequests, err := reconciler.reapUnregisteredWaiting(ctx, registry, lifecycles)
	if err != nil {
		return reapRequests, err
	}
	adoptedReaps, err := reconciler.adoptUntrackedRunners(ctx, registry, tracked)
	return reapRequests + adoptedReaps, err
}

func (reconciler Reconciler) reapUnregisteredWaiting(
	ctx context.Context,
	registry jitRunnerRegistry,
	lifecycles []RunnerLifecycle,
) (map[string]RunnerLifecycle, int, error) {
	tracked := make(map[string]RunnerLifecycle, len(lifecycles))
	reapRequests := 0
	for _, runner := range lifecycles {
		tracked[runner.RunnerName] = runner
		if runner.Phase != RunnerWaitingForJob {
			continue
		}
		_, found, err := registry.Find(ctx, runner.RunnerName)
		if err != nil {
			return nil, reapRequests, fmt.Errorf("inspect waiting github runner %q: %w", runner.RunnerName, err)
		}
		if found {
			continue
		}
		if err = reconciler.ScaleSet.Lifecycle.RequestRunnerReap(ctx, runner.ScaleSet, runner.Repository,
			runner.RequestID, runner.RunnerName, "github_registration_absent", reconciler.now()); err != nil {
			return nil, reapRequests, fmt.Errorf("request orphaned runner reap %q: %w", runner.RunnerName, err)
		}
		reapRequests++
	}
	return tracked, reapRequests, nil
}

func (reconciler Reconciler) adoptUntrackedRunners(
	ctx context.Context,
	registry jitRunnerRegistry,
	tracked map[string]RunnerLifecycle,
) (int, error) {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return 0, fmt.Errorf("list provider instances for lifecycle adoption: %w", err)
	}
	reapRequests, err := reconciler.requestAbsentBusyRunnerReaps(ctx, tracked, instances)
	if err != nil {
		return reapRequests, err
	}
	for _, instance := range instances {
		reapRequested, adoptErr := reconciler.adoptRunnerInstance(ctx, registry, tracked, instance)
		if adoptErr != nil {
			return reapRequests, adoptErr
		}
		reapRequests += reapRequested
	}
	return reapRequests, nil
}

func (reconciler Reconciler) requestAbsentBusyRunnerReaps(
	ctx context.Context,
	tracked map[string]RunnerLifecycle,
	instances []RunnerInstance,
) (int, error) {
	present := make(map[string]bool, len(instances))
	for _, instance := range instances {
		present[instance.RunnerName] = true
	}
	reapRequests := 0
	for _, runner := range tracked {
		if runner.Phase != RunnerBusy || present[runner.RunnerName] {
			continue
		}
		if err := reconciler.ScaleSet.Lifecycle.RequestRunnerReap(
			ctx, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName,
			"provider_instance_absent", reconciler.now(),
		); err != nil {
			return reapRequests, fmt.Errorf("request absent busy runner reap %q: %w", runner.RunnerName, err)
		}
		reapRequests++
	}
	return reapRequests, nil
}

func (reconciler Reconciler) adoptRunnerInstance(
	ctx context.Context,
	registry jitRunnerRegistry,
	tracked map[string]RunnerLifecycle,
	instance RunnerInstance,
) (int, error) {
	if instance.RunnerName == "" {
		return 0, nil
	}
	if _, exists := tracked[instance.RunnerName]; exists {
		return 0, nil
	}
	registered, found, err := registry.Find(ctx, instance.RunnerName)
	if err != nil {
		return 0, fmt.Errorf("inspect adopted github runner %q: %w", instance.RunnerName, err)
	}
	adopted := reconciler.missingRegistrationLifecycle(instance.RunnerName)
	if found {
		adopted = reconciler.busyAdoptedLifecycle(instance.RunnerName, registered.ID)
	}
	if err = reconciler.ScaleSet.Lifecycle.RegisterRunner(ctx, adopted); err != nil {
		return 0, fmt.Errorf("adopt provider runner %q: %w", instance.RunnerName, err)
	}
	if found {
		return 0, nil
	}
	return 1, nil
}

func (reconciler Reconciler) missingRegistrationLifecycle(runnerName string) RunnerLifecycle {
	return reconciler.adoptedLifecycle(runnerName, 0, RunnerReapPending, "github_registration_absent")
}

func (reconciler Reconciler) busyAdoptedLifecycle(runnerName string, githubRunnerID int64) RunnerLifecycle {
	return reconciler.adoptedLifecycle(runnerName, githubRunnerID, RunnerBusy, "")
}

func (reconciler Reconciler) adoptedLifecycle(
	runnerName string,
	githubRunnerID int64,
	phase RunnerLifecyclePhase,
	reason string,
) RunnerLifecycle {
	now := reconciler.now()
	return RunnerLifecycle{NodeID: reconciler.ScaleSet.NodeID, ScaleSet: reconciler.lifecycleScaleSet(),
		Repository: reconciler.ScaleSet.Repository, Provider: reconciler.ScaleSet.ProviderKind,
		RunnerName: runnerName, GitHubRunnerID: githubRunnerID, Phase: phase,
		CreatedAt: now, UpdatedAt: now, ReapReason: reason}
}

func (reconciler Reconciler) runnerInstance(ctx context.Context, runnerName string) (RunnerInstance, bool, error) {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return RunnerInstance{}, false, fmt.Errorf("list runner instances: %w", err)
	}
	for _, instance := range instances {
		if instance.RunnerName == runnerName {
			return instance, true, nil
		}
	}
	return RunnerInstance{}, false, nil
}

func (reconciler Reconciler) RunnerPresent(ctx context.Context, runnerName string) (bool, error) {
	_, exists, err := reconciler.runnerInstance(ctx, runnerName)
	return exists, err
}

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
	if err := reconciler.startMissingRunners(ctx, runningCount, desiredCount, usedRunnerNames); err != nil {
		return 0, err
	}
	return desiredCount, nil
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
