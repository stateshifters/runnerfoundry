package scaler

import (
	"context"
	"errors"
	"fmt"
)

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
