package scaler

import (
	"context"
	"fmt"
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

type JITConfigGenerator interface {
	Generate(ctx context.Context, runnerName string) (JITRunnerConfig, error)
	Remove(ctx context.Context, runner JITRunner) error
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
	Name       string
	MaxRunners int
	Provider   Provider
	JITConfig  JITConfigGenerator
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
	return reconciler.startOne(ctx, "")
}

func (reconciler Reconciler) StartNamed(ctx context.Context, runnerName string) error {
	return reconciler.startOne(ctx, runnerName)
}

func (reconciler Reconciler) startOne(ctx context.Context, runnerName string) error {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return fmt.Errorf("list runner instances: %w", err)
	}
	runningCount, usedRunnerNames, err := reconciler.cleanupCompletedRunners(ctx, instances)
	if err != nil {
		return err
	}
	if runningCount >= reconciler.ScaleSet.MaxRunners {
		return nil
	}
	if runnerName != "" {
		usedRunnerNames[runnerName] = true
		return reconciler.startRunner(ctx, runnerName)
	}
	return reconciler.startMissingRunners(ctx, runningCount, runningCount+1, usedRunnerNames)
}

func (reconciler Reconciler) Exists(ctx context.Context, runnerName string) (bool, error) {
	instances, err := reconciler.ScaleSet.Provider.List(ctx, reconciler.ScaleSet.Name)
	if err != nil {
		return false, fmt.Errorf("list runner instances: %w", err)
	}
	for _, instance := range instances {
		if instance.RunnerName == runnerName && instance.State == RunnerStateRunning {
			return true, nil
		}
	}
	return false, nil
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
		if err := reconciler.startRunner(ctx, runnerName); err != nil {
			return err
		}
	}
	return nil
}

func (reconciler Reconciler) startRunner(ctx context.Context, runnerName string) error {
	jitConfig, err := reconciler.ScaleSet.JITConfig.Generate(ctx, runnerName)
	if err != nil {
		return fmt.Errorf("generate jit config for %q: %w", runnerName, err)
	}
	if err := reconciler.ScaleSet.Provider.Start(ctx, RunnerStartRequest{
		ScaleSetName: reconciler.ScaleSet.Name,
		RunnerName:   runnerName,
		JITConfig:    jitConfig.JITConfig,
	}); err != nil {
		if removeErr := reconciler.ScaleSet.JITConfig.Remove(ctx, jitConfig.Runner); removeErr != nil {
			return fmt.Errorf("start runner %q: %w; remove github runner: %w", runnerName, err, removeErr)
		}
		return fmt.Errorf("start runner %q: %w", runnerName, err)
	}
	return nil
}

func (reconciler Reconciler) nextRunnerName(usedRunnerNames map[string]bool, nextRunnerIndex *int) string {
	for {
		runnerName := fmt.Sprintf("%s-%d", reconciler.ScaleSet.Name, *nextRunnerIndex)
		(*nextRunnerIndex)++
		if !usedRunnerNames[runnerName] {
			usedRunnerNames[runnerName] = true
			return runnerName
		}
	}
}
