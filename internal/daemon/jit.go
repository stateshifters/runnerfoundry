package daemon

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type scalesetJITGenerator struct {
	Client     scaleSetJITClient
	ScaleSetID int
}

func (generator scalesetJITGenerator) Generate(ctx context.Context, runnerName string) (scaler.JITRunnerConfig, error) {
	jitConfig, err := generator.Client.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, generator.ScaleSetID)
	if err != nil {
		if !errors.Is(err, upstream.RunnerExistsError) {
			return scaler.JITRunnerConfig{}, fmt.Errorf("generate jit runner config: %w", err)
		}
		return generator.replaceStaleRunnerAndGenerate(ctx, runnerName, err)
	}
	return scaler.JITRunnerConfig{
		Runner:    jitRunnerReference(jitConfig.Runner),
		JITConfig: jitConfig.EncodedJITConfig,
	}, nil
}

func (generator scalesetJITGenerator) replaceStaleRunnerAndGenerate(
	ctx context.Context,
	runnerName string,
	generateErr error,
) (scaler.JITRunnerConfig, error) {
	staleRunner, err := generator.Client.GetRunnerByName(ctx, runnerName)
	if err != nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("find stale runner after jit conflict for %q: %w", runnerName, err)
	}
	if staleRunner == nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("find stale runner after jit conflict for %q: %w", runnerName, generateErr)
	}
	removeErr := generator.Client.RemoveRunner(ctx, int64(staleRunner.ID))
	if removeErr != nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("remove stale runner %q: %w", runnerName, removeErr)
	}
	jitConfig, err := generator.Client.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, generator.ScaleSetID)
	if err != nil {
		return scaler.JITRunnerConfig{}, fmt.Errorf("generate jit runner config after stale runner cleanup: %w", err)
	}
	return scaler.JITRunnerConfig{
		Runner:    jitRunnerReference(jitConfig.Runner),
		JITConfig: jitConfig.EncodedJITConfig,
	}, nil
}

func (generator scalesetJITGenerator) Remove(ctx context.Context, runner scaler.JITRunner) error {
	if runner.ID == 0 {
		return nil
	}
	if err := generator.Client.RemoveRunner(ctx, runner.ID); err != nil {
		return fmt.Errorf("remove jit runner %q: %w", runner.Name, err)
	}
	return nil
}

func jitRunnerReference(runner *upstream.RunnerReference) scaler.JITRunner {
	if runner == nil {
		return scaler.JITRunner{}
	}
	return scaler.JITRunner{ID: int64(runner.ID), Name: runner.Name}
}
