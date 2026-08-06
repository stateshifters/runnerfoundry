package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

type preparedLogChange interface {
	Activate() error
	Close() error
}

type loggingGeneration struct {
	generation generation
	prepared   preparedLogChange
	activated  bool
}

func (wrapped *loggingGeneration) Start(ctx context.Context) error {
	if err := wrapped.generation.Start(ctx); err != nil {
		return errors.Join(err, wrapped.prepared.Close())
	}
	wrapped.activated = true
	if err := wrapped.prepared.Activate(); err != nil {
		return errors.Join(fmt.Errorf("activate replacement logging: %w", err),
			wrapped.generation.Stop(context.WithoutCancel(ctx)))
	}
	return nil
}

func (wrapped *loggingGeneration) Activate() error {
	if err := wrapped.generation.Activate(); err != nil {
		return fmt.Errorf("activate logging runtime generation: %w", err)
	}
	return nil
}

func (wrapped *loggingGeneration) Stop(ctx context.Context) error {
	if wrapped.activated {
		if err := wrapped.generation.Stop(ctx); err != nil {
			return fmt.Errorf("stop logging generation: %w", err)
		}
		return nil
	}
	return errors.Join(wrapped.generation.Stop(ctx), wrapped.prepared.Close())
}

func (wrapped *loggingGeneration) Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer {
	return wrapped.generation.Finalizers(runnerPools)
}
