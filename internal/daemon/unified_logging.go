package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

type preparedLogChange interface {
	Activate() error
	Commit() error
	Rollback() error
	Close() error
}

type loggingGeneration struct {
	generation generation
	prepared   preparedLogChange
}

func (wrapped *loggingGeneration) Prepare(ctx context.Context) error {
	if err := wrapped.generation.Prepare(ctx); err != nil {
		return errors.Join(err, wrapped.prepared.Close())
	}
	return nil
}

func (wrapped *loggingGeneration) Activate(ctx context.Context) error {
	if err := wrapped.prepared.Activate(); err != nil {
		return fmt.Errorf("activate replacement logging: %w", err)
	}
	if err := wrapped.generation.Activate(ctx); err != nil {
		return errors.Join(fmt.Errorf("activate logging runtime generation: %w", err), wrapped.prepared.Rollback())
	}
	if err := wrapped.prepared.Commit(); err != nil {
		return fmt.Errorf("commit replacement logging: %w", err)
	}
	return nil
}

func (wrapped *loggingGeneration) Deactivate(ctx context.Context) error {
	if err := wrapped.generation.Deactivate(ctx); err != nil {
		return fmt.Errorf("deactivate logging runtime generation: %w", err)
	}
	return nil
}

func (wrapped *loggingGeneration) Close(ctx context.Context) error {
	return errors.Join(wrapped.generation.Close(ctx), wrapped.prepared.Close())
}

func (wrapped *loggingGeneration) Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer {
	return wrapped.generation.Finalizers(runnerPools)
}
