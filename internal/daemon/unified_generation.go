package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type cycleEngine interface {
	Cycle(ctx context.Context) error
	Close(ctx context.Context) error
}

type cycleTicker interface {
	Ticks() <-chan time.Time
	Stop()
}

type runtimeCloser interface {
	Close(ctx context.Context) error
}

type runtimeFinalizer interface {
	Cleanup(ctx context.Context) error
}

type gatedCycleEngine struct {
	engine     cycleEngine
	leadership generationLeadership
}

type generationLeadership interface {
	leadershipRuntime
	Close(ctx context.Context) error
}

func newGatedCycleEngine(engine cycleEngine, leadership generationLeadership) *gatedCycleEngine {
	return &gatedCycleEngine{engine: engine, leadership: leadership}
}

func (engine *gatedCycleEngine) Cycle(ctx context.Context) error {
	active, err := engine.leadership.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("inspect local generation leadership: %w", err)
	}
	if !active {
		return nil
	}
	if err := engine.engine.Cycle(ctx); err != nil {
		return fmt.Errorf("run active generation cycle: %w", err)
	}
	return nil
}

func (engine *gatedCycleEngine) Close(ctx context.Context) error {
	return errors.Join(engine.engine.Close(ctx), engine.leadership.Close(ctx))
}

type unifiedGeneration struct {
	engine     cycleEngine
	ticker     cycleTicker
	report     func(error)
	closers    []runtimeCloser
	finalizers []runtimeFinalizer

	mutex     sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}
	closed    bool
	finalized bool
}

func newUnifiedGeneration(
	engine cycleEngine,
	ticker cycleTicker,
	report func(error),
	closers []runtimeCloser,
	finalizers []runtimeFinalizer,
) *unifiedGeneration {
	if report == nil {
		report = func(error) {}
	}
	return &unifiedGeneration{
		engine: engine, ticker: ticker, report: report,
		closers:    append([]runtimeCloser(nil), closers...),
		finalizers: append([]runtimeFinalizer(nil), finalizers...),
	}
}

// Shutdown performs process-final cleanup after stopping claims and source
// polls. Reloads call Stop directly and therefore preserve active runners.
func (generation *unifiedGeneration) Shutdown(ctx context.Context) error {
	stopErr := generation.Stop(ctx)
	generation.mutex.Lock()
	if generation.finalized {
		generation.mutex.Unlock()
		return stopErr
	}
	generation.mutex.Unlock()

	cleanupErrors := make([]error, 0, len(generation.finalizers))
	for _, finalizer := range generation.finalizers {
		if err := finalizer.Cleanup(ctx); err != nil {
			cleanupErrors = append(cleanupErrors, err)
		}
	}
	cleanupErr := errors.Join(cleanupErrors...)
	if cleanupErr == nil {
		generation.mutex.Lock()
		generation.finalized = true
		generation.mutex.Unlock()
	}
	return errors.Join(stopErr, cleanupErr)
}

func (generation *unifiedGeneration) Start(ctx context.Context) error {
	generation.mutex.Lock()
	defer generation.mutex.Unlock()
	if generation.cancel != nil {
		return errors.New("runtime generation is already started")
	}
	if generation.closed {
		return errors.New("runtime generation is already closed")
	}
	runContext, cancel := context.WithCancel(ctx)
	generation.cancel = cancel
	generation.done = make(chan struct{})
	go generation.run(runContext)
	return nil
}

func (generation *unifiedGeneration) run(ctx context.Context) {
	defer close(generation.done)
	generation.runCycle(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-generation.ticker.Ticks():
			generation.runCycle(ctx)
		}
	}
}

func (generation *unifiedGeneration) runCycle(ctx context.Context) {
	if err := generation.engine.Cycle(ctx); err != nil {
		generation.report(fmt.Errorf("runtime maintenance cycle: %w", err))
	}
}

func (generation *unifiedGeneration) Stop(ctx context.Context) error {
	generation.mutex.Lock()
	if generation.closed {
		generation.mutex.Unlock()
		return nil
	}
	generation.closed = true
	cancel, done := generation.cancel, generation.done
	generation.mutex.Unlock()

	if cancel != nil {
		generation.ticker.Stop()
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("wait for runtime generation: %w", ctx.Err())
		}
	} else {
		generation.ticker.Stop()
	}

	closeErrors := make([]error, 0, len(generation.closers)+1)
	if err := generation.engine.Close(ctx); err != nil {
		closeErrors = append(closeErrors, err)
	}
	for index := len(generation.closers) - 1; index >= 0; index-- {
		if err := generation.closers[index].Close(ctx); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}
