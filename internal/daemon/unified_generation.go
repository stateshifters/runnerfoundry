package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

type cycleEngine interface {
	Activate() error
	Deactivate(ctx context.Context) error
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
	RunnerPool() core.RunnerPool
	Terminate(ctx context.Context) (remainingProcessing int, err error)
}

type gatedCycleEngine struct {
	engine     cycleEngineRuntime
	leadership generationLeadership
}

type cycleEngineRuntime interface {
	Cycle(ctx context.Context) error
	Close(ctx context.Context) error
}

type generationLeadership interface {
	leadershipRuntime
	Activate() error
	Close(ctx context.Context) error
}

func newGatedCycleEngine(engine cycleEngineRuntime, leadership generationLeadership) *gatedCycleEngine {
	return &gatedCycleEngine{engine: engine, leadership: leadership}
}

func (engine *gatedCycleEngine) Activate() error {
	if err := engine.leadership.Activate(); err != nil {
		return fmt.Errorf("activate local generation leadership: %w", err)
	}
	return nil
}

func (engine *gatedCycleEngine) Deactivate(ctx context.Context) error {
	if err := engine.leadership.Close(ctx); err != nil {
		return fmt.Errorf("deactivate local generation leadership: %w", err)
	}
	return nil
}

func (engine *gatedCycleEngine) Cycle(ctx context.Context) error {
	active, err := engine.leadership.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("inspect local generation leadership: %w", err)
	}
	if !active {
		return errors.New("runtime generation does not own the local gate")
	}
	if err := engine.engine.Cycle(ctx); err != nil {
		return fmt.Errorf("run active generation cycle: %w", err)
	}
	return nil
}

func (engine *gatedCycleEngine) Close(ctx context.Context) error {
	return errors.Join(engine.engine.Close(ctx), engine.leadership.Close(ctx))
}

// maintenanceGeneration owns one prepared, restartable set of runtime
// resources. Activation does not commit until one complete cycle succeeds.
type maintenanceGeneration struct {
	engine     cycleEngine
	ticker     cycleTicker
	report     func(error)
	closers    []runtimeCloser
	finalizers []runtimeFinalizer

	mutex         sync.Mutex
	closeMutex    sync.Mutex
	cycleMutex    sync.Mutex
	runCancel     context.CancelFunc
	runDone       chan struct{}
	prepared      bool
	active        bool
	tickerStopped bool
	engineClosed  bool
	closerClosed  []bool
	closed        bool
	failures      map[string]string
}

// Keep the internal construction name stable while callers migrate away from
// the old transitional terminology.
type unifiedGeneration = maintenanceGeneration

func newUnifiedGeneration(
	engine cycleEngine,
	ticker cycleTicker,
	report func(error),
	closers []runtimeCloser,
	finalizers []runtimeFinalizer,
) *maintenanceGeneration {
	if report == nil {
		report = func(error) {}
	}
	return &maintenanceGeneration{
		engine: engine, ticker: ticker, report: report,
		closers: append([]runtimeCloser(nil), closers...), finalizers: append([]runtimeFinalizer(nil), finalizers...),
		closerClosed: make([]bool, len(closers)),
		failures:     make(map[string]string),
	}
}

func (generation *maintenanceGeneration) Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer {
	wanted := make(map[core.RunnerPool]struct{}, len(runnerPools))
	for _, runnerPool := range runnerPools {
		wanted[runnerPool] = struct{}{}
	}
	selected := make([]runtimeFinalizer, 0, len(generation.finalizers))
	for _, finalizer := range generation.finalizers {
		if _, exists := wanted[finalizer.RunnerPool()]; exists {
			selected = append(selected, finalizer)
		}
	}
	return selected
}

func (generation *maintenanceGeneration) Prepare(ctx context.Context) error {
	generation.mutex.Lock()
	defer generation.mutex.Unlock()
	if generation.prepared {
		return errors.New("runtime generation is already prepared")
	}
	if generation.closed {
		return errors.New("runtime generation is already closed")
	}
	generation.prepared = true
	return nil
}

// Start is retained as a compatibility spelling for tests and internal callers.
func (generation *maintenanceGeneration) Start(ctx context.Context) error {
	return generation.Prepare(ctx)
}

func (generation *maintenanceGeneration) Activate(ctx context.Context) error {
	if err := generation.activationReadiness(); err != nil {
		return err
	}
	if err := generation.activateEngine(ctx); err != nil {
		return err
	}
	generation.startRunLoop(ctx)
	return nil
}

func (generation *maintenanceGeneration) activationReadiness() error {
	generation.mutex.Lock()
	defer generation.mutex.Unlock()
	if !generation.prepared {
		return errors.New("runtime generation is not prepared")
	}
	if generation.closed {
		return errors.New("runtime generation is already closed")
	}
	if generation.active {
		return errors.New("runtime generation is already active")
	}
	return nil
}

func (generation *maintenanceGeneration) activateEngine(ctx context.Context) error {
	if err := generation.engine.Activate(); err != nil {
		return fmt.Errorf("activate generation engine: %w", err)
	}
	generation.cycleMutex.Lock()
	err := generation.engine.Cycle(ctx)
	generation.cycleMutex.Unlock()
	if err != nil {
		deactivateErr := generation.engine.Deactivate(context.WithoutCancel(ctx))
		if deactivateErr != nil {
			deactivateErr = fmt.Errorf("deactivate failed generation engine: %w", deactivateErr)
		}
		return errors.Join(fmt.Errorf("run first maintenance cycle: %w", err), deactivateErr)
	}
	return nil
}

func (generation *maintenanceGeneration) startRunLoop(ctx context.Context) {
	generation.mutex.Lock()
	runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	generation.runCancel = cancel
	generation.runDone = make(chan struct{})
	generation.active = true
	done := generation.runDone
	generation.mutex.Unlock()
	go generation.run(runContext, done)
}

func (generation *maintenanceGeneration) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-generation.ticker.Ticks():
			generation.cycleMutex.Lock()
			err := generation.engine.Cycle(ctx)
			generation.cycleMutex.Unlock()
			generation.reportCycle(err)
		}
	}
}

func (generation *maintenanceGeneration) reportCycle(err error) {
	generation.mutex.Lock()
	defer generation.mutex.Unlock()
	if err == nil {
		if len(generation.failures) != 0 {
			clear(generation.failures)
			generation.report(nil)
		}
		return
	}
	operation := "maintenance_cycle"
	fingerprint := fmt.Sprintf("%T:%s", err, err.Error())
	if generation.failures[operation] == fingerprint {
		return
	}
	generation.failures[operation] = fingerprint
	generation.report(fmt.Errorf("runtime maintenance cycle: %w", err))
}

func (generation *maintenanceGeneration) Deactivate(ctx context.Context) error {
	generation.mutex.Lock()
	if !generation.active {
		generation.mutex.Unlock()
		return nil
	}
	cancel, done := generation.runCancel, generation.runDone
	generation.mutex.Unlock()
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("wait for runtime generation: %w", ctx.Err())
	}
	if err := generation.engine.Deactivate(ctx); err != nil {
		return fmt.Errorf("deactivate generation engine: %w", err)
	}
	generation.mutex.Lock()
	generation.active = false
	generation.runCancel = nil
	generation.runDone = nil
	generation.mutex.Unlock()
	return nil
}

func (generation *maintenanceGeneration) Close(ctx context.Context) error {
	generation.closeMutex.Lock()
	defer generation.closeMutex.Unlock()
	if err := generation.Deactivate(ctx); err != nil {
		return err
	}
	if generation.beginClose() {
		return nil
	}
	closeErr := errors.Join(generation.closeEngine(ctx), generation.closeResources(ctx))
	if closeErr == nil {
		generation.mutex.Lock()
		generation.closed = true
		generation.mutex.Unlock()
	}
	return closeErr
}

func (generation *maintenanceGeneration) beginClose() bool {
	generation.mutex.Lock()
	defer generation.mutex.Unlock()
	if generation.closed {
		return true
	}
	if !generation.tickerStopped {
		generation.ticker.Stop()
		generation.tickerStopped = true
	}
	return false
}

func (generation *maintenanceGeneration) closeEngine(ctx context.Context) error {
	if generation.engineClosed {
		return nil
	}
	if err := generation.engine.Close(ctx); err != nil {
		return fmt.Errorf("close generation engine: %w", err)
	}
	generation.engineClosed = true
	return nil
}

func (generation *maintenanceGeneration) closeResources(ctx context.Context) error {
	var closeErrors []error
	for index := len(generation.closers) - 1; index >= 0; index-- {
		if generation.closerClosed[index] {
			continue
		}
		if err := generation.closers[index].Close(ctx); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close generation resource %d: %w", index, err))
		} else {
			generation.closerClosed[index] = true
		}
	}
	return errors.Join(closeErrors...)
}

func (generation *maintenanceGeneration) Stop(ctx context.Context) error {
	return generation.Close(ctx)
}
