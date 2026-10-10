package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

type maintenanceLaneRuntime interface {
	MaintainMembership(ctx context.Context) error
	MaintainSource(ctx context.Context) error
	MaintainExecution(ctx context.Context) error
}

type cycleTicker interface {
	Ticks() <-chan time.Time
	Stop()
}

type maintenanceLaneTickers struct {
	membership cycleTicker
	source     cycleTicker
	execution  cycleTicker
}

type runtimeCloser interface {
	Close(ctx context.Context) error
}

type runtimeFinalizer interface {
	RunnerPool() core.RunnerPool
	Terminate(ctx context.Context) (remainingProcessing int, err error)
}

type queuedJobNotificationRuntime interface {
	Start(startupContext, runtimeContext context.Context) error
	Stop(ctx context.Context) error
}

type gatedMaintenanceEngine struct {
	engine     maintenanceEngineRuntime
	leadership generationLeadership
}

type maintenanceEngineRuntime interface {
	Close(ctx context.Context) error
	maintenanceLaneRuntime
}

type generationLeadership interface {
	leadershipRuntime
	Activate() error
	Close(ctx context.Context) error
}

func newGatedMaintenanceEngine(engine maintenanceEngineRuntime, leadership generationLeadership) *gatedMaintenanceEngine {
	return &gatedMaintenanceEngine{engine: engine, leadership: leadership}
}

func (engine *gatedMaintenanceEngine) Activate() error {
	if err := engine.leadership.Activate(); err != nil {
		return fmt.Errorf("activate local generation leadership: %w", err)
	}
	return nil
}

func (engine *gatedMaintenanceEngine) Deactivate(ctx context.Context) error {
	if err := engine.leadership.Close(ctx); err != nil {
		return fmt.Errorf("deactivate local generation leadership: %w", err)
	}
	return nil
}

func (engine *gatedMaintenanceEngine) MaintainMembership(ctx context.Context) error {
	return engine.runActive(ctx, "membership", engine.engine.MaintainMembership)
}

func (engine *gatedMaintenanceEngine) MaintainSource(ctx context.Context) error {
	return engine.runActive(ctx, "source", engine.engine.MaintainSource)
}

func (engine *gatedMaintenanceEngine) MaintainExecution(ctx context.Context) error {
	return engine.runActive(ctx, "execution", engine.engine.MaintainExecution)
}

func (engine *gatedMaintenanceEngine) runActive(
	ctx context.Context,
	name string,
	run func(context.Context) error,
) error {
	active, err := engine.leadership.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("inspect local generation leadership: %w", err)
	}
	if !active {
		return errors.New("runtime generation does not own the local gate")
	}
	if err := run(ctx); err != nil {
		return fmt.Errorf("run active generation %s: %w", name, err)
	}
	return nil
}

func (engine *gatedMaintenanceEngine) Close(ctx context.Context) error {
	return errors.Join(engine.engine.Close(ctx), engine.leadership.Close(ctx))
}

// maintenanceGeneration owns one prepared, restartable set of runtime
// resources. Activation commits after the local generation gate is acquired;
// maintenance begins immediately in the background.
type maintenanceGeneration struct {
	engine                   generationLifecycleEngine
	lanes                    []scheduledMaintenanceLane
	startupPrerequisite      string
	report                   func(error)
	currentTime              func() time.Time
	maintenanceLaneCompleted func(laneName string, ticker cycleTicker)
	closers                  []runtimeCloser
	finalizers               []runtimeFinalizer
	processor                *queuedJobProcessor
	notifications            queuedJobNotificationRuntime

	mutex         sync.Mutex
	closeMutex    sync.Mutex
	runCancel     context.CancelFunc
	runDone       chan struct{}
	prepared      bool
	active        bool
	tickerStopped bool
	engineClosed  bool
	closerClosed  []bool
	closed        bool
}

type generationLifecycleEngine interface {
	Activate() error
	Deactivate(ctx context.Context) error
	Close(ctx context.Context) error
}

type scheduledMaintenanceLane struct {
	name   string
	ticker cycleTicker
	run    func(context.Context) error
}

func newIndependentMaintenanceGeneration(
	engine generationLifecycleEngine,
	runtime maintenanceLaneRuntime,
	tickers maintenanceLaneTickers,
	report func(error),
	closers []runtimeCloser,
	finalizers []runtimeFinalizer,
) *maintenanceGeneration {
	return newScheduledMaintenanceGeneration(engine, []scheduledMaintenanceLane{
		{name: "membership", ticker: tickers.membership, run: runtime.MaintainMembership},
		{name: "source", ticker: tickers.source, run: runtime.MaintainSource},
		{name: "execution", ticker: tickers.execution, run: runtime.MaintainExecution},
	}, "membership", report, closers, finalizers)
}

func newScheduledMaintenanceGeneration(
	engine generationLifecycleEngine,
	lanes []scheduledMaintenanceLane,
	startupPrerequisite string,
	report func(error),
	closers []runtimeCloser,
	finalizers []runtimeFinalizer,
) *maintenanceGeneration {
	if report == nil {
		report = func(error) {}
	}
	return &maintenanceGeneration{
		engine: engine, lanes: append([]scheduledMaintenanceLane(nil), lanes...),
		startupPrerequisite: startupPrerequisite, report: report,
		currentTime: time.Now,
		closers:     append([]runtimeCloser(nil), closers...), finalizers: append([]runtimeFinalizer(nil), finalizers...),
		closerClosed: make([]bool, len(closers)),
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

func (generation *maintenanceGeneration) Activate(ctx context.Context) error {
	if err := generation.activationReadiness(); err != nil {
		return err
	}
	if err := generation.activateEngine(); err != nil {
		return err
	}
	if err := generation.startRunLoop(ctx); err != nil {
		return errors.Join(err, generation.engine.Deactivate(ctx))
	}
	reportProgress(generation.report, slog.LevelInfo, operationalEvent{
		operation: "generation", stage: "activation", outcome: "activated",
	})
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

func (generation *maintenanceGeneration) activateEngine() error {
	if err := generation.engine.Activate(); err != nil {
		return fmt.Errorf("activate generation engine: %w", err)
	}
	return nil
}

func (generation *maintenanceGeneration) startRunLoop(ctx context.Context) error {
	runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if generation.processor != nil {
		if err := generation.processor.Start(runContext); err != nil {
			cancel()
			return fmt.Errorf("start queued-job processor: %w", err)
		}
	}
	if generation.notifications != nil {
		if err := generation.notifications.Start(ctx, runContext); err != nil {
			cancel()
			return errors.Join(
				fmt.Errorf("start queued-job notifications: %w", err),
				generation.stopQueuedJobProcessor(context.WithoutCancel(ctx)),
			)
		}
	}
	generation.mutex.Lock()
	generation.runCancel = cancel
	generation.runDone = make(chan struct{})
	generation.active = true
	done := generation.runDone
	generation.mutex.Unlock()
	go generation.run(runContext, done)
	return nil
}

func (generation *maintenanceGeneration) stopQueuedJobProcessor(ctx context.Context) error {
	if generation.processor == nil {
		return nil
	}
	return generation.processor.Stop(ctx)
}

func (generation *maintenanceGeneration) run(ctx context.Context, done chan struct{}) {
	prerequisiteReady := make(chan struct{})
	var lanes sync.WaitGroup
	lanes.Add(len(generation.lanes))
	for _, lane := range generation.lanes {
		go func() {
			defer lanes.Done()
			if lane.name == generation.startupPrerequisite {
				generation.runPrerequisiteLane(ctx, lane, prerequisiteReady)
				return
			}
			generation.runLaneAfterPrerequisite(ctx, lane, prerequisiteReady)
		}()
	}
	lanes.Wait()
	close(done)
}

func (generation *maintenanceGeneration) runLane(ctx context.Context, lane scheduledMaintenanceLane) {
	generation.runCycle(ctx, lane, "immediate", slog.LevelInfo)
	generation.runScheduledLane(ctx, lane)
}

func (generation *maintenanceGeneration) runPrerequisiteLane(
	ctx context.Context,
	lane scheduledMaintenanceLane,
	ready chan<- struct{},
) {
	stage := "immediate"
	level := slog.LevelInfo
	for {
		if generation.runCycle(ctx, lane, stage, level) {
			close(ready)
			generation.runScheduledLane(ctx, lane)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-lane.ticker.Ticks():
			stage = "scheduled"
			level = slog.LevelDebug
		}
	}
}

func (generation *maintenanceGeneration) runLaneAfterPrerequisite(
	ctx context.Context,
	lane scheduledMaintenanceLane,
	ready <-chan struct{},
) {
	select {
	case <-ctx.Done():
		return
	case <-ready:
		generation.runLane(ctx, lane)
	}
}

func (generation *maintenanceGeneration) runScheduledLane(ctx context.Context, lane scheduledMaintenanceLane) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-lane.ticker.Ticks():
			generation.runCycle(ctx, lane, "scheduled", slog.LevelDebug)
		}
	}
}

func (generation *maintenanceGeneration) runCycle(
	ctx context.Context,
	lane scheduledMaintenanceLane,
	stage string,
	startLevel slog.Level,
) bool {
	started := generation.currentTime()
	reportProgress(generation.report, startLevel, operationalEvent{
		operation: "maintenance_" + lane.name, stage: stage, outcome: "started",
	})
	err := lane.run(ctx)
	if generation.maintenanceLaneCompleted != nil {
		generation.maintenanceLaneCompleted(lane.name, lane.ticker)
	}
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	reportProgress(generation.report, slog.LevelDebug, operationalEvent{
		operation: "maintenance_" + lane.name, stage: stage, outcome: outcome,
		duration: generation.currentTime().Sub(started),
	})
	generation.reportLane(lane.name, err)
	return err == nil
}

func (generation *maintenanceGeneration) reportLane(name string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	event := operationalEvent{operation: "maintenance_lane", stage: name, outcome: "recovered"}
	if err != nil {
		event.outcome = "failed"
		event.err = err
	}
	reportOperational(generation.report, event)
}

func (generation *maintenanceGeneration) Deactivate(ctx context.Context) error {
	cancel, done, active := generation.deactivationState()
	if !active {
		return nil
	}
	reportProgress(generation.report, slog.LevelInfo, operationalEvent{
		operation: "generation", stage: "deactivation", outcome: "started",
	})
	if err := generation.stopActiveRuntime(ctx, cancel, done); err != nil {
		return err
	}
	generation.mutex.Lock()
	generation.active = false
	generation.runCancel = nil
	generation.runDone = nil
	generation.mutex.Unlock()
	reportProgress(generation.report, slog.LevelInfo, operationalEvent{
		operation: "generation", stage: "deactivation", outcome: "completed",
	})
	return nil
}

func (generation *maintenanceGeneration) deactivationState() (context.CancelFunc, <-chan struct{}, bool) {
	generation.mutex.Lock()
	defer generation.mutex.Unlock()
	return generation.runCancel, generation.runDone, generation.active
}

func (generation *maintenanceGeneration) stopActiveRuntime(
	ctx context.Context,
	cancel context.CancelFunc,
	done <-chan struct{},
) error {
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("wait for runtime generation: %w", ctx.Err())
	}
	if generation.notifications != nil {
		if err := generation.notifications.Stop(ctx); err != nil {
			return fmt.Errorf("stop queued-job notifications: %w", err)
		}
	}
	if generation.processor != nil {
		if err := generation.processor.Stop(ctx); err != nil {
			return fmt.Errorf("stop queued-job processor: %w", err)
		}
	}
	if err := generation.engine.Deactivate(ctx); err != nil {
		return fmt.Errorf("deactivate generation engine: %w", err)
	}
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
	reportProgress(generation.report, slog.LevelInfo, operationalEvent{
		operation: "generation", stage: "shutdown", outcome: "started",
	})
	closeErr := errors.Join(generation.closeEngine(ctx), generation.closeResources(ctx))
	if closeErr == nil {
		generation.mutex.Lock()
		generation.closed = true
		generation.mutex.Unlock()
	}
	if closeErr == nil {
		reportProgress(generation.report, slog.LevelInfo, operationalEvent{
			operation: "generation", stage: "shutdown", outcome: "completed",
		})
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
		for _, lane := range generation.lanes {
			lane.ticker.Stop()
		}
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
