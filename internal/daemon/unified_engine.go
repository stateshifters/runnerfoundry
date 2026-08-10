package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

type membershipRuntime interface {
	Heartbeat(ctx context.Context) error
}

type leadershipRuntime interface {
	IsLeader(ctx context.Context) (bool, error)
}

type sourceRuntime interface {
	RefreshMembers(ctx context.Context) error
	OpenLeader(ctx context.Context) error
	Poll(ctx context.Context, liveMemberCutoff time.Time) error
	CloseLeader(ctx context.Context) error
}

type poolRuntime interface {
	RequestWork(ctx context.Context, runnerPool core.RunnerPool) (runtimeengine.ProcessResult, error)
	Reconcile(ctx context.Context, runnerPool core.RunnerPool) error
	RecoverForeign(ctx context.Context, runnerPool core.RunnerPool) error
}

type foreignRecovery func(context.Context, RunnerPoolRuntime) error

// RunnerPoolRuntime binds one configured Runner Pool to its member-local
// execution runtime. Every backend uses this same lifecycle.
type RunnerPoolRuntime struct {
	RunnerPool core.RunnerPool
	runtime    poolRuntime
}

// UnifiedEngineConfig contains the boundaries used by a member's maintenance
// lanes. Callers supply a deterministic clock and backend-specific state
// implementations; the engine never selects behavior from a database type.
type UnifiedEngineConfig struct {
	CurrentTime func() time.Time
	MemberTTL   time.Duration
	Membership  membershipRuntime
	Leadership  leadershipRuntime
	Source      sourceRuntime
	Pools       []RunnerPoolRuntime
	Inventory   execution.RunnerInventory
	Report      func(error)
	JobGate     *queuedJobEligibilityGate
	WakeJobs    func(core.RunnerPool)
}

// UnifiedEngine advances one source-neutral lifecycle for a member. Production
// schedules membership, source control, and execution independently. Source
// and execution maintenance each serialize their own mutable state.
type UnifiedEngine struct {
	currentTime    func() time.Time
	memberTTL      time.Duration
	membership     membershipRuntime
	leadership     leadershipRuntime
	source         sourceRuntime
	pools          []RunnerPoolRuntime
	inventory      execution.RunnerInventory
	leaderOpen     bool
	sourceMutex    sync.Mutex
	executionMutex sync.Mutex
	report         func(error)
	jobGate        *queuedJobEligibilityGate
	wakeJobs       func(core.RunnerPool)
	reportedLeader operationalLeadership
}

func NewUnifiedEngine(config UnifiedEngineConfig) *UnifiedEngine {
	return &UnifiedEngine{
		currentTime: config.CurrentTime,
		memberTTL:   config.MemberTTL,
		membership:  config.Membership,
		leadership:  config.Leadership,
		source:      config.Source,
		pools:       append([]RunnerPoolRuntime(nil), config.Pools...),
		inventory:   config.Inventory,
		report:      config.Report,
		jobGate:     config.JobGate,
		wakeJobs:    config.WakeJobs,
	}
}

// Cycle preserves the former test and diagnostic entry point. Production
// scheduling invokes the three maintenance lanes independently.
func (engine *UnifiedEngine) Cycle(ctx context.Context) error {
	membershipErr := engine.MaintainMembership(ctx)
	leader, sourceErr := engine.runSourceMaintenance(ctx)
	var foreignWork foreignRecovery
	if leader {
		foreignWork = engine.recoverForeignPool
	}
	return errors.Join(membershipErr, sourceErr, engine.runExecutionMaintenance(ctx, foreignWork))
}

func (engine *UnifiedEngine) MaintainMembership(ctx context.Context) error {
	started := engine.phaseStarted("heartbeat", "membership", "")
	err := engine.membership.Heartbeat(ctx)
	engine.phaseCompleted("heartbeat", "membership", "", started, err)
	if err != nil {
		return fmt.Errorf("heartbeat member: %w", err)
	}
	return nil
}

func (engine *UnifiedEngine) MaintainSource(ctx context.Context) error {
	_, err := engine.runSourceMaintenance(ctx)
	return err
}

func (engine *UnifiedEngine) runSourceMaintenance(ctx context.Context) (bool, error) {
	engine.sourceMutex.Lock()
	defer engine.sourceMutex.Unlock()
	var sourceErrors []error
	started := engine.phaseStarted("refresh_member_routes", "source", "")
	if err := engine.source.RefreshMembers(ctx); err != nil {
		engine.phaseCompleted("refresh_member_routes", "source", "", started, err)
		sourceErrors = append(sourceErrors, fmt.Errorf("refresh source member routes: %w", err))
	} else {
		engine.phaseCompleted("refresh_member_routes", "source", "", started, nil)
	}
	liveMemberCutoff := engine.currentTime().Add(-engine.memberTTL)
	started = engine.phaseStarted("leader_work", "source", "")
	leader, leaderErrors := engine.runLeaderWork(ctx, liveMemberCutoff)
	engine.phaseCompleted("leader_work", "source", "", started, errors.Join(leaderErrors...))
	sourceErrors = append(sourceErrors, leaderErrors...)
	return leader, errors.Join(sourceErrors...)
}

func (engine *UnifiedEngine) MaintainExecution(ctx context.Context) error {
	leader, err := engine.leadership.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("inspect source leadership for foreign recovery: %w", err)
	}
	var foreignWork foreignRecovery
	if leader {
		foreignWork = engine.recoverForeignPool
	}
	return engine.runExecutionMaintenance(ctx, foreignWork)
}

func (engine *UnifiedEngine) runExecutionMaintenance(ctx context.Context, foreignWork foreignRecovery) error {
	engine.executionMutex.Lock()
	defer engine.executionMutex.Unlock()
	return errors.Join(engine.runExecutionPhases(ctx, foreignWork)...)
}

func (engine *UnifiedEngine) runExecutionPhases(
	ctx context.Context,
	foreignWork foreignRecovery,
) []error {
	started := engine.phaseStarted("refresh_inventory", "inventory", "")
	refresh := func() error { return engine.refreshInventory(ctx) }
	var refreshErr error
	if engine.jobGate == nil {
		refreshErr = refresh()
	} else {
		refreshErr = engine.jobGate.RefreshInventory(refresh)
	}
	if refreshErr != nil {
		engine.phaseCompleted("refresh_inventory", "inventory", "", started, refreshErr)
		return []error{refreshErr}
	}
	engine.phaseCompleted("refresh_inventory", "inventory", "", started, nil)
	return engine.maintainRunnerPools(ctx, foreignWork)
}

func (engine *UnifiedEngine) maintainRunnerPools(ctx context.Context, foreignWork foreignRecovery) []error {
	poolErrors := make(chan error, len(engine.pools))
	var pools sync.WaitGroup
	pools.Add(len(engine.pools))
	for _, runnerPool := range engine.pools {
		go func() {
			defer pools.Done()
			if err := engine.maintainRunnerPool(ctx, runnerPool, foreignWork); err != nil {
				poolErrors <- err
			}
		}()
	}
	pools.Wait()
	close(poolErrors)
	failures := make([]error, 0, len(poolErrors))
	for err := range poolErrors {
		failures = append(failures, err)
	}
	return failures
}

func (engine *UnifiedEngine) maintainRunnerPool(
	ctx context.Context,
	runnerPool RunnerPoolRuntime,
	foreignWork foreignRecovery,
) error {
	work := func() (bool, error) {
		var foreignErr error
		if foreignWork != nil && engine.inventory != nil {
			foreignErr = foreignWork(ctx, runnerPool)
		}
		started := engine.phaseStarted("reconcile_runner_pool", "runner_pool", runnerPool.RunnerPool)
		reconcileErr := errors.Join(engine.runPoolWork(ctx, runnerPool)...)
		engine.phaseCompleted("reconcile_runner_pool", "runner_pool", runnerPool.RunnerPool, started, reconcileErr)
		if reconcileErr == nil && engine.wakeJobs != nil {
			engine.wakeJobs(runnerPool.RunnerPool)
		}
		return reconcileErr == nil, errors.Join(foreignErr, reconcileErr)
	}
	if engine.jobGate == nil {
		_, err := work()
		return err
	}
	return engine.jobGate.MaintainPool(runnerPool.RunnerPool, work)
}

func (engine *UnifiedEngine) recoverForeignPool(ctx context.Context, runnerPool RunnerPoolRuntime) error {
	started := engine.phaseStarted("recover_foreign", "recovery", runnerPool.RunnerPool)
	err := runnerPool.runtime.RecoverForeign(ctx, runnerPool.RunnerPool)
	if err != nil {
		err = fmt.Errorf("recover foreign Runner Pool %q: %w", runnerPool.RunnerPool, err)
	}
	engine.phaseCompleted("recover_foreign", "recovery", runnerPool.RunnerPool, started, err)
	return err
}

func (engine *UnifiedEngine) phaseStarted(operation, stage string, runnerPool core.RunnerPool) time.Time {
	started := engine.currentTime()
	reportProgress(engine.report, slog.LevelDebug, operationalEvent{
		operation: operation, stage: stage, outcome: "started", runnerPool: runnerPool,
	})
	return started
}

func (engine *UnifiedEngine) phaseCompleted(operation, stage string, runnerPool core.RunnerPool, started time.Time, err error) {
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	reportProgress(engine.report, slog.LevelDebug, operationalEvent{
		operation: operation, stage: stage, outcome: outcome, runnerPool: runnerPool,
		duration: engine.currentTime().Sub(started),
	})
	if err != nil {
		if operationalErrorWasReported(err) {
			return
		}
		reportOperational(engine.report, operationalEvent{
			operation: operation, stage: stage, outcome: "failed", runnerPool: runnerPool,
			duration: engine.currentTime().Sub(started), err: err,
		})
		return
	}
	reportOperational(engine.report, operationalEvent{
		operation: operation, stage: stage, outcome: "recovered", runnerPool: runnerPool,
	})
}

func (engine *UnifiedEngine) refreshInventory(ctx context.Context) error {
	if engine.inventory == nil {
		return nil
	}
	if err := engine.inventory.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh runner inventory: %w", err)
	}
	return nil
}

func (engine *UnifiedEngine) runLeaderWork(ctx context.Context, liveMemberCutoff time.Time) (bool, []error) {
	leader, err := engine.leadership.IsLeader(ctx)
	if err != nil {
		return false, []error{fmt.Errorf("inspect source leadership: %w", err)}
	}
	if !leader {
		engine.reportLeadership(leadershipStandby)
		return false, engine.closeLostLeadership(ctx)
	}
	engine.reportLeadership(leadershipLeader)
	if err = engine.openLeadership(ctx); err != nil {
		return true, []error{err}
	}
	return true, engine.runOwnedLeaderWork(ctx, liveMemberCutoff)
}

func (engine *UnifiedEngine) reportLeadership(leadership operationalLeadership) {
	if engine.reportedLeader == leadership {
		return
	}
	engine.reportedLeader = leadership
	outcome := "standby"
	if leadership == leadershipLeader {
		outcome = "acquired"
	}
	reportOperational(engine.report, operationalEvent{operation: "leadership", outcome: outcome, leadership: leadership})
}

func (engine *UnifiedEngine) closeLostLeadership(ctx context.Context) []error {
	if !engine.leaderOpen {
		return nil
	}
	return engine.closeSourceLeadership(ctx, "close lost source leadership")
}

func (engine *UnifiedEngine) closeSourceLeadership(ctx context.Context, description string) []error {
	// Leadership loss is a hard fence. Cleanup failure must not make the old
	// term active again or prevent a later term from opening.
	engine.leaderOpen = false
	if err := engine.source.CloseLeader(ctx); err != nil {
		return []error{fmt.Errorf("%s: %w", description, err)}
	}
	return nil
}

func (engine *UnifiedEngine) openLeadership(ctx context.Context) error {
	if engine.leaderOpen {
		return nil
	}
	if err := engine.source.OpenLeader(ctx); err != nil {
		return fmt.Errorf("open source leadership: %w", err)
	}
	engine.leaderOpen = true
	return nil
}

func (engine *UnifiedEngine) runOwnedLeaderWork(ctx context.Context, liveMemberCutoff time.Time) []error {
	leaderErrors := make([]error, 0, 1)
	if err := engine.source.Poll(ctx, liveMemberCutoff); err != nil {
		leaderErrors = append(leaderErrors, fmt.Errorf("poll source requests: %w", err))
	}
	return leaderErrors
}

func (engine *UnifiedEngine) runPoolWork(ctx context.Context, runnerPool RunnerPoolRuntime) []error {
	if err := runnerPool.runtime.Reconcile(ctx, runnerPool.RunnerPool); err != nil {
		return []error{fmt.Errorf("reconcile Runner Pool %q: %w", runnerPool.RunnerPool, err)}
	}
	return nil
}

func (engine *UnifiedEngine) RequestWork(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (runtimeengine.ProcessResult, error) {
	for _, configuredPool := range engine.pools {
		if configuredPool.RunnerPool == runnerPool {
			result, err := configuredPool.runtime.RequestWork(ctx, runnerPool)
			if err != nil {
				return result, fmt.Errorf("request work from Runner Pool %q: %w", runnerPool, err)
			}
			return result, nil
		}
	}
	return runtimeengine.ProcessResult{}, fmt.Errorf("configured Runner Pool %q was not found", runnerPool)
}

// Close releases leader-owned source sessions. It is idempotent and does not
// close member-owned state or providers, whose lifetimes belong to generation.
func (engine *UnifiedEngine) Close(ctx context.Context) error {
	if !engine.leaderOpen {
		return nil
	}
	if closeErrors := engine.closeSourceLeadership(ctx, "close source leadership"); len(closeErrors) != 0 {
		return errors.Join(closeErrors...)
	}
	return nil
}
