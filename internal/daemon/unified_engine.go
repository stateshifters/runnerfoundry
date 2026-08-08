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

// RunnerPoolRuntime binds one configured Runner Pool to its member-local
// execution runtime. Every backend uses this same lifecycle.
type RunnerPoolRuntime struct {
	RunnerPool core.RunnerPool
	runtime    poolRuntime
}

// UnifiedEngineConfig contains the boundaries used by a member's maintenance
// cycle. Callers supply a deterministic clock and backend-specific state
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

// UnifiedEngine advances one source-neutral lifecycle for a member. Callers own
// scheduling; concurrent Cycle calls are accepted and serialized.
type UnifiedEngine struct {
	currentTime    func() time.Time
	memberTTL      time.Duration
	membership     membershipRuntime
	leadership     leadershipRuntime
	source         sourceRuntime
	pools          []RunnerPoolRuntime
	inventory      execution.RunnerInventory
	leaderOpen     bool
	cycleMutex     sync.Mutex
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

// Cycle performs one serialized, bounded maintenance pass. Independent work
// continues after individual failures and all failures are returned together.
func (engine *UnifiedEngine) Cycle(ctx context.Context) error {
	engine.cycleMutex.Lock()
	defer engine.cycleMutex.Unlock()
	now := engine.currentTime()
	liveMemberCutoff := now.Add(-engine.memberTTL)
	cycleErrors := engine.runMemberPhases(ctx)
	started := engine.phaseStarted("leader_work", "source", "")
	leader, leaderErrors := engine.runLeaderWork(ctx, liveMemberCutoff)
	engine.phaseCompleted("leader_work", "source", "", started, errors.Join(leaderErrors...))
	cycleErrors = append(cycleErrors, leaderErrors...)
	var recoverForeign func(context.Context) []error
	if leader {
		recoverForeign = engine.recoverForeign
	}
	cycleErrors = append(cycleErrors, engine.runExecutionPhases(ctx, recoverForeign)...)
	return errors.Join(cycleErrors...)
}

func (engine *UnifiedEngine) runMemberPhases(ctx context.Context) []error {
	cycleErrors := make([]error, 0, 2)
	started := engine.phaseStarted("heartbeat", "membership", "")
	if err := engine.membership.Heartbeat(ctx); err != nil {
		engine.phaseCompleted("heartbeat", "membership", "", started, err)
		cycleErrors = append(cycleErrors, fmt.Errorf("heartbeat member: %w", err))
	} else {
		engine.phaseCompleted("heartbeat", "membership", "", started, nil)
	}
	started = engine.phaseStarted("refresh_member_routes", "source", "")
	if err := engine.source.RefreshMembers(ctx); err != nil {
		engine.phaseCompleted("refresh_member_routes", "source", "", started, err)
		cycleErrors = append(cycleErrors, fmt.Errorf("refresh source member routes: %w", err))
	} else {
		engine.phaseCompleted("refresh_member_routes", "source", "", started, nil)
	}
	return cycleErrors
}

func (engine *UnifiedEngine) runExecutionPhases(
	ctx context.Context,
	recoverForeign func(context.Context) []error,
) []error {
	if engine.jobGate == nil {
		return engine.runEligibleExecutionPhases(ctx, recoverForeign, nil)
	}
	eligibleRunnerPools := make([]core.RunnerPool, 0, len(engine.pools))
	var cycleErrors []error
	engine.jobGate.Maintain(func(maintenance *queuedJobMaintenance) {
		cycleErrors = engine.runEligibleExecutionPhases(ctx, recoverForeign, func(runnerPool core.RunnerPool) {
			maintenance.MarkEligible(runnerPool)
			eligibleRunnerPools = append(eligibleRunnerPools, runnerPool)
		})
	})
	if engine.wakeJobs != nil {
		for _, runnerPool := range eligibleRunnerPools {
			engine.wakeJobs(runnerPool)
		}
	}
	return cycleErrors
}

func (engine *UnifiedEngine) runEligibleExecutionPhases(
	ctx context.Context,
	recoverForeign func(context.Context) []error,
	markEligible func(core.RunnerPool),
) []error {
	cycleErrors := make([]error, 0)
	started := engine.phaseStarted("refresh_inventory", "inventory", "")
	if err := engine.refreshInventory(ctx); err != nil {
		engine.phaseCompleted("refresh_inventory", "inventory", "", started, err)
		cycleErrors = append(cycleErrors, err)
		return cycleErrors
	}
	engine.phaseCompleted("refresh_inventory", "inventory", "", started, nil)
	if recoverForeign != nil && engine.inventory != nil {
		started = engine.phaseStarted("recover_foreign", "recovery", "")
		foreignErrors := recoverForeign(ctx)
		engine.phaseCompleted("recover_foreign", "recovery", "", started, errors.Join(foreignErrors...))
		cycleErrors = append(cycleErrors, foreignErrors...)
	}
	for _, runnerPool := range engine.pools {
		started = engine.phaseStarted("reconcile_runner_pool", "runner_pool", runnerPool.RunnerPool)
		poolErrors := engine.runPoolWork(ctx, runnerPool)
		engine.phaseCompleted("reconcile_runner_pool", "runner_pool", runnerPool.RunnerPool, started, errors.Join(poolErrors...))
		cycleErrors = append(cycleErrors, poolErrors...)
		if len(poolErrors) == 0 && markEligible != nil {
			markEligible(runnerPool.RunnerPool)
		}
	}

	return cycleErrors
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

func (engine *UnifiedEngine) recoverForeign(ctx context.Context) []error {
	failures := make([]error, 0)
	for _, runnerPool := range engine.pools {
		if err := runnerPool.runtime.RecoverForeign(ctx, runnerPool.RunnerPool); err != nil {
			failures = append(failures, fmt.Errorf(
				"recover foreign Runner Pool %q: %w", runnerPool.RunnerPool, err,
			))
		}
	}
	return failures
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
