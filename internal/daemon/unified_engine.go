package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
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
	RequestWork(ctx context.Context, runnerPool core.RunnerPool) error
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
	leaderClosing  bool
	cycleMutex     sync.Mutex
	report         func(error)
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
	}
}

// Cycle performs one serialized, bounded maintenance pass. Independent work
// continues after individual failures and all failures are returned together.
func (engine *UnifiedEngine) Cycle(ctx context.Context) error {
	engine.cycleMutex.Lock()
	defer engine.cycleMutex.Unlock()
	now := engine.currentTime()
	liveMemberCutoff := now.Add(-engine.memberTTL)
	cycleErrors := make([]error, 0)

	if err := engine.membership.Heartbeat(ctx); err != nil {
		cycleErrors = append(cycleErrors, fmt.Errorf("heartbeat member: %w", err))
	}
	if err := engine.source.RefreshMembers(ctx); err != nil {
		cycleErrors = append(cycleErrors, fmt.Errorf("refresh source member routes: %w", err))
	}

	leader, leaderErrors := engine.runLeaderWork(ctx, liveMemberCutoff)
	cycleErrors = append(cycleErrors, leaderErrors...)
	if err := engine.refreshInventory(ctx); err != nil {
		cycleErrors = append(cycleErrors, err)
		return errors.Join(cycleErrors...)
	}
	if leader && engine.inventory != nil {
		cycleErrors = append(cycleErrors, engine.recoverForeign(ctx)...)
	}
	for _, runnerPool := range engine.pools {
		cycleErrors = append(cycleErrors, engine.runPoolWork(ctx, runnerPool)...)
	}

	return errors.Join(cycleErrors...)
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
	if engine.leaderClosing {
		if closeErrors := engine.closeSourceLeadership(ctx, "finish closing source leadership"); len(closeErrors) != 0 {
			return true, closeErrors
		}
	}
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
	engine.leaderClosing = true
	return engine.closeSourceLeadership(ctx, "close lost source leadership")
}

func (engine *UnifiedEngine) closeSourceLeadership(ctx context.Context, description string) []error {
	if err := engine.source.CloseLeader(ctx); err != nil {
		return []error{fmt.Errorf("%s: %w", description, err)}
	}
	engine.leaderOpen = false
	engine.leaderClosing = false
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
	poolErrors := make([]error, 0, 1)
	if err := runnerPool.runtime.Reconcile(ctx, runnerPool.RunnerPool); err != nil {
		poolErrors = append(poolErrors, fmt.Errorf("reconcile Runner Pool %q: %w", runnerPool.RunnerPool, err))
		return poolErrors
	}
	if err := runnerPool.runtime.RequestWork(ctx, runnerPool.RunnerPool); err != nil {
		poolErrors = append(poolErrors, fmt.Errorf("request work for Runner Pool %q: %w", runnerPool.RunnerPool, err))
	}
	return poolErrors
}

// Close releases leader-owned source sessions. It is idempotent and does not
// close member-owned state or providers, whose lifetimes belong to generation.
func (engine *UnifiedEngine) Close(ctx context.Context) error {
	if !engine.leaderOpen {
		return nil
	}
	engine.leaderClosing = true
	if closeErrors := engine.closeSourceLeadership(ctx, "close source leadership"); len(closeErrors) != 0 {
		return errors.Join(closeErrors...)
	}
	return nil
}
