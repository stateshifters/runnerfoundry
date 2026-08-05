package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

type membershipRuntime interface {
	Heartbeat(ctx context.Context) error
	Expire(ctx context.Context, cutoff time.Time) (int, error)
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
}

// UnifiedEngine advances one source-neutral lifecycle for a member. Cycles are
// sequential; callers own scheduling and must not call Cycle concurrently.
type UnifiedEngine struct {
	currentTime   func() time.Time
	memberTTL     time.Duration
	membership    membershipRuntime
	leadership    leadershipRuntime
	source        sourceRuntime
	pools         []RunnerPoolRuntime
	leaderOpen    bool
	leaderClosing bool
}

func NewUnifiedEngine(config UnifiedEngineConfig) *UnifiedEngine {
	return &UnifiedEngine{
		currentTime: config.CurrentTime,
		memberTTL:   config.MemberTTL,
		membership:  config.Membership,
		leadership:  config.Leadership,
		source:      config.Source,
		pools:       append([]RunnerPoolRuntime(nil), config.Pools...),
	}
}

// Cycle performs one bounded maintenance pass. Independent work continues
// after individual failures and all failures are returned together.
func (engine *UnifiedEngine) Cycle(ctx context.Context) error {
	now := engine.currentTime()
	liveMemberCutoff := now.Add(-engine.memberTTL)
	cycleErrors := make([]error, 0)

	if err := engine.membership.Heartbeat(ctx); err != nil {
		cycleErrors = append(cycleErrors, fmt.Errorf("heartbeat member: %w", err))
	}
	if _, err := engine.membership.Expire(ctx, liveMemberCutoff); err != nil {
		cycleErrors = append(cycleErrors, fmt.Errorf("expire members: %w", err))
	}
	if err := engine.source.RefreshMembers(ctx); err != nil {
		cycleErrors = append(cycleErrors, fmt.Errorf("refresh source member routes: %w", err))
	}

	cycleErrors = append(cycleErrors, engine.runLeaderWork(ctx, liveMemberCutoff)...)
	for _, runnerPool := range engine.pools {
		cycleErrors = append(cycleErrors, engine.runPoolWork(ctx, runnerPool)...)
	}

	return errors.Join(cycleErrors...)
}

func (engine *UnifiedEngine) runLeaderWork(ctx context.Context, liveMemberCutoff time.Time) []error {
	leader, err := engine.leadership.IsLeader(ctx)
	if err != nil {
		return []error{fmt.Errorf("inspect source leadership: %w", err)}
	}
	if !leader {
		return engine.closeLostLeadership(ctx)
	}
	if engine.leaderClosing {
		if closeErrors := engine.closeSourceLeadership(ctx, "finish closing source leadership"); len(closeErrors) != 0 {
			return closeErrors
		}
	}
	if err = engine.openLeadership(ctx); err != nil {
		return []error{err}
	}
	return engine.runOwnedLeaderWork(ctx, liveMemberCutoff)
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
