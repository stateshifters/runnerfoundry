package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

const maintenanceBatchSize = 100

type unifiedStore interface {
	core.JobStore
	runtimeengine.MembershipStore
	statecontract.RegistrationStore
	statecontract.RunnerPoolCapacityStore
}

type unifiedBuilderDependencies struct {
	store       unifiedStore
	manager     *githubsource.Manager
	member      core.MemberID
	providers   map[core.RunnerPool]execution.ExecutionProvider
	leadership  leadershipRuntime
	currentTime func() time.Time
	memberTTL   time.Duration
	ticker      cycleTicker
	report      func(error)
	closers     []runtimeCloser
	processGate *localLeadershipGate
}

func buildUnifiedGeneration(
	configured config.Config,
	dependencies unifiedBuilderDependencies,
) (*unifiedGeneration, error) {
	if err := validateUnifiedDependencies(dependencies); err != nil {
		return nil, err
	}
	discoveryInterval, err := configured.GitHub.DiscoveryDuration()
	if err != nil {
		return nil, fmt.Errorf("read GitHub discovery interval: %w", err)
	}
	assembled, err := assembleConfiguredRuntime(configured, dependencies)
	if err != nil {
		return nil, err
	}
	engine := buildConfiguredEngine(assembled, dependencies, discoveryInterval)
	processEngine := newGatedCycleEngine(engine, dependencies.processGate.NewLeadership())
	return newUnifiedGeneration(
		processEngine, dependencies.ticker, dependencies.report, configuredClosers(dependencies),
	), nil
}

func buildConfiguredEngine(
	assembled assembledRuntime,
	dependencies unifiedBuilderDependencies,
	discoveryInterval time.Duration,
) *UnifiedEngine {
	lifecycle := orchestrationLifecycle{orchestrator: assembled.lifecycle, batchSize: maintenanceBatchSize}
	source := newGitHubSourceRuntime(
		dependencies.manager, dependencies.store, dependencies.member.String(), assembled.lifecycle,
		assembled.managerConfig, discoveryInterval, dependencies.currentTime,
	)
	membership := runtimeengine.NewMembershipService(
		dependencies.store, assembled.lifecycle, dependencies.member, assembled.localCapacity, dependencies.currentTime,
	)
	return NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: dependencies.currentTime, MemberTTL: dependencies.memberTTL,
		Membership: membership, Leadership: dependencies.leadership, Source: source,
		Lifecycle: lifecycle, Pools: assembled.pools,
	})
}

func configuredClosers(dependencies unifiedBuilderDependencies) []runtimeCloser {
	closers := append([]runtimeCloser(nil), dependencies.closers...)
	if leadershipCloser, ok := dependencies.leadership.(runtimeCloser); ok {
		closers = append(closers, leadershipCloser)
	}
	return closers
}

type assembledRuntime struct {
	managerConfig githubsource.ManagerConfig
	localCapacity []core.RunnerPoolCapacity
	pools         []RunnerPoolRuntime
	lifecycle     *runtimeengine.Orchestrator
}

func assembleConfiguredRuntime(
	configured config.Config,
	dependencies unifiedBuilderDependencies,
) (assembledRuntime, error) {
	assembled := assembledRuntime{
		managerConfig: githubsource.ManagerConfig{Prune: configured.Prune},
		localCapacity: make([]core.RunnerPoolCapacity, 0, len(configured.ScaleSets)),
		pools:         make([]RunnerPoolRuntime, 0, len(configured.ScaleSets)),
	}
	for _, scaleSet := range configured.ScaleSets {
		if err := assembled.appendScaleSet(scaleSet, dependencies); err != nil {
			return assembledRuntime{}, err
		}
	}
	if assembled.lifecycle == nil {
		return assembledRuntime{}, errors.New("at least one Runner Pool is required")
	}
	return assembled, nil
}

func (assembled *assembledRuntime) appendScaleSet(
	scaleSet config.ScaleSetConfig,
	dependencies unifiedBuilderDependencies,
) error {
	runnerPool := core.RunnerPool(scaleSet.Name)
	provider := dependencies.providers[runnerPool]
	if provider == nil {
		return fmt.Errorf("execution provider for Runner Pool %q is required", runnerPool)
	}
	jobStartTimeout, err := scaleSet.JobStartDuration()
	if err != nil {
		return fmt.Errorf("runner pool %q job start timeout: %w", runnerPool, err)
	}
	orchestrator := runtimeengine.NewOrchestrator(
		dependencies.store, dependencies.manager.Source(), provider, dependencies.member, dependencies.currentTime,
	)
	if assembled.lifecycle == nil {
		assembled.lifecycle = orchestrator
	}
	assembled.localCapacity = append(assembled.localCapacity, core.RunnerPoolCapacity{
		RunnerPool: runnerPool, Capacity: scaleSet.MaxRunners,
	})
	assembled.managerConfig.RunnerPools = append(assembled.managerConfig.RunnerPools, githubsource.RunnerPoolConfig{
		RunnerPool: runnerPool, RunnerGroup: scaleSet.RunnerGroup,
	})
	assembled.pools = append(assembled.pools, RunnerPoolRuntime{
		RunnerPool: runnerPool, JobStartTimeout: jobStartTimeout,
		runtime: orchestrationPool{
			worker: runtimeengine.NewPoolWorker(orchestrator, runnerPool, scaleSet.MaxRunners), orchestrator: orchestrator,
		},
	})
	return nil
}

func validateUnifiedDependencies(dependencies unifiedBuilderDependencies) error {
	if dependencies.store == nil || dependencies.manager == nil || dependencies.member == (core.MemberID{}) ||
		dependencies.leadership == nil || dependencies.currentTime == nil || dependencies.ticker == nil ||
		dependencies.processGate == nil {
		return errors.New("state, GitHub manager, member, leadership, clock, ticker, and process gate are required")
	}
	if dependencies.memberTTL <= 0 {
		return errors.New("member TTL must be positive")
	}
	return nil
}

type orchestrationLifecycle struct {
	orchestrator *runtimeengine.Orchestrator
	batchSize    int
}

func (lifecycle orchestrationLifecycle) RecoverInterruptedAcquisitions(ctx context.Context) error {
	_, err := lifecycle.orchestrator.RecoverInterruptedAcquisitions(ctx, lifecycle.batchSize)
	if err != nil {
		return fmt.Errorf("recover interrupted acquisitions: %w", err)
	}
	return nil
}

func (lifecycle orchestrationLifecycle) RetryPendingResults(ctx context.Context) error {
	_, err := lifecycle.orchestrator.RetryPendingResults(ctx, lifecycle.batchSize)
	if err != nil {
		return fmt.Errorf("retry pending results: %w", err)
	}
	return nil
}

type orchestrationPool struct {
	worker       *runtimeengine.PoolWorker
	orchestrator *runtimeengine.Orchestrator
}

func (pool orchestrationPool) RequestWork(ctx context.Context, _ core.RunnerPool) error {
	_, err := pool.worker.RequestWork(ctx)
	if err != nil {
		return fmt.Errorf("request pool work: %w", err)
	}
	return nil
}

func (pool orchestrationPool) StartAcquired(ctx context.Context, runnerPool core.RunnerPool) error {
	_, err := pool.orchestrator.StartAcquired(ctx, runnerPool)
	if err != nil {
		return fmt.Errorf("start acquired pool work: %w", err)
	}
	return nil
}

func (pool orchestrationPool) FailUnstarted(
	ctx context.Context,
	runnerPool core.RunnerPool,
	timeout time.Duration,
) error {
	_, err := pool.orchestrator.FailUnstartedAttempts(ctx, runnerPool, timeout)
	if err != nil {
		return fmt.Errorf("fail unstarted pool work: %w", err)
	}
	return nil
}

func (pool orchestrationPool) Reconcile(ctx context.Context, runnerPool core.RunnerPool) error {
	_, err := pool.orchestrator.ReconcilePool(ctx, runnerPool)
	if err != nil {
		return fmt.Errorf("reconcile pool work: %w", err)
	}
	return nil
}
