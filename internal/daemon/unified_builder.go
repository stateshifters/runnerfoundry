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

type unifiedStore interface {
	core.JobQueueStore
	core.ProcessingJobStore
	runtimeengine.MembershipStore
	githubsource.RegistrationRepository
	statecontract.RunnerPoolCapacityStore
}

type unifiedBuilderDependencies struct {
	store       unifiedStore
	manager     *githubsource.Manager
	member      core.MemberID
	providers   map[core.RunnerPool]execution.ExecutionProvider
	inventory   execution.RunnerInventory
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
) (*maintenanceGeneration, error) {
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
	return newMaintenanceGeneration(
		processEngine, dependencies.ticker, dependencies.report, configuredClosers(dependencies),
		assembled.finalizers,
	), nil
}

func buildConfiguredEngine(
	assembled assembledRuntime,
	dependencies unifiedBuilderDependencies,
	discoveryInterval time.Duration,
) *UnifiedEngine {
	source := newGitHubSourceRuntime(
		dependencies.manager, dependencies.store, string(dependencies.member), runtimeengine.NewQueueHandler(dependencies.store),
		assembled.managerConfig, discoveryInterval, dependencies.currentTime,
	)
	membership := runtimeengine.NewMembershipService(
		dependencies.store, dependencies.member, assembled.localCapacity, dependencies.currentTime,
	)
	return NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: dependencies.currentTime, MemberTTL: dependencies.memberTTL,
		Membership: membership, Leadership: dependencies.leadership, Source: source, Pools: assembled.pools,
		Inventory: dependencies.inventory,
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
	finalizers    []runtimeFinalizer
}

func assembleConfiguredRuntime(
	configured config.Config,
	dependencies unifiedBuilderDependencies,
) (assembledRuntime, error) {
	assembled := assembledRuntime{
		managerConfig: githubsource.ManagerConfig{Prune: configured.Prune},
		localCapacity: make([]core.RunnerPoolCapacity, 0, len(configured.RunnerPools)),
		pools:         make([]RunnerPoolRuntime, 0, len(configured.RunnerPools)),
		finalizers:    make([]runtimeFinalizer, 0, len(configured.RunnerPools)),
	}
	for _, runnerPoolConfig := range configured.RunnerPools {
		if err := assembled.appendRunnerPool(runnerPoolConfig, dependencies); err != nil {
			return assembledRuntime{}, err
		}
	}
	if len(assembled.pools) == 0 {
		return assembledRuntime{}, errors.New("at least one Runner Pool is required")
	}
	return assembled, nil
}

func (assembled *assembledRuntime) appendRunnerPool(
	runnerPoolConfig config.RunnerPoolConfig,
	dependencies unifiedBuilderDependencies,
) error {
	runnerPool := core.RunnerPool(runnerPoolConfig.Name)
	provider := dependencies.providers[runnerPool]
	if provider == nil {
		return fmt.Errorf("execution provider for Runner Pool %q is required", runnerPool)
	}
	orchestrator := runtimeengine.NewOrchestrator(
		dependencies.store, dependencies.manager.Source(), provider, dependencies.inventory, dependencies.member,
	)
	assembled.localCapacity = append(assembled.localCapacity, core.RunnerPoolCapacity{
		RunnerPool: runnerPool, Capacity: runnerPoolConfig.MaxRunners,
	})
	assembled.managerConfig.RunnerPools = append(assembled.managerConfig.RunnerPools, githubsource.RunnerPoolConfig{
		RunnerPool: runnerPool, RunnerGroup: runnerPoolConfig.RunnerGroup,
	})
	pool := orchestrationPool{
		worker: runtimeengine.NewPoolWorker(orchestrator, runnerPool, runnerPoolConfig.MaxRunners), orchestrator: orchestrator,
		runnerPool: runnerPool,
	}
	assembled.pools = append(assembled.pools, RunnerPoolRuntime{
		RunnerPool: runnerPool,
		runtime:    pool,
	})
	assembled.finalizers = append(assembled.finalizers, pool)
	return nil
}

func validateUnifiedDependencies(dependencies unifiedBuilderDependencies) error {
	if dependencies.store == nil || dependencies.manager == nil || dependencies.member == "" ||
		dependencies.leadership == nil || dependencies.currentTime == nil || dependencies.ticker == nil ||
		dependencies.processGate == nil || dependencies.inventory == nil {
		return errors.New("state, GitHub manager, member, leadership, clock, ticker, and process gate are required")
	}
	if dependencies.memberTTL <= 0 {
		return errors.New("member TTL must be positive")
	}
	return nil
}

type orchestrationPool struct {
	worker       *runtimeengine.PoolWorker
	orchestrator *runtimeengine.Orchestrator
	runnerPool   core.RunnerPool
}

func (pool orchestrationPool) RequestWork(ctx context.Context, _ core.RunnerPool) error {
	_, err := pool.worker.RequestWork(ctx)
	if err != nil {
		return fmt.Errorf("request pool work: %w", err)
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

func (pool orchestrationPool) RunnerPool() core.RunnerPool { return pool.runnerPool }

func (pool orchestrationPool) Terminate(ctx context.Context) (int, error) {
	result, err := pool.orchestrator.TerminatePool(ctx, pool.runnerPool)
	if err != nil {
		return result.RemainingProcessing, fmt.Errorf("clean up Runner Pool %q: %w", pool.runnerPool, err)
	}
	return result.RemainingProcessing, nil
}
