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
	demandStore githubsource.AssignedDemandRepository
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
	jobGate := newQueuedJobEligibilityGate()
	var processor *queuedJobProcessor
	wakeJobs := func(runnerPool core.RunnerPool) {
		if processor != nil {
			processor.Wake(runnerPool)
		}
	}
	engine := buildConfiguredEngine(
		assembled, dependencies, discoveryInterval, jobGate, wakeJobs, stateBackendName(configured.State),
	)
	processor = newQueuedJobProcessor(engine, jobGate, dependencies.report)
	processEngine := newGatedCycleEngine(engine, dependencies.processGate.NewLeadership())
	generation := newMaintenanceGeneration(
		processEngine, dependencies.ticker, dependencies.report, configuredClosers(dependencies),
		assembled.finalizers,
	)
	generation.processor = processor
	return generation, nil
}

func buildConfiguredEngine(
	assembled assembledRuntime,
	dependencies unifiedBuilderDependencies,
	discoveryInterval time.Duration,
	jobGate *queuedJobEligibilityGate,
	wakeJobs func(core.RunnerPool),
	stateBackend string,
) *UnifiedEngine {
	queue := runtimeengine.NewQueueHandlerWithWakeAndBackend(dependencies.store, wakeJobs, stateBackend)
	handler := newGitHubDemandHandler(queue, dependencies.demandStore, wakeJobs, stateBackend)
	source := newGitHubSourceRuntime(
		dependencies.manager, dependencies.store, string(dependencies.member),
		handler,
		assembled.managerConfig, discoveryInterval, dependencies.currentTime,
	)
	source.report = dependencies.report
	membership := runtimeengine.NewMembershipService(
		dependencies.store, dependencies.member, assembled.localCapacity, dependencies.currentTime,
	)
	return NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: dependencies.currentTime, MemberTTL: dependencies.memberTTL,
		Membership: membership, Leadership: dependencies.leadership, Source: source, Pools: assembled.pools,
		Inventory: dependencies.inventory, Report: dependencies.report,
		JobGate: jobGate, WakeJobs: wakeJobs,
	})
}

func stateBackendName(configured config.StateConfig) string {
	if configured.Postgres != nil {
		return "postgresql"
	}
	return "sqlite"
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
	stateBackend  string
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
		stateBackend:  stateBackendName(configured.State),
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
	providerKind, err := configuredPoolProviderKind(runnerPoolConfig)
	if err != nil {
		return err
	}
	provider := dependencies.providers[runnerPool]
	if provider == nil {
		return fmt.Errorf("execution provider for Runner Pool %q is required", runnerPool)
	}
	orchestrator := runtimeengine.NewOrchestrator(
		dependencies.store, dependencies.manager.Source(), provider, dependencies.inventory, dependencies.member,
		dependencies.currentTime,
	)
	assembled.appendPoolConfiguration(runnerPoolConfig)
	pool := newOrchestrationPool(orchestrator, provider, providerKind, runnerPoolConfig, assembled.stateBackend,
		dependencies.report)
	assembled.pools = append(assembled.pools, RunnerPoolRuntime{RunnerPool: runnerPool, runtime: pool})
	assembled.finalizers = append(assembled.finalizers, pool)
	return nil
}

func configuredPoolProviderKind(configured config.RunnerPoolConfig) (config.ProviderKind, error) {
	runnerPool := core.RunnerPool(configured.Name)
	kind, err := configured.ProviderKind()
	if err != nil {
		return "", fmt.Errorf("identify execution provider for Runner Pool %q: %w", runnerPool, err)
	}
	return kind, nil
}

func (assembled *assembledRuntime) appendPoolConfiguration(configured config.RunnerPoolConfig) {
	runnerPool := core.RunnerPool(configured.Name)
	assembled.localCapacity = append(assembled.localCapacity,
		core.RunnerPoolCapacity{RunnerPool: runnerPool, Capacity: configured.MaxRunners})
	assembled.managerConfig.RunnerPools = append(assembled.managerConfig.RunnerPools,
		githubsource.RunnerPoolConfig{RunnerPool: runnerPool, RunnerGroup: configured.RunnerGroup})
}

func newOrchestrationPool(
	orchestrator *runtimeengine.Orchestrator,
	provider execution.ExecutionProvider,
	providerKind config.ProviderKind,
	configured config.RunnerPoolConfig,
	stateBackend string,
	report func(error),
) orchestrationPool {
	runnerPool := core.RunnerPool(configured.Name)
	return orchestrationPool{
		worker: runtimeengine.NewPoolWorker(orchestrator, runnerPool, configured.MaxRunners), orchestrator: orchestrator,
		runnerPool: runnerPool, provider: string(providerKind), endpoint: string(provider.EndpointID()),
		stateBackend: stateBackend, report: report,
	}
}

func validateUnifiedDependencies(dependencies unifiedBuilderDependencies) error {
	if dependencies.store == nil || dependencies.demandStore == nil || dependencies.manager == nil || dependencies.member == "" ||
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
	provider     string
	endpoint     string
	stateBackend string
	report       func(error)
}

func (pool orchestrationPool) RequestWork(
	ctx context.Context,
	_ core.RunnerPool,
) (runtimeengine.ProcessResult, error) {
	result, err := pool.worker.RequestWork(ctx)
	reportProcessOutcome(pool.report, pool.runnerPool, pool.provider, pool.endpoint, pool.stateBackend, result, err)
	if err != nil {
		return result, markOperationallyReported(fmt.Errorf("request pool work: %w", err))
	}
	return result, nil
}

func reportProcessOutcome(
	report func(error),
	runnerPool core.RunnerPool,
	provider string,
	endpoint string,
	stateBackend string,
	result runtimeengine.ProcessResult,
	err error,
) {
	job := processResultJob(result, runnerPool)
	base := operationalEvent{runnerPool: runnerPool, jobID: job.ID, provider: provider, endpoint: endpoint}
	if err != nil && job.ID == (core.JobID{}) {
		reportClaimFailure(report, base, stateBackend, err)
		return
	}
	base = addGitHubJobContext(report, base, job)
	reportProcessEvents(report, base, result, err)
}

func processResultJob(result runtimeengine.ProcessResult, runnerPool core.RunnerPool) core.Job {
	job := result.Job
	if job.ID == (core.JobID{}) {
		job.ID = result.JobID
		job.RunnerPool = runnerPool
	}
	return job
}

func reportClaimFailure(report func(error), base operationalEvent, stateBackend string, err error) {
	failure := base
	failure.operation = "claim"
	failure.stage = "claim_oldest"
	failure.outcome = "failed"
	failure.stateBackend = stateBackend
	failure.stateOperation = "claim_oldest"
	failure.err = err
	reportOperational(report, failure)
}

func reportProcessEvents(
	report func(error),
	base operationalEvent,
	result runtimeengine.ProcessResult,
	err error,
) {
	if base.jobID != (core.JobID{}) {
		claimed := base
		claimed.operation = "claim"
		claimed.outcome = "claimed"
		reportOperational(report, claimed)
	}
	if result.Outcome == runtimeengine.ProcessRunnerStarted {
		started := base
		started.operation = "runner_start"
		started.stage = "start_runner"
		started.outcome = "started"
		reportOperational(report, started)
	}
	if result.Outcome == runtimeengine.ProcessRetry {
		event := base
		event.operation = "runner_retry"
		event.stage = result.FailedStage
		event.outcome = string(result.RetryOutcome)
		event.err = err
		reportOperational(report, event)
	}
}

func (pool orchestrationPool) Reconcile(ctx context.Context, runnerPool core.RunnerPool) error {
	_, err := pool.orchestrator.ReconcilePool(ctx, runnerPool)
	if err != nil {
		return fmt.Errorf("reconcile pool work: %w", err)
	}
	return nil
}

func (pool orchestrationPool) RecoverForeign(ctx context.Context, runnerPool core.RunnerPool) error {
	_, err := pool.orchestrator.RecoverForeignPool(ctx, runnerPool)
	if err != nil {
		return fmt.Errorf("recover foreign pool work: %w", err)
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
