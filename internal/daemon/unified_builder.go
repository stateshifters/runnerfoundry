package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubsupervisor "github.com/stateshifters/runnerfoundry/internal/source/github/supervisor"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

type runtimeStatePorts struct {
	jobs                 statecontract.JobQueueStore
	processing           statecontract.ProcessingJobStore
	transitions          statecontract.ProcessingTransitions
	provisioningFailures statecontract.ProvisioningFailureTransitions
	members              statecontract.MemberRegistry
	capacity             statecontract.RunnerPoolCapacityStore
}

type unifiedBuilderDependencies struct {
	state              runtimeStatePorts
	demandStore        githubsource.AssignedDemandRepository
	manager            githubManagerComposition
	member             core.MemberID
	providers          map[core.RunnerPool]execution.RunnerLauncher
	inventory          execution.RunnerInventory
	leadership         leadershipRuntime
	currentTime        func() time.Time
	memberTTL          time.Duration
	tickers            maintenanceLaneTickers
	report             func(error)
	closers            []runtimeCloser
	processGate        *localLeadershipGate
	queueNotifications *queueNotificationPool
}

type githubManagerComposition interface {
	githubsupervisor.Manager
	Source() *githubsource.Source
}

func buildUnifiedGeneration(
	configured config.Config,
	dependencies unifiedBuilderDependencies,
) (*maintenanceGeneration, error) {
	if err := validateUnifiedDependencies(dependencies); err != nil {
		return nil, err
	}
	return buildValidatedUnifiedGeneration(configured, dependencies)
}

func buildValidatedUnifiedGeneration(
	configured config.Config,
	dependencies unifiedBuilderDependencies,
) (*maintenanceGeneration, error) {
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
	enqueueWake := configuredEnqueueWake(stateBackendName(configured.State), wakeJobs)
	engine := buildConfiguredEngine(
		assembled, dependencies, discoveryInterval, jobGate, wakeJobs, enqueueWake, stateBackendName(configured.State),
	)
	processor = newQueuedJobProcessorForPools(engine, jobGate, dependencies.report, configuredRunnerPoolNames(assembled.pools))
	processEngine := newGatedMaintenanceEngine(engine, dependencies.processGate.NewLeadership())
	generation := newIndependentMaintenanceGeneration(
		processEngine, processEngine, dependencies.tickers, dependencies.report,
		configuredClosers(dependencies), assembled.finalizers,
	)
	generation.processor = processor
	if notifications := configuredQueueNotifications(dependencies, wakeJobs); notifications != nil {
		generation.notifications = notifications
	}
	return generation, nil
}

func configuredRunnerPoolNames(pools []RunnerPoolRuntime) []core.RunnerPool {
	runnerPools := make([]core.RunnerPool, 0, len(pools))
	for _, pool := range pools {
		runnerPools = append(runnerPools, pool.RunnerPool)
	}
	return runnerPools
}

func configuredQueueNotifications(
	dependencies unifiedBuilderDependencies,
	wake func(core.RunnerPool),
) *postgreSQLQueueNotificationListener {
	if dependencies.queueNotifications == nil {
		return nil
	}
	listener := newPostgreSQLQueueNotificationListener(*dependencies.queueNotifications)
	listener.wake = wake
	listener.report = func(err error) { reportQueueNotificationStatus(dependencies.report, err) }
	return listener
}

func reportQueueNotificationStatus(report func(error), cause error) {
	event := operationalEvent{
		operation: "queue_notifications", stateBackend: "postgresql", stateOperation: "listen",
	}
	if cause == nil {
		event.outcome = "recovered"
		reportOperational(report, event)
		return
	}
	event.outcome = "failed"
	event.err = errors.New("PostgreSQL queue notifications unavailable")
	reportOperationalError(report, event, cause)
}

func buildConfiguredEngine(
	assembled assembledRuntime,
	dependencies unifiedBuilderDependencies,
	discoveryInterval time.Duration,
	jobGate *queuedJobEligibilityGate,
	wakeJobs func(core.RunnerPool),
	enqueueWake func(core.RunnerPool),
	stateBackend string,
) *UnifiedEngine {
	queue := runtimeengine.NewQueueHandlerWithWakeAndBackend(dependencies.state.jobs, enqueueWake, stateBackend)
	handler := newGitHubDemandHandler(queue, dependencies.demandStore, enqueueWake, stateBackend)
	capacity := runtimeengine.NewCapacitySnapshotService(
		dependencies.state.capacity, dependencies.currentTime, dependencies.memberTTL,
	)
	source := githubsupervisor.New(
		dependencies.manager, capacity, string(dependencies.member),
		handler,
		assembled.managerConfig, discoveryInterval, dependencies.currentTime,
		newGitHubManagerObserver(dependencies.report, dependencies.currentTime),
	)
	membership := runtimeengine.NewMembershipService(
		dependencies.state.members, dependencies.member, assembled.localCapacity, dependencies.currentTime,
	)
	return NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: dependencies.currentTime, MemberTTL: dependencies.memberTTL,
		Membership: membership, Leadership: dependencies.leadership, Source: source, Pools: assembled.pools,
		Inventory: dependencies.inventory, Report: dependencies.report,
		JobGate: jobGate, WakeJobs: wakeJobs,
	})
}

func configuredEnqueueWake(stateBackend string, wake func(core.RunnerPool)) func(core.RunnerPool) {
	if stateBackend == "postgresql" {
		return nil
	}
	return wake
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
	managerConfig    githubsource.ManagerConfig
	stateBackend     string
	localCapacity    []core.RunnerPoolCapacity
	pools            []RunnerPoolRuntime
	finalizers       []runtimeFinalizer
	sourceOperations core.SourceOperationsResolver
}

func assembleConfiguredRuntime(
	configured config.Config,
	dependencies unifiedBuilderDependencies,
) (assembledRuntime, error) {
	sourceOperations, err := newSourceOperationsResolver([]sourceOperationsRegistration{{
		adapter: githubsource.AdapterName, operations: dependencies.manager.Source(),
	}})
	if err != nil {
		return assembledRuntime{}, fmt.Errorf("register source operations: %w", err)
	}
	assembled := assembledRuntime{
		managerConfig:    githubsource.ManagerConfig{Prune: configured.Prune},
		stateBackend:     stateBackendName(configured.State),
		localCapacity:    make([]core.RunnerPoolCapacity, 0, len(configured.RunnerPools)),
		pools:            make([]RunnerPoolRuntime, 0, len(configured.RunnerPools)),
		finalizers:       make([]runtimeFinalizer, 0, len(configured.RunnerPools)),
		sourceOperations: sourceOperations,
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
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		dependencies.state.processing, dependencies.state.transitions, dependencies.state.provisioningFailures,
		assembled.sourceOperations, provider, dependencies.inventory, dependencies.member,
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
	provider execution.RunnerLauncher,
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
	if !hasCompleteRuntimeState(dependencies.state) ||
		dependencies.demandStore == nil || dependencies.manager == nil || dependencies.member == "" ||
		dependencies.leadership == nil || dependencies.currentTime == nil ||
		!hasCompleteMaintenanceLaneTickers(dependencies.tickers) ||
		dependencies.processGate == nil || dependencies.inventory == nil {
		return errors.New("state, GitHub manager, member, leadership, clock, ticker, and process gate are required")
	}
	if dependencies.memberTTL <= 0 {
		return errors.New("member TTL must be positive")
	}
	return nil
}

func hasCompleteMaintenanceLaneTickers(tickers maintenanceLaneTickers) bool {
	return tickers.membership != nil && tickers.source != nil && tickers.execution != nil
}

func hasCompleteRuntimeState(ports runtimeStatePorts) bool {
	return ports.jobs != nil && ports.processing != nil && ports.transitions != nil &&
		ports.provisioningFailures != nil && ports.members != nil && ports.capacity != nil
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

const (
	provisioningFailureReportingStage    = "report"
	provisioningFailureFinalizationStage = "finalize_provisioning_failure"
)

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
		reportRetryOutcome(report, base, result, err)
	}
	reportProvisioningFailureOutcome(report, base, result, err)
}

func reportRetryOutcome(
	report func(error),
	base operationalEvent,
	result runtimeengine.ProcessResult,
	err error,
) {
	event := retryOperationalEvent(base, result, err)
	var diagnostic interface{ OperationalDiagnostic() string }
	if errors.As(err, &diagnostic) {
		event.diagnostic = diagnostic.OperationalDiagnostic()
	}
	reportOperational(report, event)
}

func retryOperationalEvent(
	base operationalEvent,
	result runtimeengine.ProcessResult,
	err error,
) operationalEvent {
	event := base
	event.outcome = string(result.RetryOutcome)
	event.err = err
	if result.RetryOutcome == core.RetryFailureReportingPending {
		event.operation = "provisioning_failure"
		event.stage = provisioningFailureReportingStage
		return event
	}
	if result.RetryOutcome == core.RetryFailureFinalizationPending {
		event.operation = "job_processing"
		event.stage = provisioningFailureFinalizationStage
		return event
	}
	event.operation = "runner_retry"
	event.stage = result.FailedStage
	return event
}

func reportProvisioningFailureOutcome(
	report func(error),
	base operationalEvent,
	result runtimeengine.ProcessResult,
	err error,
) {
	if result.Outcome == runtimeengine.ProcessClaimed && err != nil {
		event := base
		event.operation = "job_processing"
		event.stage = result.FailedStage
		event.outcome = "failed"
		event.err = err
		reportOperational(report, event)
	}
	if result.Outcome == runtimeengine.ProcessProvisioningFailureReported ||
		result.RetryOutcome == core.RetryFailureFinalizationPending {
		event := base
		event.operation = "provisioning_failure"
		event.stage = provisioningFailureReportingStage
		event.outcome = "reported"
		reportOperational(report, event)
	}
}

func (pool orchestrationPool) Reconcile(ctx context.Context, runnerPool core.RunnerPool) error {
	result, err := pool.orchestrator.ReconcilePool(ctx, runnerPool)
	reportReconciliationOutcome(pool.report, runnerPool, pool.provider, pool.endpoint, result)
	if err != nil {
		return fmt.Errorf("reconcile pool work: %w", err)
	}
	return nil
}

func reportReconciliationOutcome(
	report func(error),
	runnerPool core.RunnerPool,
	provider string,
	endpoint string,
	result runtimeengine.PoolReconciliation,
) {
	for _, job := range result.ReportedProvisioningFailures {
		event := operationalEvent{
			operation:  "provisioning_failure",
			stage:      provisioningFailureReportingStage,
			outcome:    "reported",
			runnerPool: runnerPool,
			jobID:      job.ID,
			provider:   provider,
			endpoint:   endpoint,
			count:      1,
		}
		reportOperational(report, addGitHubJobContext(report, event, job))
		reportProvisioningFailureFinalizationRecovery(report, runnerPool, provider, endpoint, job)
	}
	for _, job := range result.FinalizedTerminalJobs {
		reportProvisioningFailureFinalizationRecovery(report, runnerPool, provider, endpoint, job)
	}
}

func reportProvisioningFailureFinalizationRecovery(
	report func(error),
	runnerPool core.RunnerPool,
	provider string,
	endpoint string,
	job core.Job,
) {
	event := operationalEvent{
		operation:  "job_processing",
		stage:      provisioningFailureFinalizationStage,
		outcome:    "recovered",
		runnerPool: runnerPool,
		jobID:      job.ID,
		provider:   provider,
		endpoint:   endpoint,
	}
	reportOperational(report, addGitHubJobContext(report, event, job))
}

func (pool orchestrationPool) RecoverForeign(ctx context.Context, runnerPool core.RunnerPool) error {
	result, err := pool.orchestrator.RecoverForeignPool(ctx, runnerPool)
	reportReconciliationOutcome(pool.report, runnerPool, pool.provider, pool.endpoint, result)
	if err != nil {
		return fmt.Errorf("recover foreign pool work: %w", err)
	}
	return nil
}

func (pool orchestrationPool) RunnerPool() core.RunnerPool { return pool.runnerPool }

func (pool orchestrationPool) Terminate(ctx context.Context) (int, error) {
	result, err := pool.orchestrator.TerminatePool(ctx, pool.runnerPool)
	reportReconciliationOutcome(pool.report, pool.runnerPool, pool.provider, pool.endpoint, result)
	if err != nil {
		return 0, fmt.Errorf("clean up Runner Pool %q: %w", pool.runnerPool, err)
	}
	return result.RemainingProcessing, nil
}
