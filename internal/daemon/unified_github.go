package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

const githubRepositorySessionConcurrency = 4

type githubManagerRuntime interface {
	RegistrationSnapshot(ctx context.Context) (githubsource.RegistrationSnapshot, error)
	Reconcile(ctx context.Context, config githubsource.ManagerConfig) (githubsource.RegistrationSnapshot, error)
	RefreshMembers(ctx context.Context) error
	OpenRepositorySessions(ctx context.Context, records []githubsource.Registration, owner string,
		handler githubsource.EventHandler) (*githubsource.RepositorySessionGeneration, error)
}

type capacityAggregator interface {
	AvailableRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error)
}

type githubRepositoryOpenResult struct {
	epoch      uint64
	repository string
	records    []githubsource.Registration
	generation *githubsource.RepositorySessionGeneration
	err        error
}

type githubDiscoveryResult struct {
	epoch       uint64
	completedAt time.Time
	snapshot    githubsource.RegistrationSnapshot
	err         error
}

type githubLeadershipTerm struct {
	epoch           uint64
	openResults     chan githubRepositoryOpenResult
	discoveryResult chan githubDiscoveryResult
	discoveryActive bool
	background      sync.WaitGroup
	openSlots       chan struct{}
	operationMutex  sync.Mutex
	operations      map[uint64]context.CancelFunc
	nextOperation   uint64
}

type githubSourceRuntime struct {
	manager           githubManagerRuntime
	capacities        capacityAggregator
	owner             string
	handler           githubsource.EventHandler
	config            githubsource.ManagerConfig
	discoveryInterval time.Duration
	currentTime       func() time.Time
	nextDiscovery     time.Time
	nextMemberRefresh time.Time
	adapters          []*githubsource.Adapter
	polls             map[*githubsource.Adapter]*githubPollAttempt
	publishedCapacity map[core.RunnerPool]int
	leaderInstalled   bool
	report            func(error)

	epoch                   uint64
	term                    *githubLeadershipTerm
	desired                 map[string][]githubsource.Registration
	opening                 map[string][]githubsource.Registration
	generations             map[string]*githubsource.RepositorySessionGeneration
	retiring                []*githubsource.RepositorySessionGeneration
	cleanupMu               sync.Mutex
	abandoned               []*githubsource.RepositorySessionGeneration
	sessionActivationActive bool
	sessionActivationTotal  int
	sessionActivationLast   int
}

func newGitHubSourceRuntime(
	manager githubManagerRuntime,
	capacities capacityAggregator,
	owner string,
	handler githubsource.EventHandler,
	config githubsource.ManagerConfig,
	discoveryInterval time.Duration,
	currentTime func() time.Time,
) *githubSourceRuntime {
	return &githubSourceRuntime{
		manager: manager, capacities: capacities, owner: owner, handler: handler,
		config: config, discoveryInterval: discoveryInterval, currentTime: currentTime,
		polls: make(map[*githubsource.Adapter]*githubPollAttempt), publishedCapacity: make(map[core.RunnerPool]int),
		desired: make(map[string][]githubsource.Registration), opening: make(map[string][]githubsource.Registration),
		generations: make(map[string]*githubsource.RepositorySessionGeneration),
	}
}

func (source *githubSourceRuntime) RefreshMembers(ctx context.Context) error {
	if source.currentTime().Before(source.nextMemberRefresh) {
		return nil
	}
	if err := source.manager.RefreshMembers(ctx); err != nil {
		return fmt.Errorf("refresh GitHub member routes: %w", err)
	}
	source.nextMemberRefresh = source.currentTime().Add(source.discoveryInterval)
	return nil
}

func (source *githubSourceRuntime) OpenLeader(ctx context.Context) error {
	if source.leaderInstalled {
		return nil
	}
	snapshot, err := source.manager.RegistrationSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("load committed GitHub registrations: %w", err)
	}
	source.epoch++
	groups := groupActiveRegistrations(snapshot.Active)
	term := &githubLeadershipTerm{
		epoch:           source.epoch,
		openResults:     make(chan githubRepositoryOpenResult, max(1, len(groups))),
		discoveryResult: make(chan githubDiscoveryResult, 1),
		openSlots:       make(chan struct{}, githubRepositorySessionConcurrency),
		operations:      make(map[uint64]context.CancelFunc),
	}
	source.term = term
	source.leaderInstalled = true
	source.desired = groups
	source.beginSessionActivation()
	source.scheduleRequiredRepositoryGenerations(ctx, term)
	source.startDiscovery(ctx, term)
	return nil
}

func (source *githubSourceRuntime) startDiscovery(ctx context.Context, term *githubLeadershipTerm) {
	if term == nil || term.discoveryActive {
		return
	}
	term.discoveryActive = true
	operationContext, operationID := term.startOperation(ctx)
	term.background.Add(1)
	go func() {
		defer term.background.Done()
		defer term.finishOperation(operationID)
		snapshot, err := source.manager.Reconcile(operationContext, source.config)
		result := githubDiscoveryResult{epoch: term.epoch, completedAt: source.currentTime(), snapshot: snapshot, err: err}
		select {
		case term.discoveryResult <- result:
		case <-operationContext.Done():
		}
	}()
}

func (source *githubSourceRuntime) scheduleRequiredRepositoryGenerations(
	ctx context.Context,
	term *githubLeadershipTerm,
) {
	for _, repository := range sortedRegistrationRepositories(source.desired) {
		records := source.desired[repository]
		if generation := source.generations[repository]; generation != nil &&
			sameRegistrations(generation.Registrations(), records) {
			continue
		}
		if pending, exists := source.opening[repository]; exists && sameRegistrations(pending, records) {
			continue
		}
		source.opening[repository] = append([]githubsource.Registration(nil), records...)
		source.startRepositoryGeneration(ctx, term, repository, records)
	}
}

func (source *githubSourceRuntime) startRepositoryGeneration(
	ctx context.Context,
	term *githubLeadershipTerm,
	repository string,
	records []githubsource.Registration,
) {
	records = append([]githubsource.Registration(nil), records...)
	operationContext, operationID := term.startOperation(ctx)
	term.background.Add(1)
	go func() {
		defer term.finishOperation(operationID)
		source.openRepositoryGeneration(operationContext, term, repository, records)
	}()
}

func (source *githubSourceRuntime) openRepositoryGeneration(
	ctx context.Context,
	term *githubLeadershipTerm,
	repository string,
	records []githubsource.Registration,
) {
	defer term.background.Done()
	select {
	case term.openSlots <- struct{}{}:
		defer func() { <-term.openSlots }()
	case <-ctx.Done():
		return
	}
	generation, err := source.manager.OpenRepositorySessions(ctx, records, source.owner, source.handler)
	if ctx.Err() != nil {
		if generation != nil {
			source.abandonGeneration(generation)
		}
		return
	}
	result := githubRepositoryOpenResult{epoch: term.epoch, repository: repository,
		records: records, generation: generation, err: err}
	select {
	case term.openResults <- result:
	case <-ctx.Done():
		if generation != nil {
			source.abandonGeneration(generation)
		}
	}
}

func (source *githubSourceRuntime) CloseLeader(ctx context.Context) error {
	if !source.leaderInstalled {
		return source.retryRetiringGenerations(ctx)
	}
	term := source.term
	source.leaderInstalled = false
	source.term = nil
	source.epoch++
	if term != nil {
		term.cancelOperations()
	}
	stopErr := source.stopAllPolls(ctx)
	active := source.generations
	source.generations = make(map[string]*githubsource.RepositorySessionGeneration)
	source.desired = make(map[string][]githubsource.Registration)
	source.opening = make(map[string][]githubsource.Registration)
	source.adapters = nil
	source.sessionActivationActive = false
	var cleanupErr error
	for _, repository := range sortedGenerationRepositories(active) {
		if err := active[repository].Close(ctx); err != nil {
			source.retiring = append(source.retiring, active[repository])
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if term != nil {
		cleanupErr = errors.Join(cleanupErr, source.waitForTerm(ctx, term), source.drainCancelledTerm(ctx, term))
	}
	source.collectAbandonedGenerations()
	cleanupErr = errors.Join(cleanupErr, source.retryRetiringGenerations(ctx))
	return errors.Join(stopErr, cleanupErr)
}

func (term *githubLeadershipTerm) startOperation(ctx context.Context) (context.Context, uint64) {
	operationContext, cancel := context.WithCancel(ctx)
	term.operationMutex.Lock()
	term.nextOperation++
	operationID := term.nextOperation
	term.operations[operationID] = cancel
	term.operationMutex.Unlock()
	return operationContext, operationID
}

func (term *githubLeadershipTerm) finishOperation(operationID uint64) {
	term.operationMutex.Lock()
	cancel := term.operations[operationID]
	delete(term.operations, operationID)
	term.operationMutex.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (term *githubLeadershipTerm) cancelOperations() {
	term.operationMutex.Lock()
	operations := term.operations
	term.operations = make(map[uint64]context.CancelFunc)
	term.operationMutex.Unlock()
	for _, cancel := range operations {
		cancel()
	}
}

func (source *githubSourceRuntime) beginSessionActivation() {
	source.sessionActivationActive = false
	required := 0
	for repository, records := range source.desired {
		generation := source.generations[repository]
		if generation == nil || !sameRegistrations(generation.Registrations(), records) {
			required++
		}
	}
	if required == 0 {
		return
	}
	source.sessionActivationActive = true
	source.sessionActivationTotal = len(source.desired)
	source.sessionActivationLast = source.readyRepositoryGenerationCount()
	reportProgress(source.report, slog.LevelInfo, operationalEvent{
		operation: "source_sessions", stage: "open_sessions", outcome: "started",
		total: source.sessionActivationTotal,
	})
}

func (source *githubSourceRuntime) reportSessionActivationProgress() {
	if !source.sessionActivationActive {
		return
	}
	current := source.readyRepositoryGenerationCount()
	total := source.sessionActivationTotal
	if current == total {
		reportProgress(source.report, slog.LevelInfo, operationalEvent{
			operation: "source_sessions", stage: "open_sessions", outcome: "completed",
			count: current, total: total,
		})
		source.sessionActivationActive = false
		return
	}
	if current == 1 || current*20/total > source.sessionActivationLast*20/total {
		reportProgress(source.report, slog.LevelInfo, operationalEvent{
			operation: "source_sessions", stage: "open_sessions", outcome: "progress",
			current: current, total: total,
		})
	}
	source.sessionActivationLast = current
}

func (source *githubSourceRuntime) readyRepositoryGenerationCount() int {
	ready := 0
	for repository, records := range source.desired {
		generation := source.generations[repository]
		if generation != nil && sameRegistrations(generation.Registrations(), records) {
			ready++
		}
	}
	return ready
}

func (source *githubSourceRuntime) waitForTerm(ctx context.Context, term *githubLeadershipTerm) error {
	finished := make(chan struct{})
	go func() {
		term.background.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for GitHub leadership operations to stop: %w", ctx.Err())
	}
}

func (source *githubSourceRuntime) drainCancelledTerm(
	ctx context.Context,
	term *githubLeadershipTerm,
) error {
	var joined error
	for {
		select {
		case result := <-term.openResults:
			if result.generation != nil {
				if err := result.generation.Close(ctx); err != nil {
					source.retiring = append(source.retiring, result.generation)
					joined = errors.Join(joined, err)
				}
			}
		default:
			return joined
		}
	}
}

func (source *githubSourceRuntime) abandonGeneration(generation *githubsource.RepositorySessionGeneration) {
	source.cleanupMu.Lock()
	source.abandoned = append(source.abandoned, generation)
	source.cleanupMu.Unlock()
}

func (source *githubSourceRuntime) collectAbandonedGenerations() {
	source.cleanupMu.Lock()
	abandoned := source.abandoned
	source.abandoned = nil
	source.cleanupMu.Unlock()
	source.retiring = append(source.retiring, abandoned...)
}

func groupActiveRegistrations(records []githubsource.Registration) map[string][]githubsource.Registration {
	groups := make(map[string][]githubsource.Registration)
	for _, record := range records {
		groups[record.RepositoryURL] = append(groups[record.RepositoryURL], record)
	}
	for repository, registrations := range groups {
		groups[repository] = canonicalRegistrations(registrations)
	}
	return groups
}

func sortedRegistrationRepositories(groups map[string][]githubsource.Registration) []string {
	repositories := make([]string, 0, len(groups))
	for repository := range groups {
		repositories = append(repositories, repository)
	}
	sort.Strings(repositories)
	return repositories
}

func sortedGenerationRepositories(
	generations map[string]*githubsource.RepositorySessionGeneration,
) []string {
	repositories := make([]string, 0, len(generations))
	for repository := range generations {
		repositories = append(repositories, repository)
	}
	sort.Strings(repositories)
	return repositories
}

func sameRegistrations(first, second []githubsource.Registration) bool {
	return slices.Equal(canonicalRegistrations(first), canonicalRegistrations(second))
}

func canonicalRegistrations(records []githubsource.Registration) []githubsource.Registration {
	canonical := append([]githubsource.Registration(nil), records...)
	sort.Slice(canonical, func(firstIndex, secondIndex int) bool {
		first := canonical[firstIndex]
		second := canonical[secondIndex]
		if first.RepositoryURL != second.RepositoryURL {
			return first.RepositoryURL < second.RepositoryURL
		}
		if first.RunnerPool != second.RunnerPool {
			return first.RunnerPool < second.RunnerPool
		}
		if first.InstallationID != second.InstallationID {
			return first.InstallationID < second.InstallationID
		}
		if first.RunnerGroupID != second.RunnerGroupID {
			return first.RunnerGroupID < second.RunnerGroupID
		}
		return first.ScaleSetID < second.ScaleSetID
	})
	return canonical
}
