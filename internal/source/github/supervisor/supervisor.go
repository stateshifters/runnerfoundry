package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

const (
	githubRegistrationSessionConcurrency = 16
	githubRegistrationSessionOpenTimeout = time.Minute
)

type Manager interface {
	ActiveRegistrations(ctx context.Context) ([]githubsource.Registration, error)
	Reconcile(ctx context.Context, config githubsource.ManagerConfig) ([]githubsource.Registration, error)
	RefreshMembers(ctx context.Context, config githubsource.ManagerConfig) error
	OpenSourceRegistrationSession(ctx context.Context, record githubsource.Registration,
		configured githubsource.RunnerPoolConfig, owner string,
		sourceEvents core.SourceEventHandler,
		assignedDemand githubsource.AssignedDemandHandler) (*githubsource.RegistrationSession, error)
}

type SessionHandler interface {
	core.SourceEventHandler
	githubsource.AssignedDemandHandler
}

type githubRegistrationKey struct {
	repository string
	runnerPool core.RunnerPool
}

type githubRegistrationOpenResult struct {
	epoch     uint64
	key       githubRegistrationKey
	requested githubsource.Registration
	session   *githubsource.RegistrationSession
	err       error
}

type githubDiscoveryResult struct {
	epoch         uint64
	completedAt   time.Time
	registrations []githubsource.Registration
	err           error
}

type githubLeadershipTerm struct {
	epoch           uint64
	openResults     chan githubRegistrationOpenResult
	discoveryResult chan githubDiscoveryResult
	discoveryActive bool
	background      sync.WaitGroup
	openSlots       chan struct{}
	openSlotsByApp  map[githubsource.AppID]chan struct{}
	openSlotMutex   sync.Mutex
	openSlotsPerApp int
	operationMutex  sync.Mutex
	operations      map[uint64]context.CancelFunc
	nextOperation   uint64
}

type Runtime struct {
	manager           Manager
	capacityProvider  core.CapacitySnapshotProvider
	owner             string
	handler           SessionHandler
	config            githubsource.ManagerConfig
	discoveryInterval time.Duration
	currentTime       func() time.Time
	nextDiscovery     time.Time
	nextMemberRefresh time.Time
	adapters          []*githubsource.Adapter
	polls             map[*githubsource.Adapter]*githubPollAttempt
	publishedCapacity map[core.RunnerPool]int
	leaderInstalled   bool
	observer          githubsource.ManagerObserver

	epoch                   uint64
	term                    *githubLeadershipTerm
	desired                 map[githubRegistrationKey]githubsource.Registration
	opening                 map[githubRegistrationKey]githubsource.Registration
	sessions                map[githubRegistrationKey]*githubsource.RegistrationSession
	retiring                []*githubsource.RegistrationSession
	cleanupMu               sync.Mutex
	abandoned               []*githubsource.RegistrationSession
	sessionActivationActive bool
	sessionActivationTotal  int
	sessionActivationLast   int
	sessionOpenTimeout      time.Duration
}

func New(
	manager Manager,
	capacityProvider core.CapacitySnapshotProvider,
	owner string,
	handler SessionHandler,
	config githubsource.ManagerConfig,
	discoveryInterval time.Duration,
	currentTime func() time.Time,
	observer githubsource.ManagerObserver,
) *Runtime {
	return &Runtime{
		manager: manager, capacityProvider: capacityProvider, owner: owner, handler: handler,
		config: config, discoveryInterval: discoveryInterval, currentTime: currentTime, observer: observer,
		polls: make(map[*githubsource.Adapter]*githubPollAttempt), publishedCapacity: make(map[core.RunnerPool]int),
		desired:            make(map[githubRegistrationKey]githubsource.Registration),
		opening:            make(map[githubRegistrationKey]githubsource.Registration),
		sessions:           make(map[githubRegistrationKey]*githubsource.RegistrationSession),
		sessionOpenTimeout: githubRegistrationSessionOpenTimeout,
	}
}

func (source *Runtime) RefreshMembers(ctx context.Context) error {
	if source.currentTime().Before(source.nextMemberRefresh) {
		return nil
	}
	if declarations, available := source.manager.(interface {
		PublishRunnerPoolDeclarations(
			ctx context.Context,
			memberID core.MemberID,
			config githubsource.ManagerConfig,
		) error
	}); available {
		if err := declarations.PublishRunnerPoolDeclarations(ctx, core.MemberID(source.owner), source.config); err != nil {
			return fmt.Errorf("publish GitHub Runner Pool declarations: %w", err)
		}
	}
	if err := source.manager.RefreshMembers(ctx, source.config); err != nil {
		return fmt.Errorf("refresh GitHub member routes: %w", err)
	}
	source.nextMemberRefresh = source.currentTime().Add(source.discoveryInterval)
	return nil
}

func (source *Runtime) OpenLeader(ctx context.Context) error {
	if source.leaderInstalled {
		return nil
	}
	registrations, err := source.manager.ActiveRegistrations(ctx)
	if err != nil {
		return fmt.Errorf("load committed GitHub registrations: %w", err)
	}
	source.epoch++
	desired := configuredGitHubRegistrationRoutes(registrations)
	term := &githubLeadershipTerm{
		epoch: source.epoch, openResults: make(chan githubRegistrationOpenResult, max(1, len(desired))),
		discoveryResult: make(chan githubDiscoveryResult, 1),
		openSlots:       make(chan struct{}, githubRegistrationSessionConcurrency),
		openSlotsByApp:  make(map[githubsource.AppID]chan struct{}),
		openSlotsPerApp: githubOpenSlotsPerApp(source.manager, registrations),
		operations:      make(map[uint64]context.CancelFunc),
	}
	source.term = term
	source.leaderInstalled = true
	source.desired = desired
	source.beginSessionActivation()
	source.scheduleRequiredRegistrationSessions(ctx, term)
	sharedDeclarations, available := source.manager.(interface{ UsesSharedRunnerPoolDeclarations() bool })
	if !available || !sharedDeclarations.UsesSharedRunnerPoolDeclarations() {
		source.startDiscovery(ctx, term, source.config)
	}
	return nil
}

func (source *Runtime) startDiscovery(
	ctx context.Context,
	term *githubLeadershipTerm,
	configs ...githubsource.ManagerConfig,
) {
	if term == nil || term.discoveryActive {
		return
	}
	term.discoveryActive = true
	config := source.config
	if len(configs) != 0 {
		config = configs[0]
	}
	operationContext, operationID := term.startOperation(ctx)
	term.background.Add(1)
	go func() {
		defer term.background.Done()
		defer term.finishOperation(operationID)
		registrations, err := source.manager.Reconcile(operationContext, config)
		result := githubDiscoveryResult{epoch: term.epoch, completedAt: source.currentTime(),
			registrations: registrations, err: err}
		select {
		case term.discoveryResult <- result:
		case <-operationContext.Done():
		}
	}()
}

func (source *Runtime) scheduleRequiredRegistrationSessions(
	ctx context.Context,
	term *githubLeadershipTerm,
) {
	for _, key := range sortedGitHubRegistrationKeys(source.desired) {
		record := source.desired[key]
		if session := source.sessions[key]; session != nil && session.Registration() == record {
			continue
		}
		if pending, exists := source.opening[key]; exists && pending == record {
			continue
		}
		source.opening[key] = record
		source.startRegistrationSession(ctx, term, key, record,
			githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool})
	}
}

func (source *Runtime) startRegistrationSession(
	ctx context.Context,
	term *githubLeadershipTerm,
	key githubRegistrationKey,
	record githubsource.Registration,
	configured githubsource.RunnerPoolConfig,
) {
	operationContext, operationID := term.startOperation(ctx)
	term.background.Add(1)
	go func() {
		defer term.finishOperation(operationID)
		source.openRegistrationSession(operationContext, term, key, record, configured)
	}()
}

func (source *Runtime) openRegistrationSession(
	ctx context.Context,
	term *githubLeadershipTerm,
	key githubRegistrationKey,
	record githubsource.Registration,
	configured githubsource.RunnerPoolConfig,
) {
	defer term.background.Done()
	release, acquired := term.acquireOpenSlot(ctx, record.GitHubAppID)
	if !acquired {
		return
	}
	defer release()
	openContext, cancel := context.WithTimeout(ctx, source.sessionOpenTimeout)
	defer cancel()
	session, err := source.manager.OpenSourceRegistrationSession(
		openContext, record, configured, source.owner, source.handler, source.handler,
	)
	if ctx.Err() != nil {
		if session != nil {
			source.abandonSession(session)
		}
		return
	}
	result := githubRegistrationOpenResult{epoch: term.epoch, key: key, requested: record, session: session, err: err}
	source.publishRegistrationOpenResult(ctx, term, result)
}

func (source *Runtime) publishRegistrationOpenResult(
	ctx context.Context,
	term *githubLeadershipTerm,
	result githubRegistrationOpenResult,
) {
	select {
	case term.openResults <- result:
	case <-ctx.Done():
		if result.session != nil {
			source.abandonSession(result.session)
		}
	}
}

func githubOpenSlotsPerApp(manager Manager, registrations []githubsource.Registration) int {
	appIDs := make(map[githubsource.AppID]struct{})
	if configured, available := manager.(interface{ GitHubAppIDs() []githubsource.AppID }); available {
		for _, appID := range configured.GitHubAppIDs() {
			appIDs[appID] = struct{}{}
		}
	}
	for _, registration := range registrations {
		appIDs[registration.GitHubAppID] = struct{}{}
	}
	appCount := max(1, len(appIDs))
	return max(1, githubRegistrationSessionConcurrency/appCount)
}

func (term *githubLeadershipTerm) acquireOpenSlot(
	ctx context.Context,
	appID githubsource.AppID,
) (func(), bool) {
	appSlots := term.appOpenSlots(appID)
	if !acquireGitHubOpenSlot(ctx, appSlots) {
		return nil, false
	}
	if !acquireGitHubOpenSlot(ctx, term.openSlots) {
		releaseGitHubOpenSlot(appSlots)
		return nil, false
	}
	return func() {
		releaseGitHubOpenSlot(term.openSlots)
		releaseGitHubOpenSlot(appSlots)
	}, true
}

func (term *githubLeadershipTerm) appOpenSlots(appID githubsource.AppID) chan struct{} {
	term.openSlotMutex.Lock()
	defer term.openSlotMutex.Unlock()
	appSlots := term.openSlotsByApp[appID]
	if appSlots == nil && term.openSlotsPerApp > 0 {
		appSlots = make(chan struct{}, term.openSlotsPerApp)
		term.openSlotsByApp[appID] = appSlots
	}
	return appSlots
}

func acquireGitHubOpenSlot(ctx context.Context, slots chan struct{}) bool {
	if slots == nil {
		return true
	}
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func releaseGitHubOpenSlot(slots chan struct{}) {
	if slots != nil {
		<-slots
	}
}

func (source *Runtime) CloseLeader(ctx context.Context) error {
	if !source.leaderInstalled {
		return source.retryRetiringSessions(ctx)
	}
	term := source.term
	source.leaderInstalled = false
	source.term = nil
	source.epoch++
	if term != nil {
		term.cancelOperations()
	}
	stopErr := source.stopAllPolls(ctx)
	active := source.sessions
	source.sessions = make(map[githubRegistrationKey]*githubsource.RegistrationSession)
	source.desired = make(map[githubRegistrationKey]githubsource.Registration)
	source.opening = make(map[githubRegistrationKey]githubsource.Registration)
	source.adapters = nil
	source.sessionActivationActive = false
	var cleanupErr error
	for _, key := range sortedGitHubRegistrationKeys(active) {
		if err := active[key].Close(ctx); err != nil {
			source.retiring = append(source.retiring, active[key])
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if term != nil {
		cleanupErr = errors.Join(cleanupErr, source.waitForTerm(ctx, term), source.drainCancelledTerm(ctx, term))
	}
	source.collectAbandonedSessions()
	cleanupErr = errors.Join(cleanupErr, source.retryRetiringSessions(ctx))
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

func (source *Runtime) beginSessionActivation() {
	source.sessionActivationActive = false
	required := 0
	for key, record := range source.desired {
		session := source.sessions[key]
		if session == nil || session.Registration() != record {
			required++
		}
	}
	if required == 0 {
		return
	}
	source.sessionActivationActive = true
	source.sessionActivationTotal = len(source.desired)
	source.sessionActivationLast = source.readyRegistrationSessionCount()
	source.report(githubsource.ManagerActivity{
		Visibility: githubsource.ActivityInfo,
		Operation:  "source_sessions", Stage: "open_sessions", Outcome: "started",
		Total: source.sessionActivationTotal,
	})
}

func (source *Runtime) reportSessionActivationProgress() {
	if !source.sessionActivationActive {
		return
	}
	current := source.readyRegistrationSessionCount()
	total := source.sessionActivationTotal
	if current == total {
		source.report(githubsource.ManagerActivity{
			Visibility: githubsource.ActivityInfo,
			Operation:  "source_sessions", Stage: "open_sessions", Outcome: "completed",
			Count: current, Total: total,
		})
		source.sessionActivationActive = false
		return
	}
	if current == 1 || current*20/total > source.sessionActivationLast*20/total {
		source.report(githubsource.ManagerActivity{
			Visibility: githubsource.ActivityInfo,
			Operation:  "source_sessions", Stage: "open_sessions", Outcome: "progress",
			Current: current, Total: total,
		})
	}
	source.sessionActivationLast = current
}

func (source *Runtime) readyRegistrationSessionCount() int {
	ready := 0
	for key, record := range source.desired {
		session := source.sessions[key]
		if session != nil && session.Registration() == record {
			ready++
		}
	}
	return ready
}

func (source *Runtime) waitForTerm(ctx context.Context, term *githubLeadershipTerm) error {
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

func (source *Runtime) drainCancelledTerm(ctx context.Context, term *githubLeadershipTerm) error {
	var joined error
	for {
		select {
		case result := <-term.openResults:
			if result.session != nil {
				if err := result.session.Close(ctx); err != nil {
					source.retiring = append(source.retiring, result.session)
					joined = errors.Join(joined, err)
				}
			}
		default:
			return joined
		}
	}
}

func (source *Runtime) abandonSession(session *githubsource.RegistrationSession) {
	source.cleanupMu.Lock()
	source.abandoned = append(source.abandoned, session)
	source.cleanupMu.Unlock()
}

func (source *Runtime) collectAbandonedSessions() {
	source.cleanupMu.Lock()
	abandoned := source.abandoned
	source.abandoned = nil
	source.cleanupMu.Unlock()
	source.retiring = append(source.retiring, abandoned...)
}

func configuredGitHubRegistrationRoutes(records []githubsource.Registration) map[githubRegistrationKey]githubsource.Registration {
	routes := make(map[githubRegistrationKey]githubsource.Registration)
	for _, record := range canonicalRegistrations(records) {
		routes[registrationKey(record)] = record
	}
	return routes
}

func registrationKey(record githubsource.Registration) githubRegistrationKey {
	return githubRegistrationKey{repository: record.RepositoryURL, runnerPool: record.RunnerPool}
}

func sortedGitHubRegistrationKeys[Value any](routes map[githubRegistrationKey]Value) []githubRegistrationKey {
	keys := make([]githubRegistrationKey, 0, len(routes))
	for key := range routes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(firstIndex, secondIndex int) bool {
		if keys[firstIndex].repository != keys[secondIndex].repository {
			return keys[firstIndex].repository < keys[secondIndex].repository
		}
		return keys[firstIndex].runnerPool < keys[secondIndex].runnerPool
	})
	return keys
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
