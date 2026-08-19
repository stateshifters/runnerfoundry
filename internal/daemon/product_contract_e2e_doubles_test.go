//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

type e2eManualTicker struct {
	ticks    chan time.Time
	stopped  chan struct{}
	once     sync.Once
	assigned atomic.Bool
}

func newE2EManualTicker() *e2eManualTicker {
	return &e2eManualTicker{ticks: make(chan time.Time, 1), stopped: make(chan struct{})}
}

func (ticker *e2eManualTicker) Ticks() <-chan time.Time { return ticker.ticks }
func (ticker *e2eManualTicker) Stop() {
	ticker.once.Do(func() { close(ticker.stopped) })
}
func (ticker *e2eManualTicker) Tick(at time.Time) bool {
	select {
	case <-ticker.stopped:
		return false
	case ticker.ticks <- at:
		return true
	}
}

type e2eGitHost struct {
	repository             transport.Repository
	repositories           []transport.Repository
	registrations          map[string]transport.Registration
	sessions               map[string]*e2eGitHubSession
	registrationOK         map[string]chan struct{}
	sessionOK              map[string]chan struct{}
	sessionOpened          chan string
	cleanups               chan string
	mutex                  sync.Mutex
	sessionOpens           map[string]int
	discoveredRepositories []int
	clientCalls            int
	ensureCalls            map[string]int
	openGates              map[string]*e2eSessionOpenGate
	discoveryGates         []*e2eDiscoveryGate
	cleanupGates           []*e2eCleanupGate
	pruneRemoved           []transport.OwnedRunnerPool
	pruneCalls             []e2ePruneCall
	registrationOns        map[string]*sync.Once
	sessionOns             map[string]*sync.Once
}

func newE2EGitHost() *e2eGitHost {
	registrations := map[string]transport.Registration{
		"linux":   {ID: 29, Name: "linux", RunnerGroupID: 11},
		"windows": {ID: 30, Name: "windows", RunnerGroupID: 11},
	}
	return (&e2eGitHost{
		repository:    transport.Repository{InstallationID: 71, URL: "https://github.example/runnerfoundry/e2e"},
		registrations: registrations,
		sessions: map[string]*e2eGitHubSession{
			"linux": newE2EGitHubSession(), "windows": newE2EGitHubSession(),
		},
		registrationOK: map[string]chan struct{}{"linux": make(chan struct{}), "windows": make(chan struct{})},
		sessionOK:      map[string]chan struct{}{"linux": make(chan struct{}), "windows": make(chan struct{})},
		registrationOns: map[string]*sync.Once{
			"linux": {}, "windows": {},
		},
		sessionOns: map[string]*sync.Once{"linux": {}, "windows": {}},
		cleanups:   make(chan string, 8), sessionOpened: make(chan string, 16),
		sessionOpens: make(map[string]int), openGates: make(map[string]*e2eSessionOpenGate),
		ensureCalls: make(map[string]int),
	}).withDefaultRepository()
}

type e2eSessionOpenGate struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

type e2eCleanupGate struct {
	started chan struct{}
	release chan struct{}
}

type e2eDiscoveryGate struct {
	started     chan struct{}
	finished    chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	err         error
}

type e2ePruneCall struct {
	runnerGroup transport.RunnerGroupID
	current     []transport.OwnedRunnerPool
	owned       []transport.OwnedRunnerPool
}

func (host *e2eGitHost) blockNextCleanup() *e2eCleanupGate {
	gate := &e2eCleanupGate{started: make(chan struct{}), release: make(chan struct{})}
	host.mutex.Lock()
	host.cleanupGates = append(host.cleanupGates, gate)
	host.mutex.Unlock()
	return gate
}

func (host *e2eGitHost) blockNextDiscovery(err error) *e2eDiscoveryGate {
	gate := &e2eDiscoveryGate{
		started: make(chan struct{}), finished: make(chan struct{}), release: make(chan struct{}), err: err,
	}
	host.mutex.Lock()
	host.discoveryGates = append(host.discoveryGates, gate)
	host.mutex.Unlock()
	return gate
}

func (gate *e2eDiscoveryGate) unblock() {
	gate.releaseOnce.Do(func() { close(gate.release) })
}

func (host *e2eGitHost) blockNextSessionOpen(runnerPool string) *e2eSessionOpenGate {
	gate := &e2eSessionOpenGate{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	host.mutex.Lock()
	host.openGates[runnerPool] = gate
	host.mutex.Unlock()
	return gate
}

func (host *e2eGitHost) withDefaultRepository() *e2eGitHost {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	host.repositories = []transport.Repository{host.repository}
	return host
}

func (host *e2eGitHost) withoutRepositories() {
	host.mutex.Lock()
	host.repositories = nil
	host.mutex.Unlock()
}

func (host *e2eGitHost) Discover(ctx context.Context) (transport.DiscoveryResult, error) {
	host.mutex.Lock()
	repositories := append([]transport.Repository(nil), host.repositories...)
	host.discoveredRepositories = append(host.discoveredRepositories, len(repositories))
	var gate *e2eDiscoveryGate
	if len(host.discoveryGates) != 0 {
		gate = host.discoveryGates[0]
		host.discoveryGates = host.discoveryGates[1:]
	}
	host.mutex.Unlock()
	if gate != nil {
		close(gate.started)
		defer close(gate.finished)
		select {
		case <-ctx.Done():
			return transport.DiscoveryResult{}, fmt.Errorf("wait for scripted GitHub discovery: %w", ctx.Err())
		case <-gate.release:
			if gate.err != nil {
				return transport.DiscoveryResult{}, fmt.Errorf("scripted GitHub discovery: %w", gate.err)
			}
		}
	}
	return transport.DiscoveryResult{Repositories: repositories}, nil
}

func (host *e2eGitHost) Client(
	_ context.Context,
	repository transport.Repository,
) (githubsource.ManagerClient, error) {
	host.mutex.Lock()
	host.clientCalls++
	host.mutex.Unlock()
	if repository != host.repository {
		return githubsource.ManagerClient{}, errors.New("unexpected GitHub repository")
	}
	return githubsource.ManagerClient{
		EnsureRunnerPool:   host.ensureRunnerPool,
		ResolveRunnerGroup: host.resolveRunnerGroup,
		OpenSession: func(
			ctx context.Context,
			registration transport.Registration,
			name string,
		) (githubsource.ManagedSession, error) {
			return host.openSession(ctx, registration, name)
		},
		GenerateBootstrap: host.generateBootstrap,
		CleanupRunner:     host.cleanupRunner,
		PruneRunnerPools:  host.pruneRunnerPools,
	}, nil
}

func (host *e2eGitHost) ensureRunnerPool(
	_ context.Context,
	runnerPool string,
	runnerGroup string,
) (transport.Registration, error) {
	host.mutex.Lock()
	host.ensureCalls[runnerPool]++
	host.mutex.Unlock()
	registration, exists := host.registrations[runnerPool]
	if !exists || runnerGroup != "default" {
		return transport.Registration{}, errors.New("unexpected GitHub Runner Pool registration")
	}
	host.registrationOns[runnerPool].Do(func() { close(host.registrationOK[runnerPool]) })
	return registration, nil
}

func (host *e2eGitHost) callSummary() (repositories []int, clientCalls int, ensures map[string]int) {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	ensures = make(map[string]int, len(host.ensureCalls))
	for runnerPool, calls := range host.ensureCalls {
		ensures[runnerPool] = calls
	}
	return append([]int(nil), host.discoveredRepositories...), host.clientCalls, ensures
}

func (host *e2eGitHost) resolveRunnerGroup(context.Context, string) (transport.RunnerGroupID, error) {
	return 11, nil
}

func (host *e2eGitHost) openSession(
	ctx context.Context,
	registration transport.Registration,
	_ string,
) (*e2eGitHubSession, error) {
	runnerPool := registration.Name
	expected, exists := host.registrations[runnerPool]
	if !exists || registration != expected {
		return nil, errors.New("unexpected GitHub registration session")
	}
	host.mutex.Lock()
	gate := host.openGates[runnerPool]
	delete(host.openGates, runnerPool)
	host.sessionOpens[runnerPool]++
	host.mutex.Unlock()
	if gate != nil {
		close(gate.started)
		select {
		case <-ctx.Done():
			close(gate.canceled)
			<-gate.release
		case <-gate.release:
		}
	}
	host.sessionOns[runnerPool].Do(func() { close(host.sessionOK[runnerPool]) })
	host.sessionOpened <- runnerPool
	return host.sessions[runnerPool], nil
}

func (host *e2eGitHost) sessionOpenCount(runnerPool string) int {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	return host.sessionOpens[runnerPool]
}

func (host *e2eGitHost) observeManagerActivity(activity githubsource.ManagerActivity) {
	host.mutex.Lock()
	session := host.sessions[string(activity.RunnerPool)]
	host.mutex.Unlock()
	if session != nil {
		session.observePoll(activity)
	}
}

func e2eObserveHostManagerActivities(
	host *e2eGitHost,
	observer githubsource.ManagerObserver,
) githubsource.ManagerObserver {
	report := observer.Report
	observer.Report = func(activity githubsource.ManagerActivity) {
		if report != nil {
			report(activity)
		}
		host.observeManagerActivity(activity)
	}
	return observer
}

func (*e2eGitHost) generateBootstrap(context.Context, transport.Registration, string) ([]byte, error) {
	return []byte("e2e-bootstrap"), nil
}

func (host *e2eGitHost) cleanupRunner(ctx context.Context, runnerName string) error {
	host.mutex.Lock()
	var gate *e2eCleanupGate
	if len(host.cleanupGates) != 0 {
		gate = host.cleanupGates[0]
		host.cleanupGates = host.cleanupGates[1:]
	}
	host.mutex.Unlock()
	if gate != nil {
		close(gate.started)
		select {
		case <-gate.release:
		case <-ctx.Done():
			return fmt.Errorf("scripted GitHub runner cleanup canceled: %w", ctx.Err())
		}
	}
	host.cleanups <- runnerName
	return nil
}

func (host *e2eGitHost) pruneRunnerPools(
	_ context.Context,
	runnerGroup transport.RunnerGroupID,
	current []transport.OwnedRunnerPool,
	owned []transport.OwnedRunnerPool,
) (transport.PruneResult, error) {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	host.pruneCalls = append(host.pruneCalls, e2ePruneCall{
		runnerGroup: runnerGroup,
		current:     append([]transport.OwnedRunnerPool(nil), current...),
		owned:       append([]transport.OwnedRunnerPool(nil), owned...),
	})
	return transport.PruneResult{Removed: append([]transport.OwnedRunnerPool(nil), host.pruneRemoved...)}, nil
}

func (host *e2eGitHost) scriptPruneRemoval(removed []transport.OwnedRunnerPool) {
	host.mutex.Lock()
	host.pruneRemoved = append([]transport.OwnedRunnerPool(nil), removed...)
	host.pruneCalls = nil
	host.mutex.Unlock()
}

func (host *e2eGitHost) recordedPruneCalls() []e2ePruneCall {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	return append([]e2ePruneCall(nil), host.pruneCalls...)
}

type e2eGitHubSession struct {
	messages       chan *upstream.RunnerScaleSetMessage
	deliveries     chan *upstream.RunnerScaleSetMessage
	pollRequested  chan struct{}
	capacities     chan int
	ackAttempts    chan int
	ackErrors      []error
	acquireScripts []e2eAcquisitionScript
	acquisitions   chan []int64
	ackEffects     map[int]chan struct{}
	pollEffects    map[int]chan struct{}
	pendingPollAck int
	nextMessageID  int
	closed         chan struct{}
	closeStarted   chan struct{}
	closeRelease   <-chan struct{}
	capacityValue  int
	capacityTick   uint64
	capacityRead   uint64
	capacityChange chan struct{}
	mutex          sync.Mutex
	closeStartOnce sync.Once
	closeOnce      sync.Once
}

type e2eAcquisitionScript struct {
	acquired []int64
	err      error
}

type e2eCapacityObservation struct {
	published int
	tick      uint64
	read      uint64
	changed   <-chan struct{}
}

func (session *e2eGitHubSession) assignMessageID(message *upstream.RunnerScaleSetMessage) int {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	session.nextMessageID++
	message.MessageID = session.nextMessageID
	session.ackEffectLocked(message.MessageID)
	session.pollEffectLocked(message.MessageID)
	return message.MessageID
}

func (session *e2eGitHubSession) acknowledgement(messageID int) <-chan struct{} {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return session.ackEffectLocked(messageID)
}

func (session *e2eGitHubSession) ackEffectLocked(messageID int) chan struct{} {
	effect := session.ackEffects[messageID]
	if effect == nil {
		effect = make(chan struct{})
		session.ackEffects[messageID] = effect
	}
	return effect
}

func (session *e2eGitHubSession) pollCompletion(messageID int) <-chan struct{} {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return session.pollEffectLocked(messageID)
}

func (session *e2eGitHubSession) pollEffectLocked(messageID int) chan struct{} {
	effect := session.pollEffects[messageID]
	if effect == nil {
		effect = make(chan struct{})
		session.pollEffects[messageID] = effect
	}
	return effect
}

func (session *e2eGitHubSession) observePoll(activity githubsource.ManagerActivity) {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	if activity.Stage == "acknowledge_message" && activity.Outcome == "completed" {
		session.pendingPollAck = int(activity.MessageID)
		return
	}
	if activity.Stage != "poll" || activity.Outcome != "completed" || session.pendingPollAck == 0 {
		return
	}
	effect := session.pollEffectLocked(session.pendingPollAck)
	select {
	case <-effect:
	default:
		close(effect)
	}
	session.pendingPollAck = 0
}

func newE2EGitHubSession() *e2eGitHubSession {
	return &e2eGitHubSession{
		messages: make(chan *upstream.RunnerScaleSetMessage, 4), pollRequested: make(chan struct{}, 4),
		deliveries:  make(chan *upstream.RunnerScaleSetMessage),
		capacities:  make(chan int, 16),
		ackAttempts: make(chan int, 8), ackEffects: make(map[int]chan struct{}),
		pollEffects:    make(map[int]chan struct{}),
		acquisitions:   make(chan []int64, 16),
		capacityChange: make(chan struct{}),
		closed:         make(chan struct{}), closeStarted: make(chan struct{}),
	}
}

func (session *e2eGitHubSession) blockClose() (<-chan struct{}, func()) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	session.mutex.Lock()
	session.closeRelease = release
	session.mutex.Unlock()
	return session.closeStarted, func() { releaseOnce.Do(func() { close(release) }) }
}

func (session *e2eGitHubSession) GetMessage(
	ctx context.Context,
	_ int,
	maxCapacity int,
) (*upstream.RunnerScaleSetMessage, error) {
	session.recordCapacity(maxCapacity)
	select {
	case session.pollRequested <- struct{}{}:
	default:
	}
	select {
	case message := <-session.deliveries:
		return message, nil
	case message := <-session.messages:
		return message, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for scripted GitHub message: %w", ctx.Err())
	}
}

func (session *e2eGitHubSession) recordCapacity(capacity int) {
	session.mutex.Lock()
	session.capacityValue = capacity
	session.capacityTick++
	close(session.capacityChange)
	session.capacityChange = make(chan struct{})
	session.mutex.Unlock()
	select {
	case session.capacities <- capacity:
	default:
	}
}

func (session *e2eGitHubSession) observeCapacity() e2eCapacityObservation {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return e2eCapacityObservation{
		published: session.capacityValue,
		tick:      session.capacityTick,
		read:      session.capacityRead,
		changed:   session.capacityChange,
	}
}

func (session *e2eGitHubSession) markCapacityRead(tick uint64) {
	session.mutex.Lock()
	if tick > session.capacityRead {
		session.capacityRead = tick
	}
	session.mutex.Unlock()
}

func (session *e2eGitHubSession) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	requested := append([]int64(nil), requestIDs...)
	session.acquisitions <- requested
	session.mutex.Lock()
	if len(session.acquireScripts) == 0 {
		session.mutex.Unlock()
		return requested, nil
	}
	script := session.acquireScripts[0]
	session.acquireScripts = session.acquireScripts[1:]
	session.mutex.Unlock()
	return append([]int64(nil), script.acquired...), script.err
}

func (session *e2eGitHubSession) scriptAcquisition(acquired []int64, err error) {
	session.mutex.Lock()
	session.acquireScripts = append(session.acquireScripts, e2eAcquisitionScript{
		acquired: append([]int64(nil), acquired...), err: err,
	})
	session.mutex.Unlock()
}

func (session *e2eGitHubSession) failNextAcknowledgement(err error) {
	session.mutex.Lock()
	session.ackErrors = append(session.ackErrors, err)
	session.mutex.Unlock()
}

func (session *e2eGitHubSession) DeleteMessage(_ context.Context, messageID int) error {
	session.mutex.Lock()
	var acknowledgementErr error
	if len(session.ackErrors) != 0 {
		acknowledgementErr = session.ackErrors[0]
		session.ackErrors = session.ackErrors[1:]
	}
	session.mutex.Unlock()
	session.ackAttempts <- messageID
	if acknowledgementErr != nil {
		return acknowledgementErr
	}
	session.mutex.Lock()
	effect := session.ackEffectLocked(messageID)
	select {
	case <-effect:
	default:
		close(effect)
	}
	session.mutex.Unlock()
	return nil
}

func (session *e2eGitHubSession) Close(ctx context.Context) error {
	session.closeStartOnce.Do(func() { close(session.closeStarted) })
	session.mutex.Lock()
	release := session.closeRelease
	session.mutex.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return fmt.Errorf("scripted GitHub session close canceled: %w", ctx.Err())
		}
	}
	session.closeOnce.Do(func() { close(session.closed) })
	return nil
}

type e2eProvider struct {
	mutex           sync.Mutex
	endpointID      execution.InventoryEndpointID
	instances       []execution.RunnerInstance
	startBehaviors  []e2eStartBehavior
	attemptHistory  []execution.StartRequest
	destroyHistory  []execution.RunnerInstance
	destroyErr      error
	inspectErr      error
	inspectionCount int
	destroyBarrier  *e2eProviderDestroyBarrier
	attempts        chan execution.StartRequest
	starts          chan execution.StartRequest
	destroys        chan execution.RunnerInstance
	destroyAttempts chan execution.RunnerInstance
	inspected       chan struct{}
	validated       chan struct{}
	refreshed       chan struct{}
	contexts        chan e2eProviderContext
	refreshOn       sync.Once
}

type e2eStartBehavior struct {
	err           error
	createPartial bool
	gate          *e2eStartGate
}

type e2eStartGate struct {
	started chan struct{}
	release chan struct{}
}

type e2eProviderContext struct {
	operation string
	deadline  time.Time
	observed  time.Time
	has       bool
}

func newE2EProvider() *e2eProvider {
	return newE2EProviderForEndpoint("e2e:provider")
}

func newE2EProviderForEndpoint(endpointID execution.InventoryEndpointID) *e2eProvider {
	return &e2eProvider{
		endpointID: endpointID,
		attempts:   make(chan execution.StartRequest, 8), starts: make(chan execution.StartRequest, 8),
		destroys:        make(chan execution.RunnerInstance, 8),
		destroyAttempts: make(chan execution.RunnerInstance, 8),
		inspected:       make(chan struct{}, 16), refreshed: make(chan struct{}),
		validated: make(chan struct{}, 8), contexts: make(chan e2eProviderContext, 32),
	}
}

func (provider *e2eProvider) Validate(ctx context.Context) error {
	provider.recordContext(ctx, "validate")
	provider.validated <- struct{}{}
	return nil
}
func (provider *e2eProvider) EndpointID() execution.InventoryEndpointID {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	return provider.endpointID
}

func (provider *e2eProvider) setEndpointID(endpointID execution.InventoryEndpointID) {
	provider.mutex.Lock()
	provider.endpointID = endpointID
	provider.mutex.Unlock()
}
func (provider *e2eProvider) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	return endpointID == provider.EndpointID()
}
func (provider *e2eProvider) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	_ core.MemberID,
) bool {
	return provider.KnowsEndpoint(endpointID)
}
func (provider *e2eProvider) Refresh(ctx context.Context) error {
	provider.recordContext(ctx, "refresh")
	provider.refreshOn.Do(func() { close(provider.refreshed) })
	return nil
}
func (provider *e2eProvider) Start(ctx context.Context, request execution.StartRequest) error {
	provider.recordContext(ctx, "start")
	behavior := provider.beginStart(request)
	if err := awaitE2EStartGate(ctx, behavior.gate); err != nil {
		return err
	}
	if behavior.err != nil {
		return behavior.err
	}
	provider.recordRunningInstance(request)
	provider.starts <- request
	return nil
}

func (provider *e2eProvider) beginStart(request execution.StartRequest) e2eStartBehavior {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	var behavior e2eStartBehavior
	if len(provider.startBehaviors) != 0 {
		behavior = provider.startBehaviors[0]
		provider.startBehaviors = provider.startBehaviors[1:]
	}
	provider.attemptHistory = append(provider.attemptHistory, request)
	provider.attempts <- request
	if behavior.createPartial {
		provider.instances = append(provider.instances, execution.RunnerInstance{
			ID: string(request.RunnerName), RunnerName: request.RunnerName, RunnerPool: request.RunnerPool,
			EndpointID: request.EndpointID, State: execution.RunnerStateUnknown,
		})
	}
	return behavior
}

func awaitE2EStartGate(ctx context.Context, gate *e2eStartGate) error {
	if gate == nil {
		return nil
	}
	close(gate.started)
	select {
	case <-gate.release:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("scripted provider start canceled: %w", ctx.Err())
	}
}

func (provider *e2eProvider) recordRunningInstance(request execution.StartRequest) {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	provider.instances = append(provider.instances, execution.RunnerInstance{
		ID: string(request.RunnerName), RunnerName: request.RunnerName, RunnerPool: request.RunnerPool,
		EndpointID: request.EndpointID, State: execution.RunnerStateRunning,
	})
}

func (provider *e2eProvider) failNextStart(err error) {
	provider.mutex.Lock()
	provider.startBehaviors = append(provider.startBehaviors, e2eStartBehavior{err: err})
	provider.mutex.Unlock()
}

func (provider *e2eProvider) failNextStartWithPartial(err error) {
	provider.mutex.Lock()
	provider.startBehaviors = append(provider.startBehaviors, e2eStartBehavior{err: err, createPartial: true})
	provider.mutex.Unlock()
}

func (provider *e2eProvider) blockNextStart(err error) *e2eStartGate {
	gate := &e2eStartGate{started: make(chan struct{}), release: make(chan struct{})}
	provider.mutex.Lock()
	provider.startBehaviors = append(provider.startBehaviors, e2eStartBehavior{err: err, gate: gate})
	provider.mutex.Unlock()
	return gate
}

func (provider *e2eProvider) attemptCount() int {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	return len(provider.attemptHistory)
}
func (provider *e2eProvider) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	provider.recordContext(ctx, "inspect")
	provider.refreshOn.Do(func() { close(provider.refreshed) })
	provider.inspected <- struct{}{}
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	provider.inspectionCount++
	if provider.inspectErr != nil {
		return nil, provider.inspectErr
	}
	return append([]execution.RunnerInstance(nil), provider.instances...), nil
}

func (provider *e2eProvider) inspectCount() int {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	return provider.inspectionCount
}

func (provider *e2eProvider) recordContext(ctx context.Context, operation string) {
	deadline, hasDeadline := ctx.Deadline()
	provider.contexts <- e2eProviderContext{
		operation: operation, deadline: deadline, observed: time.Now(), has: hasDeadline,
	}
}

func (provider *e2eProvider) setInspectionError(err error) {
	provider.mutex.Lock()
	provider.inspectErr = err
	provider.mutex.Unlock()
}
func (provider *e2eProvider) InspectEndpoint(
	ctx context.Context,
	_ execution.InventoryEndpointID,
) ([]execution.RunnerInstance, error) {
	return provider.Inspect(ctx)
}
func (provider *e2eProvider) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	provider.mutex.Lock()
	barrier := provider.destroyBarrier
	provider.destroyBarrier = nil
	provider.destroyAttempts <- instance
	provider.mutex.Unlock()
	if barrier != nil {
		barrier.started <- instance
		select {
		case <-barrier.release:
		case <-ctx.Done():
			return fmt.Errorf("scripted provider destroy canceled: %w", ctx.Err())
		}
	}
	provider.mutex.Lock()
	if provider.destroyErr != nil {
		err := provider.destroyErr
		provider.mutex.Unlock()
		return err
	}
	retained := provider.instances[:0]
	for _, candidate := range provider.instances {
		if candidate.RunnerName != instance.RunnerName {
			retained = append(retained, candidate)
		}
	}
	provider.instances = retained
	provider.destroyHistory = append(provider.destroyHistory, instance)
	provider.mutex.Unlock()
	provider.destroys <- instance
	return nil
}

func (provider *e2eProvider) destroyCount() int {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	return len(provider.destroyHistory)
}

func (provider *e2eProvider) setDestroyError(err error) {
	provider.mutex.Lock()
	provider.destroyErr = err
	provider.mutex.Unlock()
}
