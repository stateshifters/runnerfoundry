package github

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
	"golang.org/x/sync/errgroup"
)

const repositoryConcurrency = 4

type repositoryRegistrationResult struct {
	repository string
	records    []Registration
	err        error
}

type RunnerPoolConfig struct {
	RunnerPool  core.RunnerPool
	RunnerGroup string
}

type ManagerConfig struct {
	RunnerPools []RunnerPoolConfig
	Prune       bool
}

type RepositoryDiscoverer interface {
	Discover(ctx context.Context) (transport.DiscoveryResult, error)
}

type ManagedSession interface {
	MessageClient
	Close(ctx context.Context) error
}

type ManagerClient struct {
	EnsureRunnerPool  func(context.Context, string, string) (transport.Registration, error)
	OpenSession       func(context.Context, transport.Registration, string) (ManagedSession, error)
	GenerateBootstrap func(context.Context, transport.Registration, string) ([]byte, error)
	CleanupRunner     func(context.Context, string) error
	PruneRunnerPools  func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error)
}

type managedBootstrapClient struct{ client ManagerClient }

func (client managedBootstrapClient) GenerateBootstrap(
	ctx context.Context,
	registration transport.Registration,
	runnerName core.RunnerName,
) ([]byte, error) {
	return client.client.GenerateBootstrap(ctx, registration, string(runnerName))
}

func (client managedBootstrapClient) CleanupRunner(ctx context.Context, runnerName core.RunnerName) error {
	if client.client.CleanupRunner == nil {
		return errors.New("GitHub runner cleanup is required")
	}
	return client.client.CleanupRunner(ctx, string(runnerName))
}

type ManagerClientFactoryFunc func(context.Context, transport.Repository) (ManagerClient, error)

// TransportManagerClient adapts the concrete actions/scaleset transport client
// to the lifecycle manager's narrow testable boundary.
func NewTransportManagerClient(client *transport.Client) ManagerClient {
	return composeTransportManagerClient(
		client.EnsureRunnerPool,
		client.OpenSession,
		client.GenerateBootstrap,
		client.PruneRunnerPools,
		client.CleanupRunner,
	)
}

func composeTransportManagerClient(
	ensure func(context.Context, string, string) (transport.Registration, error),
	open func(context.Context, transport.Registration, string) (*transport.Session, error),
	generate func(context.Context, transport.Registration, string) ([]byte, error),
	prune func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error),
	cleanup func(context.Context, string) error,
) ManagerClient {
	return ManagerClient{
		EnsureRunnerPool:  wrapTransportEnsure(ensure),
		OpenSession:       wrapTransportOpen(open),
		GenerateBootstrap: wrapTransportGenerate(generate),
		PruneRunnerPools:  wrapTransportPrune(prune),
		CleanupRunner:     cleanup,
	}
}

func wrapTransportEnsure(
	ensure func(context.Context, string, string) (transport.Registration, error),
) func(context.Context, string, string) (transport.Registration, error) {
	return func(ctx context.Context, pool, group string) (transport.Registration, error) {
		registration, err := ensure(ctx, pool, group)
		if err != nil {
			return transport.Registration{}, fmt.Errorf("ensure GitHub Runner Pool: %w", err)
		}
		return registration, nil
	}
}

func wrapTransportOpen(
	open func(context.Context, transport.Registration, string) (*transport.Session, error),
) func(context.Context, transport.Registration, string) (ManagedSession, error) {
	return func(ctx context.Context, registration transport.Registration, owner string) (ManagedSession, error) {
		session, err := open(ctx, registration, owner)
		if err != nil {
			return nil, fmt.Errorf("open GitHub message session: %w", err)
		}
		return session, nil
	}
}

func wrapTransportGenerate(
	generate func(context.Context, transport.Registration, string) ([]byte, error),
) func(context.Context, transport.Registration, string) ([]byte, error) {
	return func(ctx context.Context, registration transport.Registration, runner string) ([]byte, error) {
		content, err := generate(ctx, registration, runner)
		if err != nil {
			return nil, fmt.Errorf("generate GitHub bootstrap: %w", err)
		}
		return content, nil
	}
}

func wrapTransportPrune(
	prune func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error),
) func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error) {
	return func(ctx context.Context, groupID transport.RunnerGroupID, current, owned []transport.OwnedRunnerPool) (transport.PruneResult, error) {
		result, err := prune(ctx, groupID, current, owned)
		if err != nil {
			return result, fmt.Errorf("prune GitHub Runner Pools: %w", err)
		}
		return result, nil
	}
}

// Manager owns explicit GitHub discovery, registration, message-session, and
// member-route lifecycle operations. Callers own scheduling, leadership, and
// supervision; Manager starts no background work.
type Manager struct {
	discoverer    RepositoryDiscoverer
	factory       ManagerClientFactoryFunc
	registrations RegistrationRepository
	observer      ManagerObserver
	source        *Source
	activityMu    sync.Mutex
}

type openedLeader struct {
	session ManagedSession
	adapter *Adapter
}

func NewManager(
	discoverer RepositoryDiscoverer,
	factory ManagerClientFactoryFunc,
	registrations RegistrationRepository,
	observer ManagerObserver,
) (*Manager, error) {
	if discoverer == nil || factory == nil || registrations == nil {
		return nil, errors.New("GitHub discoverer, client factory, and registration repository are required")
	}
	source := &Source{routes: make(map[routeKey]memberRoute)}
	return &Manager{
		discoverer: discoverer, factory: factory, registrations: registrations, observer: observer, source: source,
	}, nil
}

func (manager *Manager) Source() *Source {
	return manager.source
}

// RegistrationSnapshot returns the currently committed registration facts.
// The returned slices are defensive copies so callers cannot mutate durable
// repository-owned state.
func (manager *Manager) RegistrationSnapshot(ctx context.Context) (RegistrationSnapshot, error) {
	snapshot, err := manager.registrations.RegistrationSnapshot(ctx)
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("load GitHub registration snapshot: %w", err)
	}
	canonical, err := canonicalRegistrationSnapshot(snapshot)
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("validate GitHub registration snapshot: %w", err)
	}
	return canonical, nil
}

// Reconcile discovers repositories, ensures every configured Runner Pool,
// optionally prunes absent owned scale sets, and atomically persists the
// resulting registration records. It works on at most four repositories at a
// time while preserving discovery order in the committed snapshot and errors.
// A successful return is the exact snapshot passed to the repository boundary;
// failures return an empty snapshot.
func (manager *Manager) Reconcile(ctx context.Context, config ManagerConfig) (
	committed RegistrationSnapshot,
	resultErr error,
) {
	reconciliationStarted := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "reconciliation", Outcome: "started"})
	defer func() {
		outcome := "completed"
		if resultErr != nil {
			outcome = "failed"
		}
		manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
			Stage: "reconciliation", Outcome: outcome,
			Duration: manager.observer.currentTime().Sub(reconciliationStarted)})
	}()
	if err := validateManagerConfig(config); err != nil {
		return RegistrationSnapshot{}, err
	}
	return manager.reconcileConfigured(ctx, config)
}

func (manager *Manager) reconcileConfigured(ctx context.Context, config ManagerConfig) (RegistrationSnapshot, error) {
	previous, err := manager.loadRegistrationSnapshot(ctx)
	if err != nil {
		return RegistrationSnapshot{}, err
	}
	result, err := manager.discoverRepositories(ctx)
	if err != nil {
		return RegistrationSnapshot{}, err
	}
	if failureErr := discoveryFailures(result.InstallationFailures); failureErr != nil {
		return RegistrationSnapshot{}, failureErr
	}
	active, err := manager.ensureRegistrations(ctx, result.Repositories, config)
	if err != nil {
		return RegistrationSnapshot{}, err
	}
	owned := mergeOwnedRegistrations(previous.Owned, active)
	if config.Prune {
		owned, err = manager.pruneOwnedRegistrations(ctx, result.Repositories, active, owned)
		if err != nil {
			return RegistrationSnapshot{}, err
		}
	}
	return manager.persistRegistrationSnapshot(ctx, active, owned)
}

func (manager *Manager) loadRegistrationSnapshot(ctx context.Context) (RegistrationSnapshot, error) {
	started := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "load_registration_snapshot", Outcome: "started"})
	snapshot, err := manager.registrations.RegistrationSnapshot(ctx)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "load_registration_snapshot", Outcome: outcome, Count: len(snapshot.Active), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("load owned GitHub registrations: %w", err)
	}
	return snapshot, nil
}

func (manager *Manager) discoverRepositories(ctx context.Context) (transport.DiscoveryResult, error) {
	started := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "discover_repositories", Outcome: "started"})
	result, err := manager.discoverer.Discover(ctx)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "discover_repositories", Outcome: outcome, RepositoryCount: len(result.Repositories),
		InstallationCount: discoveredInstallationCount(result), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	if err != nil {
		return transport.DiscoveryResult{}, fmt.Errorf("discover GitHub repositories: %w", err)
	}
	return result, nil
}

func (manager *Manager) pruneOwnedRegistrations(ctx context.Context, repositories []transport.Repository, active, owned []Registration) ([]Registration, error) {
	started := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "prune_registrations", Outcome: "started", Total: len(owned)})
	pruned, err := manager.pruneRegistrations(ctx, repositories, active, owned)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "prune_registrations", Outcome: outcome, Count: len(pruned), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	return pruned, err
}

func (manager *Manager) persistRegistrationSnapshot(
	ctx context.Context,
	active []Registration,
	owned []Registration,
) (RegistrationSnapshot, error) {
	started := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "persist_registrations", Outcome: "started", Total: len(active)})
	snapshot, err := canonicalRegistrationSnapshot(RegistrationSnapshot{Active: active, Owned: owned})
	if err != nil {
		return RegistrationSnapshot{}, err
	}
	err = manager.replaceRegistrations(ctx, snapshot)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "persist_registrations", Outcome: outcome, Count: len(active), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("persist GitHub registrations: %w", err)
	}
	return snapshot, nil
}

func (manager *Manager) ensureRegistrations(
	ctx context.Context,
	repositories []transport.Repository,
	config ManagerConfig,
) ([]Registration, error) {
	results := make([]repositoryRegistrationResult, len(repositories))
	total := len(repositories)
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "configure_repositories", Outcome: "started", Total: total})
	waitErr := manager.ensureRepositoriesConcurrently(ctx, repositories, config, results)
	records, resultErr := manager.collectRepositoryResults(results, total, waitErr)
	if resultErr != nil {
		return nil, resultErr
	}
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "configure_repositories", Outcome: "completed", Count: len(records), Total: total})
	return records, nil
}

func (manager *Manager) ensureRepositoriesConcurrently(
	ctx context.Context,
	repositories []transport.Repository,
	config ManagerConfig,
	results []repositoryRegistrationResult,
) error {
	var group errgroup.Group
	var completionMutex sync.Mutex
	completed := 0
	group.SetLimit(repositoryConcurrency)
	for index, repository := range repositories {
		results[index].repository = repository.URL
		group.Go(func() error {
			current := index + 1
			results[index].records, results[index].err = manager.ensureRepositoryWithActivity(
				ctx, repository, config, current, len(repositories),
			)
			completionMutex.Lock()
			completed++
			manager.reportRepositoryProgress(completed, len(repositories))
			completionMutex.Unlock()
			return nil
		})
	}
	return errors.Join(group.Wait())
}

func (manager *Manager) collectRepositoryResults(
	results []repositoryRegistrationResult,
	total int,
	waitErr error,
) ([]Registration, error) {
	records := make([]Registration, 0)
	var joined error
	for index, result := range results {
		if result.err != nil {
			manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
				Stage: "configure_repositories", Outcome: "failed", Repository: result.repository,
				Current: index + 1, Total: total})
			joined = errors.Join(joined, result.err)
			continue
		}
		records = append(records, result.records...)
	}
	if resultErr := errors.Join(joined, waitErr); resultErr != nil {
		return nil, resultErr
	}
	return records, nil
}

func (manager *Manager) reportRepositoryProgress(current, total int) {
	if shouldReportManagerProgress(current, total) {
		manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
			Stage: "configure_repositories", Outcome: "progress", Current: current, Total: total})
	}
}

func (manager *Manager) ensureRepositoryWithActivity(ctx context.Context, repository transport.Repository,
	config ManagerConfig, current, total int,
) ([]Registration, error) {
	started := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "configure_repository", Outcome: "started", Repository: repository.URL, Current: current, Total: total})
	records, err := manager.ensureRepository(ctx, repository, config)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "configure_repository", Outcome: outcome, Repository: repository.URL, Current: current, Total: total,
		Count: len(records), Duration: manager.observer.currentTime().Sub(started)})
	return records, err
}

func discoveredInstallationCount(result transport.DiscoveryResult) int {
	installations := make(map[transport.InstallationID]struct{}, len(result.Repositories)+len(result.InstallationFailures))
	for _, repository := range result.Repositories {
		installations[repository.InstallationID] = struct{}{}
	}
	for installationID := range result.InstallationFailures {
		installations[installationID] = struct{}{}
	}
	return len(installations)
}

func (manager *Manager) ensureRepository(
	ctx context.Context,
	repository transport.Repository,
	config ManagerConfig,
) ([]Registration, error) {
	if len(config.RunnerPools) == 0 {
		return nil, nil
	}
	client, err := manager.factory(ctx, repository)
	if err != nil {
		manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
			Stage: "create_repository_client", Outcome: "failed", Repository: repository.URL, Err: err})
		return nil, fmt.Errorf("create GitHub client for repository %q: %w", repository.URL, err)
	}
	records := make([]Registration, 0, len(config.RunnerPools))
	for index, runnerPool := range config.RunnerPools {
		record, ensureErr := manager.ensureRunnerPoolRecord(ctx, client, repository, runnerPool, index+1, len(config.RunnerPools))
		if ensureErr != nil {
			return nil, ensureErr
		}
		records = append(records, record)
	}
	return records, nil
}

func (manager *Manager) ensureRunnerPoolRecord(ctx context.Context, client ManagerClient, repository transport.Repository,
	runnerPool RunnerPoolConfig, current, total int,
) (Registration, error) {
	started := manager.observer.currentTime()
	manager.reportRunnerPoolActivity(repository, runnerPool.RunnerPool, transport.Registration{},
		current, total, "started", started, nil)
	registration, err := client.EnsureRunnerPool(ctx, string(runnerPool.RunnerPool), runnerPool.RunnerGroup)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.reportRunnerPoolActivity(repository, runnerPool.RunnerPool, registration,
		current, total, outcome, started, err)
	if err != nil {
		return Registration{}, fmt.Errorf("ensure Runner Pool %q for repository %q: %w", runnerPool.RunnerPool, repository.URL, err)
	}
	return Registration{RepositoryURL: repository.URL, RunnerPool: runnerPool.RunnerPool,
		InstallationID: repository.InstallationID, RunnerGroupID: registration.RunnerGroupID, ScaleSetID: registration.ID}, nil
}

func (manager *Manager) reportRunnerPoolActivity(repository transport.Repository, runnerPool core.RunnerPool,
	registration transport.Registration, current, total int, outcome string, started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "configure_runner_pool", Outcome: outcome, Repository: repository.URL, RunnerPool: runnerPool,
		InstallationID: int64(repository.InstallationID), RunnerGroupID: int64(registration.RunnerGroupID),
		ScaleSetID: int64(registration.ID),
		Current:    current, Total: total, Duration: duration, Err: err})
}

func (manager *Manager) replaceRegistrations(ctx context.Context, snapshot RegistrationSnapshot) error {
	if err := manager.registrations.ReplaceRegistrationSnapshot(ctx, snapshot); err != nil {
		return fmt.Errorf("replace GitHub registration snapshot: %w", err)
	}
	return nil
}

func validateManagerConfig(config ManagerConfig) error {
	seen := make(map[core.RunnerPool]struct{}, len(config.RunnerPools))
	for _, runnerPool := range config.RunnerPools {
		if runnerPool.RunnerPool == "" || runnerPool.RunnerGroup == "" {
			return errors.New("GitHub Runner Pool and runner group are required")
		}
		if _, duplicate := seen[runnerPool.RunnerPool]; duplicate {
			return fmt.Errorf("duplicate GitHub Runner Pool %q", runnerPool.RunnerPool)
		}
		seen[runnerPool.RunnerPool] = struct{}{}
	}
	return nil
}

func discoveryFailures(failures map[transport.InstallationID]error) error {
	installationIDs := make([]transport.InstallationID, 0, len(failures))
	for installationID := range failures {
		installationIDs = append(installationIDs, installationID)
	}
	sort.Slice(installationIDs, func(first, second int) bool { return installationIDs[first] < installationIDs[second] })
	var joined error
	for _, installationID := range installationIDs {
		joined = errors.Join(joined, fmt.Errorf("discover GitHub installation %d: %w", installationID, failures[installationID]))
	}
	return joined
}

// RefreshMembers replaces bootstrap routes from the persisted registration
// snapshot. No leader message session is needed on ordinary members.
func (manager *Manager) RefreshMembers(ctx context.Context) (resultErr error) {
	started := manager.observer.currentTime()
	routeCount := 0
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_routes",
		Stage: "replace_routes", Outcome: "started"})
	defer func() {
		outcome := "completed"
		if resultErr != nil {
			outcome = "failed"
		}
		manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_routes",
			Stage: "replace_routes", Outcome: outcome, Count: routeCount,
			Duration: manager.observer.currentTime().Sub(started), Err: resultErr})
	}()
	snapshot, err := manager.registrations.RegistrationSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("list GitHub registrations: %w", err)
	}
	routes, err := manager.buildMemberRoutes(ctx, snapshot.Active)
	if err != nil {
		return err
	}
	if err := manager.source.ReplaceRoutes(routes); err != nil {
		return fmt.Errorf("replace GitHub member routes: %w", err)
	}
	routeCount = len(routes)
	return nil
}

func (manager *Manager) buildMemberRoutes(ctx context.Context, registrations []Registration) ([]MemberRoute, error) {
	routes := make([]MemberRoute, 0, len(registrations))
	for index, record := range registrations {
		started := manager.observer.currentTime()
		manager.reportRouteActivity(record, index+1, len(registrations), "started", started, nil)
		repository := repositoryFromRegistration(record)
		client, clientErr := manager.factory(ctx, repository)
		if clientErr != nil {
			manager.reportRouteActivity(record, index+1, len(registrations), "failed", started, clientErr)
			return nil, fmt.Errorf("create GitHub member client for repository %q: %w", record.RepositoryURL, clientErr)
		}
		routes = append(routes, MemberRoute{
			Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
			Client: managedBootstrapClient{client: client}, Registration: transportRegistration(record),
		})
		manager.reportRouteActivity(record, index+1, len(registrations), "completed", started, nil)
	}
	return routes, nil
}

func (manager *Manager) reportRouteActivity(record Registration, current, total int, outcome string,
	started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_routes",
		Stage: "configure_route", Outcome: outcome, Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		Current: current, Total: total, Duration: duration, Err: err})
}
