package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

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

type ManagerClientFactoryFunc func(context.Context, transport.Repository) (ManagerClient, error)

// TransportManagerClient adapts the concrete actions/scaleset transport client
// to the lifecycle manager's narrow testable boundary.
func NewTransportManagerClient(client *transport.Client) ManagerClient {
	return composeTransportManagerClient(
		client.EnsureRunnerPool,
		client.OpenSession,
		client.GenerateBootstrap,
		client.PruneRunnerPools,
	)
}

func composeTransportManagerClient(
	ensure func(context.Context, string, string) (transport.Registration, error),
	open func(context.Context, transport.Registration, string) (*transport.Session, error),
	generate func(context.Context, transport.Registration, string) ([]byte, error),
	prune func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error),
) ManagerClient {
	return ManagerClient{
		EnsureRunnerPool:  wrapTransportEnsure(ensure),
		OpenSession:       wrapTransportOpen(open),
		GenerateBootstrap: wrapTransportGenerate(generate),
		PruneRunnerPools:  wrapTransportPrune(prune),
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
	leaders       []ManagedSession
	retiring      []ManagedSession
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

// Reconcile discovers repositories, ensures every configured Runner Pool,
// optionally prunes absent owned scale sets, and atomically persists the
// resulting registration records.
func (manager *Manager) Reconcile(ctx context.Context, config ManagerConfig) (resultErr error) {
	reconciliationStarted := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "reconciliation", Outcome: "started"})
	defer func() {
		outcome := "completed"
		if resultErr != nil {
			outcome = "failed"
		}
		manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
			Stage: "reconciliation", Outcome: outcome,
			Duration: manager.observer.currentTime().Sub(reconciliationStarted), Err: resultErr})
	}()
	if err := validateManagerConfig(config); err != nil {
		return err
	}
	return manager.reconcileConfigured(ctx, config)
}

func (manager *Manager) reconcileConfigured(ctx context.Context, config ManagerConfig) error {
	previous, err := manager.loadRegistrationSnapshot(ctx)
	if err != nil {
		return err
	}
	result, err := manager.discoverRepositories(ctx)
	if err != nil {
		return err
	}
	if failureErr := discoveryFailures(result.InstallationFailures); failureErr != nil {
		return failureErr
	}
	active, err := manager.ensureRegistrations(ctx, result.Repositories, config)
	if err != nil {
		return err
	}
	owned := mergeOwnedRegistrations(previous.Owned, active)
	if config.Prune {
		owned, err = manager.pruneOwnedRegistrations(ctx, result.Repositories, active, owned)
		if err != nil {
			return err
		}
	}
	return manager.persistRegistrationSnapshot(ctx, active, owned)
}

func (manager *Manager) loadRegistrationSnapshot(ctx context.Context) (RegistrationSnapshot, error) {
	started := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "load_registration_snapshot", Outcome: "started"})
	snapshot, err := manager.registrations.RegistrationSnapshot(ctx)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "load_registration_snapshot", Outcome: outcome, Count: len(snapshot.Active), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("load owned GitHub registrations: %w", err)
	}
	return snapshot, nil
}

func (manager *Manager) discoverRepositories(ctx context.Context) (transport.DiscoveryResult, error) {
	started := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "discover_repositories", Outcome: "started"})
	result, err := manager.discoverer.Discover(ctx)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
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
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "prune_registrations", Outcome: "started", Total: len(owned)})
	pruned, err := manager.pruneRegistrations(ctx, repositories, active, owned)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "prune_registrations", Outcome: outcome, Count: len(pruned), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	return pruned, err
}

func (manager *Manager) persistRegistrationSnapshot(ctx context.Context, active, owned []Registration) error {
	started := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "persist_registrations", Outcome: "started", Total: len(active)})
	err := manager.replaceRegistrations(ctx, RegistrationSnapshot{Active: active, Owned: owned})
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "persist_registrations", Outcome: outcome, Count: len(active), Err: err,
		Duration: manager.observer.currentTime().Sub(started)})
	if err != nil {
		return fmt.Errorf("persist GitHub registrations: %w", err)
	}
	return nil
}

func (manager *Manager) ensureRegistrations(
	ctx context.Context,
	repositories []transport.Repository,
	config ManagerConfig,
) ([]Registration, error) {
	records := make([]Registration, 0, len(repositories)*len(config.RunnerPools))
	total := len(repositories)
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "configure_repositories", Outcome: "started", Total: total})
	for index, repository := range repositories {
		current := index + 1
		manager.reportRepositoryProgress(current, total)
		repositoryRecords, err := manager.ensureRepositoryWithActivity(ctx, repository, config, current, total)
		if err != nil {
			manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
				Stage: "configure_repositories", Outcome: "failed", Current: current, Total: total, Err: err})
			return nil, err
		}
		records = append(records, repositoryRecords...)
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "configure_repositories", Outcome: "completed", Count: len(records), Total: total})
	return records, nil
}

func (manager *Manager) reportRepositoryProgress(current, total int) {
	if shouldReportManagerProgress(current, total) {
		manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
			Stage: "configure_repositories", Outcome: "progress", Current: current, Total: total})
	}
}

func (manager *Manager) ensureRepositoryWithActivity(ctx context.Context, repository transport.Repository,
	config ManagerConfig, current, total int,
) ([]Registration, error) {
	started := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "configure_repository", Outcome: "started", Repository: repository.URL, Current: current, Total: total})
	records, err := manager.ensureRepository(ctx, repository, config)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "configure_repository", Outcome: outcome, Repository: repository.URL, Current: current, Total: total,
		Count: len(records), Duration: manager.observer.currentTime().Sub(started), Err: err})
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
	manager.reportRunnerPoolActivity(repository.URL, runnerPool.RunnerPool, current, total, "started", started, nil)
	registration, err := client.EnsureRunnerPool(ctx, string(runnerPool.RunnerPool), runnerPool.RunnerGroup)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.reportRunnerPoolActivity(repository.URL, runnerPool.RunnerPool, current, total, outcome, started, err)
	if err != nil {
		return Registration{}, fmt.Errorf("ensure Runner Pool %q for repository %q: %w", runnerPool.RunnerPool, repository.URL, err)
	}
	return Registration{RepositoryURL: repository.URL, RunnerPool: runnerPool.RunnerPool,
		InstallationID: repository.InstallationID, RunnerGroupID: registration.RunnerGroupID, ScaleSetID: registration.ID}, nil
}

func (manager *Manager) reportRunnerPoolActivity(repository string, runnerPool core.RunnerPool, current, total int,
	outcome string, started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "configure_runner_pool", Outcome: outcome, Repository: repository, RunnerPool: runnerPool,
		Current: current, Total: total, Duration: duration, Err: err})
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
	var joined error
	for installationID, err := range failures {
		joined = errors.Join(joined, fmt.Errorf("discover GitHub installation %d: %w", installationID, err))
	}
	return joined
}

// RefreshMembers replaces bootstrap routes from the persisted registration
// snapshot. No leader message session is needed on ordinary members.
func (manager *Manager) RefreshMembers(ctx context.Context) (resultErr error) {
	started := manager.observer.currentTime()
	routeCount := 0
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_routes",
		Stage: "replace_routes", Outcome: "started"})
	defer func() {
		outcome := "completed"
		if resultErr != nil {
			outcome = "failed"
		}
		manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_routes",
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
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_routes",
		Stage: "configure_route", Outcome: outcome, Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		Current: current, Total: total, Duration: duration, Err: err})
}
