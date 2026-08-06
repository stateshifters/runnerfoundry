package github

import (
	"context"
	"errors"
	"fmt"

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
	PruneRunnerPools  func(context.Context, string, []string, []transport.OwnedRunnerPool) error
}

type managedBootstrapClient struct{ client ManagerClient }

func (client managedBootstrapClient) GenerateBootstrap(
	ctx context.Context,
	registration transport.Registration,
	runnerName string,
) ([]byte, error) {
	return client.client.GenerateBootstrap(ctx, registration, runnerName)
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
	prune func(context.Context, string, []string, []transport.OwnedRunnerPool) error,
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
	prune func(context.Context, string, []string, []transport.OwnedRunnerPool) error,
) func(context.Context, string, []string, []transport.OwnedRunnerPool) error {
	return func(ctx context.Context, group string, desired []string, owned []transport.OwnedRunnerPool) error {
		if err := prune(ctx, group, desired, owned); err != nil {
			return fmt.Errorf("prune GitHub Runner Pools: %w", err)
		}
		return nil
	}
}

// Manager owns explicit GitHub discovery, registration, message-session, and
// member-route lifecycle operations. Callers own scheduling, leadership, and
// supervision; Manager starts no background work.
type Manager struct {
	discoverer    RepositoryDiscoverer
	factory       ManagerClientFactoryFunc
	registrations RegistrationRepository
	source        *Source
	leaders       []ManagedSession
	retiring      []ManagedSession
	members       []MemberRoute
}

type openedLeader struct {
	session ManagedSession
	adapter *Adapter
}

func NewManager(
	discoverer RepositoryDiscoverer,
	factory ManagerClientFactoryFunc,
	registrations RegistrationRepository,
) (*Manager, error) {
	if discoverer == nil || factory == nil || registrations == nil {
		return nil, errors.New("GitHub discoverer, client factory, and registration repository are required")
	}
	source := &Source{routes: make(map[routeKey]memberRoute)}
	return &Manager{
		discoverer: discoverer, factory: factory, registrations: registrations, source: source,
	}, nil
}

func (manager *Manager) Source() *Source {
	return manager.source
}

// Reconcile discovers repositories, ensures every configured Runner Pool,
// optionally prunes absent owned scale sets, and atomically persists the
// resulting registration records.
func (manager *Manager) Reconcile(ctx context.Context, config ManagerConfig) error {
	if err := validateManagerConfig(config); err != nil {
		return err
	}
	existing, err := manager.registrations.Registrations(ctx)
	if err != nil {
		return fmt.Errorf("load owned GitHub registrations: %w", err)
	}
	result, err := manager.discoverer.Discover(ctx)
	if err != nil {
		return fmt.Errorf("discover GitHub repositories: %w", err)
	}
	if failureErr := discoveryFailures(result.InstallationFailures); failureErr != nil {
		return failureErr
	}
	records, err := manager.ensureRegistrations(ctx, result.Repositories, existing, config)
	if err != nil {
		return err
	}
	if err := manager.replaceRegistrations(ctx, records); err != nil {
		return fmt.Errorf("persist GitHub registrations: %w", err)
	}
	return nil
}

func (manager *Manager) ensureRegistrations(
	ctx context.Context,
	repositories []transport.Repository,
	existing []Registration,
	config ManagerConfig,
) ([]Registration, error) {
	records := make([]Registration, 0, len(repositories)*len(config.RunnerPools))
	for _, repository := range repositories {
		repositoryRecords, err := manager.ensureRepository(ctx, repository, existing, config)
		if err != nil {
			return nil, err
		}
		records = append(records, repositoryRecords...)
	}
	return records, nil
}

func (manager *Manager) ensureRepository(
	ctx context.Context,
	repository transport.Repository,
	existing []Registration,
	config ManagerConfig,
) ([]Registration, error) {
	client, err := manager.factory(ctx, repository)
	if err != nil {
		return nil, fmt.Errorf("create GitHub client for repository %q: %w", repository.URL, err)
	}
	records := make([]Registration, 0, len(config.RunnerPools))
	for _, runnerPool := range config.RunnerPools {
		registration, ensureErr := client.EnsureRunnerPool(ctx, string(runnerPool.RunnerPool), runnerPool.RunnerGroup)
		if ensureErr != nil {
			return nil, fmt.Errorf("ensure Runner Pool %q for repository %q: %w",
				runnerPool.RunnerPool, repository.URL, ensureErr)
		}
		records = append(records, Registration{
			RepositoryURL: repository.URL, RunnerPool: runnerPool.RunnerPool,
			InstallationID: int64(repository.InstallationID), RunnerGroupID: int64(registration.RunnerGroupID),
			ScaleSetID: int64(registration.ID),
		})
	}
	if config.Prune {
		if err := pruneRepository(ctx, client, repository.URL, config.RunnerPools, existing); err != nil {
			return nil, fmt.Errorf("prune repository %q: %w", repository.URL, err)
		}
	}
	return records, nil
}

func (manager *Manager) replaceRegistrations(ctx context.Context, desired []Registration) error {
	if err := manager.registrations.ReplaceRegistrations(ctx, desired); err != nil {
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

func pruneRepository(
	ctx context.Context,
	client ManagerClient,
	repositoryURL string,
	configs []RunnerPoolConfig,
	existing []Registration,
) error {
	desiredByGroup := make(map[string][]string)
	for _, config := range configs {
		desiredByGroup[config.RunnerGroup] = append(desiredByGroup[config.RunnerGroup], string(config.RunnerPool))
	}
	ownedByGroup := make(map[int64][]transport.OwnedRunnerPool)
	for _, registration := range existing {
		if registration.RepositoryURL != repositoryURL {
			continue
		}
		ownedByGroup[registration.RunnerGroupID] = append(ownedByGroup[registration.RunnerGroupID], transport.OwnedRunnerPool{
			ID: int(registration.ScaleSetID), Name: string(registration.RunnerPool),
			RunnerGroupID: int(registration.RunnerGroupID),
		})
	}
	for runnerGroup, desired := range desiredByGroup {
		var owned []transport.OwnedRunnerPool
		for _, registrations := range ownedByGroup {
			owned = append(owned, registrations...)
		}
		if err := client.PruneRunnerPools(ctx, runnerGroup, desired, owned); err != nil {
			return err
		}
	}
	return nil
}

// RefreshMembers replaces bootstrap routes from the persisted registration
// snapshot. No leader message session is needed on ordinary members.
func (manager *Manager) RefreshMembers(ctx context.Context) error {
	records, err := manager.registrations.Registrations(ctx)
	if err != nil {
		return fmt.Errorf("list GitHub registrations: %w", err)
	}
	routes := make([]MemberRoute, 0, len(records))
	for _, record := range records {
		repository := repositoryFromRegistration(record)
		client, clientErr := manager.factory(ctx, repository)
		if clientErr != nil {
			return fmt.Errorf("create GitHub member client for repository %q: %w", record.RepositoryURL, clientErr)
		}
		routes = append(routes, MemberRoute{
			Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
			Client: managedBootstrapClient{client: client}, Registration: transportRegistration(record),
		})
	}
	if err := manager.source.ReplaceRoutes(routes); err != nil {
		return fmt.Errorf("replace GitHub member routes: %w", err)
	}
	manager.members = routes
	return nil
}

// OpenLeader opens one actions/scaleset message session per persisted
// registration and returns its polling adapter. Callers pass current shared
// capacity to each PollOnce call.
func (manager *Manager) OpenLeader(
	ctx context.Context,
	owner string,
	handler EventHandler,
) ([]*Adapter, error) {
	if owner == "" || handler == nil {
		return nil, errors.New("GitHub leader owner and event handler are required")
	}
	cleanupErr := manager.retryRetiring(ctx)
	records, err := manager.registrations.Registrations(ctx)
	if err != nil {
		return nil, errors.Join(cleanupErr, fmt.Errorf("list GitHub registrations: %w", err))
	}
	sessions, adapters, err := manager.openLeaderRecords(ctx, records, owner, handler)
	if err != nil {
		return nil, errors.Join(cleanupErr, err)
	}
	oldSessions := manager.leaders
	manager.leaders = sessions
	return adapters, errors.Join(cleanupErr, manager.retireSessions(ctx, oldSessions))
}

func (manager *Manager) openLeaderRecords(
	ctx context.Context,
	records []Registration,
	owner string,
	handler EventHandler,
) ([]ManagedSession, []*Adapter, error) {
	sessions := make([]ManagedSession, 0, len(records))
	adapters := make([]*Adapter, 0, len(records))
	for _, record := range records {
		opened, err := manager.openLeaderRecord(ctx, record, owner, handler)
		if err != nil {
			return nil, nil, manager.closeOpened(ctx, sessions, err)
		}
		sessions = append(sessions, opened.session)
		adapters = append(adapters, opened.adapter)
	}
	return sessions, adapters, nil
}

func (manager *Manager) openLeaderRecord(
	ctx context.Context,
	record Registration,
	owner string,
	handler EventHandler,
) (openedLeader, error) {
	client, err := manager.factory(ctx, repositoryFromRegistration(record))
	if err != nil {
		return openedLeader{}, err
	}
	session, err := client.OpenSession(ctx, transportRegistration(record), owner)
	if err != nil {
		return openedLeader{}, err
	}
	if session == nil {
		return openedLeader{}, errors.New("GitHub client returned nil message session")
	}
	return openedLeader{
		session: session,
		adapter: &Adapter{
			Client: session, Handler: handler, Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		},
	}, nil
}

func repositoryFromRegistration(record Registration) transport.Repository {
	return transport.Repository{InstallationID: transport.InstallationID(record.InstallationID), URL: record.RepositoryURL}
}

func transportRegistration(record Registration) transport.Registration {
	return transport.Registration{
		ID: int(record.ScaleSetID), Name: string(record.RunnerPool), RunnerGroupID: int(record.RunnerGroupID),
	}
}

func (manager *Manager) closeOpened(ctx context.Context, sessions []ManagedSession, cause error) error {
	return errors.Join(cause, manager.retireSessions(ctx, sessions))
}

// CloseLeader closes every actions/scaleset message session.
func (manager *Manager) CloseLeader(ctx context.Context) error {
	sessions := append(manager.retiring, manager.leaders...)
	manager.leaders = nil
	manager.retiring = nil
	return manager.retireSessions(ctx, sessions)
}

func (manager *Manager) retireSessions(ctx context.Context, sessions []ManagedSession) error {
	var joined error
	for _, session := range sessions {
		if err := session.Close(ctx); err != nil {
			manager.retiring = append(manager.retiring, session)
			joined = errors.Join(joined, fmt.Errorf("close GitHub message session: %w", err))
		}
	}
	return joined
}

func (manager *Manager) retryRetiring(ctx context.Context) error {
	sessions := manager.retiring
	manager.retiring = nil
	return manager.retireSessions(ctx, sessions)
}
