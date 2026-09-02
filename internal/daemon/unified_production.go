package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	libvirtprovider "github.com/stateshifters/runnerfoundry/internal/provider/libvirt"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubpostgres "github.com/stateshifters/runnerfoundry/internal/source/github/persistence/postgres"
	githubsqlite "github.com/stateshifters/runnerfoundry/internal/source/github/persistence/sqlite"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

const (
	runtimeCycleInterval = time.Second
	runtimeMemberTTL     = 30 * time.Second
)

type productionCycleTicker struct {
	ticks <-chan time.Time
	stop  func()
}

func (ticker productionCycleTicker) Ticks() <-chan time.Time { return ticker.ticks }
func (ticker productionCycleTicker) Stop()                   { ticker.stop() }

type contextCloserFunc func(context.Context) error

func (closer contextCloserFunc) Close(ctx context.Context) error { return closer(ctx) }

type productionDependencies struct {
	readFile                 config.ReadFile
	httpTransport            http.RoundTripper
	newDiscoverer            func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error)
	newClient                func(transport.Repository, transport.Credentials, http.RoundTripper) (*transport.Client, error)
	newManager               func(githubsource.RepositoryDiscoverer, githubsource.ManagerClientFactoryFunc, githubsource.RegistrationRepository, githubsource.ManagerObserver) (*githubsource.Manager, error)
	dockerCommands           dockerprovider.CommandRunner
	libvirtCommands          libvirtprovider.CommandRunner
	newTicker                func(time.Duration) cycleTicker
	currentTime              func() time.Time
	maintenanceLaneCompleted func(laneName string, ticker cycleTicker)
	openSQLite               func(context.Context, string, ...sqlitestate.SchemaExtension) (*sqlitestate.Store, error)
	closeSQLite              func(*sqlitestate.Store) error
	newSQLiteGitHubStore     func(sqlitestate.Executor) *githubsqlite.Store
	newConnections           func(postgresconfig.Settings) (*cluster.Connections, error)
	newMigrationCatalog      func() (*cluster.Catalog, error)
	newMigrator              func(*cluster.Connections, *cluster.Catalog) *cluster.Migrator
	migratePostgreSQL        func(*cluster.Migrator, context.Context) error
	openClusterPool          func(*cluster.Connections, context.Context) (*pgxpool.Pool, error)
	newPGConnection          func(*pgxpool.Pool) postgresstate.Connection
	newPGStore               func(postgresstate.Connection) *postgresstate.Store
	newPGGitHubStore         func(postgresstate.Connection) *githubpostgres.Store
	newQueueNotificationPool func(*pgxpool.Pool) queueNotificationPool
	closePool                func(*pgxpool.Pool)
	newLeaderElector         func(*cluster.Connections) *cluster.LeaderElector
	tryLeader                func(*cluster.LeaderElector, context.Context) (*cluster.LeaderLease, error)
	buildProviderSet         func(context.Context, config.Config, core.MemberID, productionDependencies) (map[core.RunnerPool]execution.ExecutionProvider, execution.AggregateRunnerInventory, error)
	readHostname             func() (string, error)
	logDestinationFactory    logDestinationFactory
	providerObserver         func(execution.ProviderActivity)
	openState                func(context.Context, config.Config, productionDependencies) (*productionState, error)
	buildGeneration          func(context.Context, config.Config, *productionState, *localLeadershipGate, func(error), productionDependencies) (*maintenanceGeneration, error)
}

func newProductionDependencies() productionDependencies {
	return productionDependencies{
		readFile: os.ReadFile, httpTransport: http.DefaultTransport,
		newDiscoverer: transport.NewDiscoverer, newClient: transport.NewClient, newManager: githubsource.NewManager,
		dockerCommands: dockerprovider.NewExecCommandRunner(), libvirtCommands: libvirtprovider.NewExecCommandRunner(),
		newTicker: func(duration time.Duration) cycleTicker {
			operatingSystemTicker := time.NewTicker(duration)
			return productionCycleTicker{ticks: operatingSystemTicker.C, stop: operatingSystemTicker.Stop}
		}, currentTime: time.Now,
		openSQLite: sqlitestate.Open, closeSQLite: (*sqlitestate.Store).Close,
		newSQLiteGitHubStore: githubsqlite.NewStore,
		newConnections:       cluster.NewConnections, newMigrationCatalog: newPostgreSQLMigrationCatalog,
		newMigrator: cluster.NewMigrator, migratePostgreSQL: (*cluster.Migrator).Up,
		openClusterPool: (*cluster.Connections).OpenPool, newPGConnection: postgresstate.NewPGXConnection,
		newPGStore: postgresstate.NewConnectionStore, newPGGitHubStore: githubpostgres.NewStore,
		newQueueNotificationPool: newPGXQueueNotificationPool,
		closePool:                (*pgxpool.Pool).Close,
		newLeaderElector:         cluster.NewLeaderElector, tryLeader: (*cluster.LeaderElector).TryAcquire,
		buildProviderSet: buildProductionProvidersWithDependencies,
		readHostname:     os.Hostname, logDestinationFactory: newProductionLogDestinationFactory(),
		openState: openProductionStateWithDependencies, buildGeneration: buildProductionGenerationWithDependencies,
	}
}

func buildProductionGenerationWithDependencies(
	ctx context.Context,
	configured config.Config,
	state *productionState,
	processGate *localLeadershipGate,
	report func(error),
	dependencies productionDependencies,
) (*maintenanceGeneration, error) {
	manager, err := buildProductionGitHubManagerWithDependencies(configured, state.registrationStore, report, dependencies)
	if err != nil {
		return nil, err
	}
	dependencies.providerObserver = newProviderObserver(report)
	providers, inventory, err := dependencies.buildProviderSet(ctx, configured, state.member, dependencies)
	if err != nil {
		return nil, err
	}
	return buildTimedProductionGeneration(
		configured, state, processGate, report,
		productionGenerationComponents{manager: manager, providers: providers, inventory: inventory, member: state.member},
		dependencies,
	)
}

func buildProductionGitHubManagerWithDependencies(
	configured config.Config,
	store githubsource.RegistrationRepository,
	report func(error),
	dependencies productionDependencies,
) (*githubsource.MultiAppCoordinator, error) {
	credentials, err := configured.GitHub.AppCredentials(dependencies.readFile)
	if err != nil {
		return nil, fmt.Errorf("load GitHub App credentials: %w", err)
	}
	managers, err := buildGitHubAppManagers(credentials, store, report, dependencies)
	if err != nil {
		return nil, err
	}
	coordinator, err := githubsource.NewMultiAppCoordinator(store, managers...)
	if err != nil {
		return nil, fmt.Errorf("create GitHub App coordinator: %w", err)
	}
	return coordinator, nil
}

func buildGitHubAppManagers(
	credentials []config.GitHubAppCredentials,
	store githubsource.RegistrationRepository,
	report func(error),
	dependencies productionDependencies,
) ([]*githubsource.Manager, error) {
	managers := make([]*githubsource.Manager, 0, len(credentials))
	for _, appCredentials := range credentials {
		transportCredentials := transport.Credentials{AppID: appCredentials.AppID, PrivateKey: appCredentials.PrivateKey}
		discoverer, discoverErr := dependencies.newDiscoverer(transportCredentials, dependencies.httpTransport)
		if discoverErr != nil {
			return nil, fmt.Errorf("create GitHub discoverer for App %d: %w", appCredentials.AppID, discoverErr)
		}
		scopedStore, scopeErr := githubsource.ScopeRegistrationRepository(store, githubsource.AppID(appCredentials.AppID))
		if scopeErr != nil {
			return nil, fmt.Errorf("scope GitHub state for App %d: %w", appCredentials.AppID, scopeErr)
		}
		manager, managerErr := dependencies.newManager(
			discoverer,
			productionManagerClientFactory(transportCredentials, dependencies),
			scopedStore,
			newGitHubManagerObserverForApp(report, dependencies.currentTime, githubsource.AppID(appCredentials.AppID)),
		)
		if managerErr != nil {
			return nil, fmt.Errorf("create GitHub lifecycle manager for App %d: %w", appCredentials.AppID, managerErr)
		}
		managers = append(managers, manager)
	}
	return managers, nil
}

func productionManagerClientFactory(
	credentials transport.Credentials,
	dependencies productionDependencies,
) githubsource.ManagerClientFactoryFunc {
	return func(_ context.Context, repository transport.Repository) (githubsource.ManagerClient, error) {
		client, err := dependencies.newClient(repository, credentials, dependencies.httpTransport)
		if err != nil {
			return githubsource.ManagerClient{}, fmt.Errorf("create scale-set client: %w", err)
		}
		return githubsource.NewTransportManagerClient(client), nil
	}
}

type productionGenerationComponents struct {
	manager   githubManagerComposition
	providers map[core.RunnerPool]execution.ExecutionProvider
	inventory execution.RunnerInventory
	member    core.MemberID
}

func buildTimedProductionGeneration(
	configured config.Config,
	state *productionState,
	processGate *localLeadershipGate,
	report func(error),
	components productionGenerationComponents,
	dependencies productionDependencies,
) (*maintenanceGeneration, error) {
	tickers := newProductionMaintenanceLaneTickers(dependencies.newTicker)
	generation, err := buildUnifiedGeneration(configured, unifiedBuilderDependencies{
		store: state.store, demandStore: state.demandStore, manager: components.manager, member: components.member,
		providers: components.providers, inventory: components.inventory,
		leadership: state.newLeadership(), currentTime: dependencies.currentTime, memberTTL: runtimeMemberTTL,
		tickers: &tickers, report: report,
		processGate: processGate, queueNotifications: state.queueNotifications,
	})
	if err != nil {
		stopMaintenanceLaneTickers(tickers)
		return nil, err
	}
	generation.maintenanceLaneCompleted = dependencies.maintenanceLaneCompleted
	return generation, nil
}

func newProductionMaintenanceLaneTickers(newTicker func(time.Duration) cycleTicker) maintenanceLaneTickers {
	build := func() cycleTicker {
		return newTicker(runtimeCycleInterval)
	}
	return maintenanceLaneTickers{membership: build(), source: build(), execution: build()}
}

func stopMaintenanceLaneTickers(tickers maintenanceLaneTickers) {
	tickers.membership.Stop()
	tickers.source.Stop()
	tickers.execution.Stop()
}

func closeRuntimeResources(ctx context.Context, closers []runtimeCloser) error {
	var result error
	for index := len(closers) - 1; index >= 0; index-- {
		if err := closers[index].Close(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

type productionState struct {
	store              unifiedStore
	registrationStore  githubsource.RegistrationRepository
	demandStore        githubsource.AssignedDemandRepository
	member             core.MemberID
	newLeadership      func() leadershipRuntime
	closers            []runtimeCloser
	queueNotifications *queueNotificationPool
}

func openProductionStateWithDependencies(
	ctx context.Context,
	configured config.Config,
	dependencies productionDependencies,
) (*productionState, error) {
	member, err := productionMemberIdentity(dependencies.readHostname)
	if err != nil {
		return nil, err
	}
	var state *productionState
	if configured.State.Postgres == nil {
		state, err = openProductionSQLiteStateWithDependencies(ctx, configured.State.Path, dependencies)
	} else {
		state, err = openProductionPostgreSQLStateWithDependencies(ctx, *configured.State.Postgres, dependencies)
	}
	if err != nil {
		return nil, err
	}
	state.member = member
	return state, nil
}

func productionMemberIdentity(readHostname func() (string, error)) (core.MemberID, error) {
	if readHostname == nil {
		return "", errors.New("member hostname reader is required")
	}
	hostname, err := readHostname()
	if err != nil {
		return "", fmt.Errorf("read member hostname: %w", err)
	}
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", errors.New("member hostname is empty")
	}
	return core.MemberID(hostname), nil
}

func openProductionSQLiteStateWithDependencies(
	ctx context.Context,
	path string,
	dependencies productionDependencies,
) (*productionState, error) {
	store, err := dependencies.openSQLite(ctx, path, githubsqlite.Schema())
	if err != nil {
		return nil, fmt.Errorf("open SQLite state: %w", err)
	}
	closer := contextCloserFunc(func(context.Context) error { return dependencies.closeSQLite(store) })
	githubStore := dependencies.newSQLiteGitHubStore(store.Backend())
	return &productionState{
		store: store, registrationStore: githubStore, demandStore: githubStore,
		newLeadership: func() leadershipRuntime { return permanentLeadership{} },
		closers:       []runtimeCloser{closer},
	}, nil
}

func openProductionPostgreSQLStateWithDependencies(
	ctx context.Context,
	configured postgresconfig.Settings,
	dependencies productionDependencies,
) (*productionState, error) {
	connections, pool, err := openProductionPostgreSQLPool(ctx, configured, dependencies)
	if err != nil {
		return nil, err
	}
	connection := dependencies.newPGConnection(pool)
	store := dependencies.newPGStore(connection)
	githubStore := dependencies.newPGGitHubStore(connection)
	closer := contextCloserFunc(func(context.Context) error { dependencies.closePool(pool); return nil })
	queueNotifications := dependencies.newQueueNotificationPool(pool)
	elector := dependencies.newLeaderElector(connections)
	return &productionState{
		store: store, registrationStore: githubStore, demandStore: githubStore,
		queueNotifications: &queueNotifications,
		newLeadership: func() leadershipRuntime {
			return newExclusiveLeadership(cluster.ErrNotLeader, func(leaderContext context.Context) (leaderLease, error) {
				return dependencies.tryLeader(elector, leaderContext)
			})
		},
		closers: []runtimeCloser{closer},
	}, nil
}

func openProductionPostgreSQLPool(
	ctx context.Context,
	configured postgresconfig.Settings,
	dependencies productionDependencies,
) (*cluster.Connections, *pgxpool.Pool, error) {
	connections, err := dependencies.newConnections(configured)
	if err != nil {
		return nil, nil, fmt.Errorf("configure PostgreSQL connections: %w", err)
	}
	catalog, err := dependencies.newMigrationCatalog()
	if err != nil {
		return nil, nil, fmt.Errorf("compose PostgreSQL migrations: %w", err)
	}
	migrator := dependencies.newMigrator(connections, catalog)
	if err = dependencies.migratePostgreSQL(migrator, ctx); err != nil {
		return nil, nil, fmt.Errorf("migrate PostgreSQL state: %w", err)
	}
	pool, err := dependencies.openClusterPool(connections, ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open PostgreSQL state pool: %w", err)
	}
	return connections, pool, nil
}

func buildProductionProvidersWithDependencies(
	ctx context.Context,
	configured config.Config,
	member core.MemberID,
	dependencies productionDependencies,
) (map[core.RunnerPool]execution.ExecutionProvider, execution.AggregateRunnerInventory, error) {
	hostname := strings.TrimSpace(string(member))
	if len(configured.RunnerPools) != 0 && hostname == "" {
		return nil, execution.AggregateRunnerInventory{}, errors.New("provider member identity is empty")
	}
	return buildConfiguredProviders(ctx, configured, executionDependencies{
		dockerCommands: dependencies.dockerCommands, libvirtCommands: dependencies.libvirtCommands,
		readTemplate: dependencies.readFile, memberID: core.MemberID(hostname),
		providerObserver: dependencies.providerObserver,
	})
}

func buildConfiguredProviders(
	ctx context.Context,
	configured config.Config,
	dependencies executionDependencies,
) (map[core.RunnerPool]execution.ExecutionProvider, execution.AggregateRunnerInventory, error) {
	providers := make(map[core.RunnerPool]execution.ExecutionProvider, len(configured.RunnerPools))
	inventories := make([]execution.RunnerInventory, 0, len(configured.RunnerPools))
	for _, runnerPoolConfig := range configured.RunnerPools {
		provider, err := buildExecutionProvider(ctx, runnerPoolConfig, dependencies)
		if err != nil {
			return nil, execution.AggregateRunnerInventory{}, err
		}
		providers[core.RunnerPool(runnerPoolConfig.Name)] = provider
		inventories = append(inventories, provider)
	}
	inventory := execution.NewAggregateRunnerInventory(inventories...)
	return providers, inventory, nil
}
