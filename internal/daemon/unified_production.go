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
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	dockerprovider "github.com/meza/RunnerFoundry/internal/provider/docker"
	libvirtprovider "github.com/meza/RunnerFoundry/internal/provider/libvirt"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
	postgresstate "github.com/meza/RunnerFoundry/internal/state/postgres"
	sqlitestate "github.com/meza/RunnerFoundry/internal/state/sqlite"
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
	newTicker                func(time.Duration) *time.Ticker
	currentTime              func() time.Time
	openSQLite               func(context.Context, string) (*sqlitestate.Store, error)
	closeSQLite              func(*sqlitestate.Store) error
	newClusterStore          func(postgresconfig.Settings) *cluster.Store
	migrateCluster           func(*cluster.Store, context.Context) error
	openClusterPool          func(*cluster.Store, context.Context) (*pgxpool.Pool, error)
	newPGConnection          func(*pgxpool.Pool) postgresstate.Connection
	newPGStore               func(postgresstate.Connection) *postgresstate.Store
	newQueueNotificationPool func(*pgxpool.Pool) queueNotificationPool
	closePool                func(*pgxpool.Pool)
	tryLeader                func(*cluster.Store, context.Context) (*cluster.LeaderLease, error)
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
		newTicker: time.NewTicker, currentTime: time.Now,
		openSQLite: sqlitestate.Open, closeSQLite: (*sqlitestate.Store).Close,
		newClusterStore: cluster.NewStore, migrateCluster: (*cluster.Store).Migrate,
		openClusterPool: (*cluster.Store).OpenPool, newPGConnection: postgresstate.NewPGXConnection,
		newPGStore: postgresstate.NewConnectionStore, newQueueNotificationPool: newPGXQueueNotificationPool,
		closePool: (*pgxpool.Pool).Close,
		tryLeader: (*cluster.Store).TryLeader, buildProviderSet: buildProductionProvidersWithDependencies,
		readHostname: os.Hostname, logDestinationFactory: newProductionLogDestinationFactory(),
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
	manager, err := buildProductionGitHubManagerWithDependencies(configured, state.store, report, dependencies)
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
	store unifiedStore,
	report func(error),
	dependencies productionDependencies,
) (*githubsource.Manager, error) {
	credentials, err := configured.GitHub.AppCredentials(dependencies.readFile)
	if err != nil {
		return nil, fmt.Errorf("load GitHub App credentials: %w", err)
	}
	transportCredentials := transport.Credentials{AppID: credentials.AppID, PrivateKey: credentials.PrivateKey}
	discoverer, err := dependencies.newDiscoverer(transportCredentials, dependencies.httpTransport)
	if err != nil {
		return nil, fmt.Errorf("create GitHub discoverer: %w", err)
	}

	manager, err := dependencies.newManager(
		discoverer,
		productionManagerClientFactory(transportCredentials, dependencies),
		store,
		newGitHubManagerObserver(report, dependencies.currentTime),
	)
	if err != nil {
		return nil, fmt.Errorf("create GitHub lifecycle manager: %w", err)
	}
	return manager, nil
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
	manager   *githubsource.Manager
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
	operatingSystemTicker := dependencies.newTicker(runtimeCycleInterval)
	ticker := productionCycleTicker{ticks: operatingSystemTicker.C, stop: operatingSystemTicker.Stop}
	generation, err := buildUnifiedGeneration(configured, unifiedBuilderDependencies{
		store: state.store, demandStore: state.demandStore, manager: components.manager, member: components.member,
		providers: components.providers, inventory: components.inventory,
		leadership: state.newLeadership(), currentTime: dependencies.currentTime, memberTTL: runtimeMemberTTL,
		ticker: ticker, report: report,
		processGate: processGate, queueNotifications: state.queueNotifications,
	})
	if err != nil {
		ticker.Stop()
		return nil, err
	}
	return generation, nil
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
	store, err := dependencies.openSQLite(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite state: %w", err)
	}
	closer := contextCloserFunc(func(context.Context) error { return dependencies.closeSQLite(store) })
	return &productionState{
		store: store, demandStore: store, newLeadership: func() leadershipRuntime { return permanentLeadership{} },
		closers: []runtimeCloser{closer},
	}, nil
}

func openProductionPostgreSQLStateWithDependencies(
	ctx context.Context,
	configured postgresconfig.Settings,
	dependencies productionDependencies,
) (*productionState, error) {
	clusterStore := dependencies.newClusterStore(configured)
	if err := dependencies.migrateCluster(clusterStore, ctx); err != nil {
		return nil, fmt.Errorf("migrate PostgreSQL state: %w", err)
	}
	pool, err := dependencies.openClusterPool(clusterStore, ctx)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL state pool: %w", err)
	}
	store := dependencies.newPGStore(dependencies.newPGConnection(pool))
	closer := contextCloserFunc(func(context.Context) error { dependencies.closePool(pool); return nil })
	queueNotifications := dependencies.newQueueNotificationPool(pool)
	return &productionState{
		store: store, demandStore: store,
		queueNotifications: &queueNotifications,
		newLeadership: func() leadershipRuntime {
			return newExclusiveLeadership(cluster.ErrNotLeader, func(leaderContext context.Context) (leaderLease, error) {
				return dependencies.tryLeader(clusterStore, leaderContext)
			})
		},
		closers: []runtimeCloser{closer},
	}, nil
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
		dockerCommands: dockerprovider.NewExecCommandRunner(), libvirtCommands: libvirtprovider.NewExecCommandRunner(),
		readTemplate: dependencies.readFile, memberHostname: hostname,
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
