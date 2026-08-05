package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
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

var (
	productionReadFile         = os.ReadFile
	productionHTTPTransport    = http.DefaultTransport
	newProductionDiscoverer    = transport.NewDiscoverer
	newProductionClient        = transport.NewClient
	newProductionManager       = githubsource.NewManager
	newProductionTicker        = time.NewTicker
	productionCurrentTime      = time.Now
	openProductionSQLite       = sqlitestate.Open
	closeProductionSQLite      = (*sqlitestate.Store).Close
	newProductionClusterStore  = cluster.NewStore
	migrateProductionCluster   = (*cluster.Store).Migrate
	openProductionClusterPool  = (*cluster.Store).OpenPool
	newProductionPGConnection  = postgresstate.NewPGXConnection
	newProductionPGStore       = postgresstate.NewConnectionStore
	closeProductionPool        = (*pgxpool.Pool).Close
	tryProductionLeader        = (*cluster.Store).TryLeader
	buildProductionProviderSet = buildProductionProviders
	productionMemberIdentifier = uuid.NewRandom
)

func buildProductionGeneration(
	ctx context.Context,
	configured config.Config,
	state *productionState,
	processGate *localLeadershipGate,
	report func(error),
) (*unifiedGeneration, error) {
	manager, err := buildProductionGitHubManager(configured, state.store)
	if err != nil {
		return nil, err
	}
	providers, err := buildProductionProviderSet(ctx, configured)
	if err != nil {
		return nil, err
	}
	return buildTimedProductionGeneration(configured, state, processGate, report, manager, providers, state.member)
}

func buildProductionGitHubManager(configured config.Config, store unifiedStore) (*githubsource.Manager, error) {
	credentials, err := configured.GitHub.AppCredentials(productionReadFile)
	if err != nil {
		return nil, fmt.Errorf("load GitHub App credentials: %w", err)
	}
	transportCredentials := transport.Credentials{AppID: credentials.AppID, PrivateKey: credentials.PrivateKey}
	discoverer, err := newProductionDiscoverer(transportCredentials, productionHTTPTransport)
	if err != nil {
		return nil, fmt.Errorf("create GitHub discoverer: %w", err)
	}

	manager, err := newProductionManager(
		discoverer,
		githubsource.ManagerClientFactoryFunc(func(
			clientContext context.Context,
			repository transport.Repository,
		) (githubsource.ManagerClient, error) {
			client, clientErr := newProductionClient(repository, transportCredentials, productionHTTPTransport)
			if clientErr != nil {
				return githubsource.ManagerClient{}, fmt.Errorf("create scale-set client: %w", clientErr)
			}
			return githubsource.NewTransportManagerClient(client), nil
		}),
		store,
	)
	if err != nil {
		return nil, fmt.Errorf("create GitHub lifecycle manager: %w", err)
	}
	return manager, nil
}

func buildTimedProductionGeneration(
	configured config.Config,
	state *productionState,
	processGate *localLeadershipGate,
	report func(error),
	manager *githubsource.Manager,
	providers map[core.RunnerPool]execution.ExecutionProvider,
	member core.MemberID,
) (*unifiedGeneration, error) {
	operatingSystemTicker := newProductionTicker(runtimeCycleInterval)
	ticker := productionCycleTicker{ticks: operatingSystemTicker.C, stop: operatingSystemTicker.Stop}
	generation, err := buildUnifiedGeneration(configured, unifiedBuilderDependencies{
		store: state.store, manager: manager, member: member, providers: providers,
		leadership: state.newLeadership(), currentTime: productionCurrentTime, memberTTL: runtimeMemberTTL,
		ticker: ticker, report: report,
		processGate: processGate,
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
	store         unifiedStore
	member        core.MemberID
	newLeadership func() leadershipRuntime
	closers       []runtimeCloser
}

func openProductionState(
	ctx context.Context,
	configured config.Config,
) (*productionState, error) {
	member, err := productionMemberIdentifier()
	if err != nil {
		return nil, fmt.Errorf("generate member identity: %w", err)
	}
	var state *productionState
	if configured.State.Postgres == nil {
		state, err = openProductionSQLiteState(ctx, configured.State.Path)
	} else {
		state, err = openProductionPostgreSQLState(ctx, *configured.State.Postgres)
	}
	if err != nil {
		return nil, err
	}
	state.member = member
	return state, nil
}

func openProductionSQLiteState(ctx context.Context, path string) (*productionState, error) {
	store, err := openProductionSQLite(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite state: %w", err)
	}
	closer := contextCloserFunc(func(context.Context) error { return closeProductionSQLite(store) })
	return &productionState{
		store: store, newLeadership: func() leadershipRuntime { return permanentLeadership{} },
		closers: []runtimeCloser{closer},
	}, nil
}

func openProductionPostgreSQLState(ctx context.Context, configured postgresconfig.Settings) (*productionState, error) {
	clusterStore := newProductionClusterStore(configured)
	if err := migrateProductionCluster(clusterStore, ctx); err != nil {
		return nil, fmt.Errorf("migrate PostgreSQL state: %w", err)
	}
	pool, err := openProductionClusterPool(clusterStore, ctx)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL state pool: %w", err)
	}
	store := newProductionPGStore(newProductionPGConnection(pool))
	closer := contextCloserFunc(func(context.Context) error { closeProductionPool(pool); return nil })
	return &productionState{
		store: store,
		newLeadership: func() leadershipRuntime {
			return newExclusiveLeadership(cluster.ErrNotLeader, func(leaderContext context.Context) (leaderLease, error) {
				return tryProductionLeader(clusterStore, leaderContext)
			})
		},
		closers: []runtimeCloser{closer},
	}, nil
}

func buildProductionProviders(
	ctx context.Context,
	configured config.Config,
) (map[core.RunnerPool]execution.ExecutionProvider, error) {
	return buildConfiguredProviders(ctx, configured, executionDependencies{
		dockerCommands: dockerprovider.NewExecCommandRunner(), libvirtCommands: libvirtprovider.NewExecCommandRunner(),
		readTemplate: productionReadFile,
	})
}

func buildConfiguredProviders(
	ctx context.Context,
	configured config.Config,
	dependencies executionDependencies,
) (map[core.RunnerPool]execution.ExecutionProvider, error) {
	providers := make(map[core.RunnerPool]execution.ExecutionProvider, len(configured.ScaleSets))
	for _, scaleSet := range configured.ScaleSets {
		provider, err := buildExecutionProvider(ctx, scaleSet, dependencies)
		if err != nil {
			return nil, err
		}
		providers[core.RunnerPool(scaleSet.Name)] = provider
	}
	return providers, nil
}
