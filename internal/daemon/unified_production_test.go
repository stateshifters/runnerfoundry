package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
	postgresstate "github.com/meza/RunnerFoundry/internal/state/postgres"
	sqlitestate "github.com/meza/RunnerFoundry/internal/state/sqlite"
)

type memoryLogFactory struct{}

func (memoryLogFactory) Open(config.LogConfig) (io.WriteCloser, error) {
	return noOpWriteCloser{writer: &bytes.Buffer{}}, nil
}

type failingLogFactory struct{ err error }

func (factory failingLogFactory) Open(config.LogConfig) (io.WriteCloser, error) {
	return nil, factory.err
}

func TestProductionAdaptersUseInjectedBoundaries(t *testing.T) {
	tickerStopped := false
	ticker := productionCycleTicker{ticks: make(chan time.Time), stop: func() { tickerStopped = true }}
	if ticker.Ticks() == nil {
		t.Fatal("ticker channel is nil")
	}
	ticker.Stop()
	if !tickerStopped {
		t.Fatal("ticker was not stopped")
	}

	closed := false
	closer := contextCloserFunc(func(context.Context) error { closed = true; return nil })
	if err := closer.Close(context.Background()); err != nil || !closed {
		t.Fatalf("context closer = (%t, %v)", closed, err)
	}
}

func TestProductionStateUsesTrimmedHostnameIdentity(t *testing.T) {
	restore := replaceProductionStateBoundaries()
	t.Cleanup(restore)
	calls := 0
	productionReadHostname = func() (string, error) {
		calls++
		return "  runner-host-7\n", nil
	}

	state, err := openProductionState(context.Background(), config.Config{State: config.StateConfig{Path: "state.db"}})
	if err != nil || state.member != daemonTestID(7) || calls != 1 {
		t.Fatalf("production member identity = (%s, %d, %v)", state.member, calls, err)
	}
}

func TestProductionStateCompositionUsesSelectedBackend(t *testing.T) {
	restore := replaceProductionStateBoundaries()
	t.Cleanup(restore)
	assertProductionSQLiteState(t)
	assertProductionPostgreSQLState(t)
}

func assertProductionSQLiteState(t *testing.T) {
	t.Helper()
	sqliteState, err := openProductionState(context.Background(), config.Config{State: config.StateConfig{Path: "state.db"}})
	if err != nil || sqliteState == nil || len(sqliteState.closers) != 1 {
		t.Fatalf("SQLite state = (%v, %v)", sqliteState, err)
	}
	if err = sqliteState.closers[0].Close(context.Background()); err != nil {
		t.Fatalf("close SQLite state: %v", err)
	}
	if leader, leaderErr := sqliteState.newLeadership().IsLeader(context.Background()); leaderErr != nil || !leader {
		t.Fatalf("SQLite leadership = (%t, %v)", leader, leaderErr)
	}
}

func assertProductionPostgreSQLState(t *testing.T) {
	t.Helper()
	settings := postgresconfig.Settings{Role: "runnerfoundry"}
	postgresState, err := openProductionState(
		context.Background(), config.Config{State: config.StateConfig{Postgres: &settings}},
	)
	if err != nil || postgresState == nil || len(postgresState.closers) != 1 {
		t.Fatalf("PostgreSQL state = (%v, %v)", postgresState, err)
	}
	if err = postgresState.closers[0].Close(context.Background()); err != nil {
		t.Fatalf("close PostgreSQL state: %v", err)
	}
	tryProductionLeader = func(*cluster.Store, context.Context) (*cluster.LeaderLease, error) {
		return nil, cluster.ErrNotLeader
	}
	if leader, leaderErr := postgresState.newLeadership().IsLeader(context.Background()); leaderErr != nil || leader {
		t.Fatalf("PostgreSQL leadership = (%t, %v)", leader, leaderErr)
	}
}

func TestProductionStateReportsBackendFailures(t *testing.T) {
	restore := replaceProductionStateBoundaries()
	t.Cleanup(restore)
	wantErr := errors.New("state failed")
	openProductionSQLite = func(context.Context, string) (*sqlitestate.Store, error) { return nil, wantErr }
	if _, err := openProductionState(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("selected backend error = %v", err)
	}
	if _, err := openProductionSQLiteState(context.Background(), "state.db"); !errors.Is(err, wantErr) {
		t.Fatalf("SQLite error = %v", err)
	}
	settings := postgresconfig.Settings{Role: "runnerfoundry"}
	migrateProductionCluster = func(*cluster.Store, context.Context) error { return wantErr }
	if _, err := openProductionPostgreSQLState(context.Background(), settings); !errors.Is(err, wantErr) {
		t.Fatalf("PostgreSQL migration error = %v", err)
	}
	migrateProductionCluster = func(*cluster.Store, context.Context) error { return nil }
	openProductionClusterPool = func(*cluster.Store, context.Context) (*pgxpool.Pool, error) { return nil, wantErr }
	if _, err := openProductionPostgreSQLState(context.Background(), settings); !errors.Is(err, wantErr) {
		t.Fatalf("PostgreSQL pool error = %v", err)
	}
}

func TestProductionStateReportsMemberIdentityFailure(t *testing.T) {
	restore := replaceProductionStateBoundaries()
	t.Cleanup(restore)
	wantErr := errors.New("identity unavailable")
	productionReadHostname = func() (string, error) { return "", wantErr }
	if _, err := openProductionState(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("member identity error = %v", err)
	}
	productionReadHostname = func() (string, error) { return " \n", nil }
	if _, err := openProductionState(context.Background(), config.Config{}); err == nil {
		t.Fatal("empty member hostname was accepted")
	}
}

func TestProductionMemberIdentityRequiresHostnameReader(t *testing.T) {
	t.Parallel()

	if _, err := productionMemberIdentity(nil); err == nil {
		t.Fatal("nil hostname reader was accepted")
	}
}

func replaceProductionStateBoundaries() func() {
	originalSQLite, originalSQLiteClose := openProductionSQLite, closeProductionSQLite
	originalCluster, originalMigrate := newProductionClusterStore, migrateProductionCluster
	originalPool, originalConnection, originalStore := openProductionClusterPool, newProductionPGConnection, newProductionPGStore
	originalPoolClose, originalLeader := closeProductionPool, tryProductionLeader
	originalReadHostname := productionReadHostname
	productionReadHostname = func() (string, error) { return string(daemonTestID(1)), nil }
	openProductionSQLite = func(context.Context, string) (*sqlitestate.Store, error) { return &sqlitestate.Store{}, nil }
	closeProductionSQLite = func(*sqlitestate.Store) error { return nil }
	newProductionClusterStore = func(settings postgresconfig.Settings) *cluster.Store { return cluster.NewStore(settings) }
	migrateProductionCluster = func(*cluster.Store, context.Context) error { return nil }
	openProductionClusterPool = func(*cluster.Store, context.Context) (*pgxpool.Pool, error) { return &pgxpool.Pool{}, nil }
	newProductionPGConnection = func(*pgxpool.Pool) postgresstate.Connection { return postgresstate.Connection{} }
	newProductionPGStore = func(postgresstate.Connection) *postgresstate.Store { return &postgresstate.Store{} }
	closeProductionPool = func(*pgxpool.Pool) {}
	tryProductionLeader = func(*cluster.Store, context.Context) (*cluster.LeaderLease, error) {
		return &cluster.LeaderLease{}, nil
	}
	return func() {
		openProductionSQLite, closeProductionSQLite = originalSQLite, originalSQLiteClose
		newProductionClusterStore, migrateProductionCluster = originalCluster, originalMigrate
		openProductionClusterPool, newProductionPGConnection, newProductionPGStore = originalPool, originalConnection, originalStore
		closeProductionPool, tryProductionLeader = originalPoolClose, originalLeader
		productionReadHostname = originalReadHostname
	}
}

func TestProductionGitHubManagerUsesTransportFactories(t *testing.T) {
	originalDiscoverer, originalClient, originalManager := newProductionDiscoverer, newProductionClient, newProductionManager
	t.Cleanup(func() {
		newProductionDiscoverer, newProductionClient, newProductionManager = originalDiscoverer, originalClient, originalManager
	})
	newProductionDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	newProductionClient = func(transport.Repository, transport.Credentials, http.RoundTripper) (*transport.Client, error) {
		return &transport.Client{}, nil
	}
	store := newBuilderStoreFake()
	manager := newBuilderManager(t, store)
	newProductionManager = func(
		_ githubsource.RepositoryDiscoverer,
		factory githubsource.ManagerClientFactoryFunc,
		_ githubsource.RegistrationRepository,
	) (*githubsource.Manager, error) {
		if _, err := factory(context.Background(), transport.Repository{}); err != nil {
			return nil, err
		}
		return manager, nil
	}
	configured := builderConfig()
	configured.GitHub.App = config.GitHubAppConfig{AppID: 1, PrivateKey: "key"}
	built, err := buildProductionGitHubManager(configured, store)
	if err != nil || built != manager {
		t.Fatalf("GitHub manager = (%p, %v)", built, err)
	}
}

func TestProductionGitHubManagerReportsBoundaryFailures(t *testing.T) {
	originalDiscoverer, originalClient, originalManager := newProductionDiscoverer, newProductionClient, newProductionManager
	t.Cleanup(func() {
		newProductionDiscoverer, newProductionClient, newProductionManager = originalDiscoverer, originalClient, originalManager
	})
	wantErr := errors.New("GitHub failed")
	store := newBuilderStoreFake()
	configured := builderConfig()
	configured.GitHub.App = config.GitHubAppConfig{AppID: 1, PrivateKey: "key"}
	assertProductionDiscovererFailure(t, configured, store, wantErr)
	assertProductionClientFailure(t, configured, store, wantErr)
	assertProductionManagerFailure(t, configured, store, wantErr)
	configured.GitHub.App.PrivateKey = ""
	configured.GitHub.App.PrivateKeyFile = "key.pem"
	originalRead := productionReadFile
	productionReadFile = func(string) ([]byte, error) { return nil, wantErr }
	t.Cleanup(func() { productionReadFile = originalRead })
	if _, err := buildProductionGitHubManager(configured, store); !errors.Is(err, wantErr) {
		t.Fatalf("credentials error = %v", err)
	}
}

func assertProductionDiscovererFailure(
	t *testing.T,
	configured config.Config,
	store unifiedStore,
	wantErr error,
) {
	t.Helper()
	newProductionDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, wantErr
	}
	if _, err := buildProductionGitHubManager(configured, store); !errors.Is(err, wantErr) {
		t.Fatalf("discoverer error = %v", err)
	}
}

func assertProductionClientFailure(t *testing.T, configured config.Config, store unifiedStore, wantErr error) {
	t.Helper()
	newProductionDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	newProductionManager = func(
		_ githubsource.RepositoryDiscoverer,
		factory githubsource.ManagerClientFactoryFunc,
		_ githubsource.RegistrationRepository,
	) (*githubsource.Manager, error) {
		_, err := factory(context.Background(), transport.Repository{})
		return nil, err
	}
	newProductionClient = func(transport.Repository, transport.Credentials, http.RoundTripper) (*transport.Client, error) {
		return nil, wantErr
	}
	if _, err := buildProductionGitHubManager(configured, store); !errors.Is(err, wantErr) {
		t.Fatalf("client error = %v", err)
	}
}

func assertProductionManagerFailure(t *testing.T, configured config.Config, store unifiedStore, wantErr error) {
	t.Helper()
	newProductionManager = func(
		githubsource.RepositoryDiscoverer,
		githubsource.ManagerClientFactoryFunc,
		githubsource.RegistrationRepository,
	) (*githubsource.Manager, error) {
		return nil, wantErr
	}
	if _, err := buildProductionGitHubManager(configured, store); !errors.Is(err, wantErr) {
		t.Fatalf("manager error = %v", err)
	}
}

func TestTimedProductionGenerationBuildsWithInjectedTicker(t *testing.T) {
	originalTicker, originalClock := newProductionTicker, productionCurrentTime
	t.Cleanup(func() { newProductionTicker, productionCurrentTime = originalTicker, originalClock })
	newProductionTicker = func(time.Duration) *time.Ticker { return &time.Ticker{C: make(chan time.Time)} }
	productionCurrentTime = builderTime
	store := newBuilderStoreFake()
	state := &productionState{store: store, member: daemonTestID(1), newLeadership: func() leadershipRuntime { return permanentLeadership{} }}
	generation, err := buildTimedProductionGeneration(
		builderConfig(), state, &localLeadershipGate{}, func(error) {}, newBuilderManager(t, store),
		map[core.RunnerPool]execution.ExecutionProvider{"linux": builderProviderFake{}},
		builderProviderFake{}, daemonTestID(1),
	)
	if err != nil || generation == nil {
		t.Fatalf("timed generation = (%v, %v)", generation, err)
	}
}

func TestTimedProductionGenerationStopsTickerWhenCompositionFails(t *testing.T) {
	originalTicker := newProductionTicker
	t.Cleanup(func() { newProductionTicker = originalTicker })
	newProductionTicker = func(time.Duration) *time.Ticker { return &time.Ticker{} }
	store := newBuilderStoreFake()
	state := &productionState{store: store, member: daemonTestID(1), newLeadership: func() leadershipRuntime { return permanentLeadership{} }}
	if _, err := buildTimedProductionGeneration(
		config.Config{}, state, &localLeadershipGate{}, nil, newBuilderManager(t, store), nil, nil, daemonTestID(1),
	); err == nil {
		t.Fatal("invalid timed generation was accepted")
	}
}

func TestBuildProductionGenerationComposesInjectedBoundaries(t *testing.T) {
	originalProviders := buildProductionProviderSet
	originalTicker, originalClock := newProductionTicker, productionCurrentTime
	originalDiscoverer, originalManager := newProductionDiscoverer, newProductionManager
	t.Cleanup(func() {
		buildProductionProviderSet = originalProviders
		newProductionTicker, productionCurrentTime = originalTicker, originalClock
		newProductionDiscoverer, newProductionManager = originalDiscoverer, originalManager
	})
	store := newBuilderStoreFake()
	manager := newBuilderManager(t, store)
	configureProductionGenerationBoundaries(manager)
	configured := builderConfig()
	configured.GitHub.App = config.GitHubAppConfig{AppID: 1, PrivateKey: "key"}
	state := &productionState{store: store, member: daemonTestID(1), newLeadership: func() leadershipRuntime { return permanentLeadership{} }}
	generation, err := buildProductionGeneration(
		context.Background(), configured, state, &localLeadershipGate{}, func(error) {},
	)
	if err != nil || generation == nil {
		t.Fatalf("production generation = (%v, %v)", generation, err)
	}
}

func configureProductionGenerationBoundaries(manager *githubsource.Manager) {
	newProductionDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	newProductionManager = func(
		githubsource.RepositoryDiscoverer,
		githubsource.ManagerClientFactoryFunc,
		githubsource.RegistrationRepository,
	) (*githubsource.Manager, error) {
		return manager, nil
	}
	buildProductionProviderSet = func(
		context.Context,
		config.Config,
	) (map[core.RunnerPool]execution.ExecutionProvider, execution.AggregateRunnerInventory, error) {
		provider := builderProviderFake{}
		inventory := execution.NewAggregateRunnerInventory(provider)
		return map[core.RunnerPool]execution.ExecutionProvider{"linux": provider}, inventory, nil
	}
	newProductionTicker = func(time.Duration) *time.Ticker { return &time.Ticker{C: make(chan time.Time)} }
	productionCurrentTime = builderTime
}

func TestBuildProductionGenerationReportsCompositionFailures(t *testing.T) {
	fixture := newProductionGenerationFailureFixture(t)
	fixture.assertManagerFailure(t)
	fixture.assertProviderFailure(t)
}

type productionGenerationFailureFixture struct {
	configured config.Config
	state      *productionState
	manager    *githubsource.Manager
	wantErr    error
}

func newProductionGenerationFailureFixture(t *testing.T) productionGenerationFailureFixture {
	t.Helper()
	originalProviders := buildProductionProviderSet
	originalDiscoverer, originalManager := newProductionDiscoverer, newProductionManager
	t.Cleanup(func() {
		buildProductionProviderSet = originalProviders
		newProductionDiscoverer, newProductionManager = originalDiscoverer, originalManager
	})
	configured := builderConfig()
	configured.GitHub.App = config.GitHubAppConfig{AppID: 1, PrivateKey: "key"}
	store := newBuilderStoreFake()
	return productionGenerationFailureFixture{
		configured: configured, state: &productionState{store: store, member: daemonTestID(1)}, manager: newBuilderManager(t, store),
		wantErr: errors.New("composition failed"),
	}
}

func (fixture productionGenerationFailureFixture) assertManagerFailure(t *testing.T) {
	t.Helper()
	newProductionDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, fixture.wantErr
	}
	if _, err := fixture.build(); !errors.Is(err, fixture.wantErr) {
		t.Fatalf("manager composition error = %v", err)
	}
}

func (fixture productionGenerationFailureFixture) useManager() {
	newProductionDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	newProductionManager = func(
		githubsource.RepositoryDiscoverer,
		githubsource.ManagerClientFactoryFunc,
		githubsource.RegistrationRepository,
	) (*githubsource.Manager, error) {
		return fixture.manager, nil
	}
}

func (fixture productionGenerationFailureFixture) assertProviderFailure(t *testing.T) {
	t.Helper()
	fixture.useManager()
	buildProductionProviderSet = func(
		context.Context,
		config.Config,
	) (map[core.RunnerPool]execution.ExecutionProvider, execution.AggregateRunnerInventory, error) {
		return nil, execution.AggregateRunnerInventory{}, fixture.wantErr
	}
	if _, err := fixture.build(); !errors.Is(err, fixture.wantErr) {
		t.Fatalf("provider composition error = %v", err)
	}
}

func (fixture productionGenerationFailureFixture) build() (*unifiedGeneration, error) {
	return buildProductionGeneration(
		context.Background(), fixture.configured, fixture.state, &localLeadershipGate{}, nil,
	)
}

func TestOpenProductionRuntimeUsesLoggingAndStateBoundaries(t *testing.T) {
	originalLogging, originalState := newDaemonLoggingManager, openDaemonState
	t.Cleanup(func() { newDaemonLoggingManager, openDaemonState = originalLogging, originalState })
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	newDaemonLoggingManager = func(config.LogConfig, logDestinationFactory) (*loggingManager, error) {
		return logging, nil
	}
	openDaemonState = func(context.Context, config.Config) (*productionState, error) {
		return &productionState{}, nil
	}
	runtime, err := openProductionRuntime(context.Background(), config.Config{})
	if err != nil || runtime == nil || runtime.report == nil {
		t.Fatalf("production runtime = (%v, %v)", runtime, err)
	}
	runtime.report(errors.New("reported"))
}

func TestOpenProductionRuntimeReportsBoundaryFailures(t *testing.T) {
	originalLogging, originalState := newDaemonLoggingManager, openDaemonState
	t.Cleanup(func() { newDaemonLoggingManager, openDaemonState = originalLogging, originalState })
	wantErr := errors.New("runtime setup failed")
	newDaemonLoggingManager = func(config.LogConfig, logDestinationFactory) (*loggingManager, error) {
		return nil, wantErr
	}
	if _, err := openProductionRuntime(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("logging setup error = %v", err)
	}
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	newDaemonLoggingManager = func(config.LogConfig, logDestinationFactory) (*loggingManager, error) {
		return logging, nil
	}
	openDaemonState = func(context.Context, config.Config) (*productionState, error) { return nil, wantErr }
	if _, err = openProductionRuntime(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("state setup error = %v", err)
	}
}

func TestProductionRuntimeBuildReportsFailures(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	wantErr := errors.New("generation failed")
	buildDaemonGeneration = func(context.Context, config.Config, *productionState, *localLeadershipGate, func(error)) (*unifiedGeneration, error) {
		return nil, wantErr
	}
	if _, err := runtime.Build(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("initial build error = %v", err)
	}
	runtime.firstBuild = false
	if _, err := runtime.Build(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("replacement build error = %v", err)
	}
	runtime.logging.factory = failingLogFactory{err: wantErr}
	if _, err := runtime.Build(context.Background(), config.Config{}); !errors.Is(err, wantErr) {
		t.Fatalf("replacement logging error = %v", err)
	}
}

func TestGenerationConfigLoaderReportsReloadFailureAndAcceptsSameBackend(t *testing.T) {
	wantErr := errors.New("reload failed")
	loader := newGenerationConfigLoader(config.Config{}, func() (config.Config, error) {
		return config.Config{}, wantErr
	})
	if _, err := loader(); err != nil {
		t.Fatalf("initial configuration: %v", err)
	}
	if _, err := loader(); !errors.Is(err, wantErr) {
		t.Fatalf("reload error = %v", err)
	}
	loader = newGenerationConfigLoader(config.Config{}, func() (config.Config, error) {
		return config.Config{Prune: true}, nil
	})
	if _, err := loader(); err != nil {
		t.Fatalf("initial same-backend configuration: %v", err)
	}
	loaded, err := loader()
	if err != nil || !loaded.Prune {
		t.Fatalf("same-backend reload = (%+v, %v)", loaded, err)
	}
}

var _ unifiedStore = (*builderStoreFake)(nil)
var _ statecontract.RegistrationStore = (*builderStoreFake)(nil)
