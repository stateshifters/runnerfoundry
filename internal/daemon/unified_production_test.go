package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubpostgres "github.com/stateshifters/runnerfoundry/internal/source/github/persistence/postgres"
	githubsqlite "github.com/stateshifters/runnerfoundry/internal/source/github/persistence/sqlite"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
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

func TestProductionDependenciesProvideUsableTicker(t *testing.T) {
	ticker := newProductionDependencies().newTicker(time.Hour)
	if ticker.Ticks() == nil {
		t.Fatal("production ticker channel is nil")
	}
	ticker.Stop()
}

func TestProductionStateUsesTrimmedHostnameIdentity(t *testing.T) {
	dependencies := testProductionStateDependencies()
	calls := 0
	dependencies.readHostname = func() (string, error) {
		calls++
		return "  runner-host-7\n", nil
	}

	state, err := openProductionStateWithDependencies(context.Background(), config.Config{State: config.StateConfig{Path: "state.db"}}, dependencies)
	if err != nil || state.member != daemonTestID(7) || calls != 1 {
		t.Fatalf("production member identity = (%s, %d, %v)", state.member, calls, err)
	}
}

func TestProductionStateCompositionUsesSelectedBackend(t *testing.T) {
	dependencies := testProductionStateDependencies()
	assertProductionSQLiteState(t, dependencies)
	assertProductionPostgreSQLState(t, dependencies)
}

func assertProductionSQLiteState(t *testing.T, dependencies productionDependencies) {
	t.Helper()
	sqliteState, err := openProductionStateWithDependencies(context.Background(), config.Config{State: config.StateConfig{Path: "state.db"}}, dependencies)
	if err != nil || sqliteState == nil || sqliteState.registrationStore == nil || len(sqliteState.closers) != 1 {
		t.Fatalf("SQLite state = (%v, %v)", sqliteState, err)
	}
	if err = sqliteState.closers[0].Close(context.Background()); err != nil {
		t.Fatalf("close SQLite state: %v", err)
	}
	if leader, leaderErr := sqliteState.newLeadership().IsLeader(context.Background()); leaderErr != nil || !leader {
		t.Fatalf("SQLite leadership = (%t, %v)", leader, leaderErr)
	}
}

func assertProductionPostgreSQLState(t *testing.T, dependencies productionDependencies) {
	t.Helper()
	settings := postgresconfig.Settings{Role: "runnerfoundry"}
	dependencies.tryLeader = func(*cluster.LeaderElector, context.Context) (*cluster.LeaderLease, error) {
		return nil, cluster.ErrNotLeader
	}
	postgresState, err := openProductionStateWithDependencies(
		context.Background(), config.Config{State: config.StateConfig{Postgres: &settings}}, dependencies,
	)
	if err != nil || postgresState == nil || postgresState.registrationStore == nil || len(postgresState.closers) != 1 {
		t.Fatalf("PostgreSQL state = (%v, %v)", postgresState, err)
	}
	if err = postgresState.closers[0].Close(context.Background()); err != nil {
		t.Fatalf("close PostgreSQL state: %v", err)
	}
	if leader, leaderErr := postgresState.newLeadership().IsLeader(context.Background()); leaderErr != nil || leader {
		t.Fatalf("PostgreSQL leadership = (%t, %v)", leader, leaderErr)
	}
}

func TestProductionStateReportsSQLiteFailures(t *testing.T) {
	dependencies := testProductionStateDependencies()
	wantErr := errors.New("state failed")
	dependencies.openSQLite = func(context.Context, string, ...sqlitestate.SchemaExtension) (*sqlitestate.Store, error) {
		return nil, wantErr
	}
	if _, err := openProductionStateWithDependencies(context.Background(), config.Config{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("selected backend error = %v", err)
	}
	if _, err := openProductionSQLiteStateWithDependencies(context.Background(), "state.db", dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("SQLite error = %v", err)
	}
}

func TestProductionStateReportsPostgreSQLFailures(t *testing.T) {
	dependencies := testProductionStateDependencies()
	wantErr := errors.New("state failed")
	settings := postgresconfig.Settings{Role: "runnerfoundry"}
	dependencies.newConnections = func(postgresconfig.Settings) (*cluster.Connections, error) {
		return nil, wantErr
	}
	assertPostgreSQLStateFailure(t, settings, dependencies, wantErr, "connection configuration")
	dependencies.newConnections = func(postgresconfig.Settings) (*cluster.Connections, error) {
		return &cluster.Connections{}, nil
	}
	dependencies.newMigrationCatalog = func() (*cluster.Catalog, error) { return nil, wantErr }
	assertPostgreSQLStateFailure(t, settings, dependencies, wantErr, "migration catalog")
	dependencies.newMigrationCatalog = func() (*cluster.Catalog, error) { return &cluster.Catalog{}, nil }
	dependencies.migratePostgreSQL = func(*cluster.Migrator, context.Context) error { return wantErr }
	assertPostgreSQLStateFailure(t, settings, dependencies, wantErr, "migration")
	dependencies.migratePostgreSQL = func(*cluster.Migrator, context.Context) error { return nil }
	dependencies.openClusterPool = func(*cluster.Connections, context.Context) (*pgxpool.Pool, error) { return nil, wantErr }
	assertPostgreSQLStateFailure(t, settings, dependencies, wantErr, "pool")
}

func assertPostgreSQLStateFailure(
	t *testing.T,
	settings postgresconfig.Settings,
	dependencies productionDependencies,
	wantErr error,
	description string,
) {
	t.Helper()
	if _, err := openProductionPostgreSQLStateWithDependencies(context.Background(), settings, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("PostgreSQL %s error = %v", description, err)
	}
}

func TestProductionStateReportsMemberIdentityFailure(t *testing.T) {
	dependencies := testProductionStateDependencies()
	wantErr := errors.New("identity unavailable")
	dependencies.readHostname = func() (string, error) { return "", wantErr }
	if _, err := openProductionStateWithDependencies(context.Background(), config.Config{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("member identity error = %v", err)
	}
	dependencies.readHostname = func() (string, error) { return " \n", nil }
	if _, err := openProductionStateWithDependencies(context.Background(), config.Config{}, dependencies); err == nil {
		t.Fatal("empty member hostname was accepted")
	}
}

func TestProductionMemberIdentityRequiresHostnameReader(t *testing.T) {
	t.Parallel()

	if _, err := productionMemberIdentity(nil); err == nil {
		t.Fatal("nil hostname reader was accepted")
	}
}

func testProductionStateDependencies() productionDependencies {
	dependencies := newProductionDependencies()
	dependencies.readHostname = func() (string, error) { return string(daemonTestID(1)), nil }
	dependencies.openSQLite = func(context.Context, string, ...sqlitestate.SchemaExtension) (*sqlitestate.Store, error) {
		return &sqlitestate.Store{}, nil
	}
	dependencies.closeSQLite = func(*sqlitestate.Store) error { return nil }
	dependencies.newSQLiteGitHubStore = func(sqlitestate.Executor) *githubsqlite.Store { return &githubsqlite.Store{} }
	dependencies.newConnections = func(postgresconfig.Settings) (*cluster.Connections, error) { return &cluster.Connections{}, nil }
	dependencies.newMigrationCatalog = func() (*cluster.Catalog, error) { return &cluster.Catalog{}, nil }
	dependencies.newMigrator = func(*cluster.Connections, *cluster.Catalog) *cluster.Migrator { return &cluster.Migrator{} }
	dependencies.migratePostgreSQL = func(*cluster.Migrator, context.Context) error { return nil }
	dependencies.openClusterPool = func(*cluster.Connections, context.Context) (*pgxpool.Pool, error) { return &pgxpool.Pool{}, nil }
	dependencies.newPGConnection = func(*pgxpool.Pool) postgresstate.Connection { return postgresstate.Connection{} }
	dependencies.newPGStore = func(postgresstate.Connection) *postgresstate.Store { return &postgresstate.Store{} }
	dependencies.newPGGitHubStore = func(postgresstate.Connection) *githubpostgres.Store { return &githubpostgres.Store{} }
	dependencies.newQueueNotificationPool = func(*pgxpool.Pool) queueNotificationPool {
		return queueNotificationPool{maxConnections: 2}
	}
	dependencies.closePool = func(*pgxpool.Pool) {}
	dependencies.newLeaderElector = func(*cluster.Connections) *cluster.LeaderElector { return &cluster.LeaderElector{} }
	dependencies.tryLeader = func(*cluster.LeaderElector, context.Context) (*cluster.LeaderLease, error) {
		return &cluster.LeaderLease{}, nil
	}
	return dependencies
}

func TestProductionGitHubManagerUsesTransportFactories(t *testing.T) {
	dependencies, observed := productionGitHubFactoryDependencies()
	configured := builderConfig()
	configured.GitHub.Apps = []config.GitHubAppConfig{
		{AppID: 11, PrivateKeyFile: "first.pem"},
		{AppID: 22, PrivateKeyFile: "second.pem"},
	}
	built, err := buildProductionGitHubManagerWithDependencies(configured, newBuilderStoreFake(), nil, dependencies)
	if err != nil || built == nil {
		t.Fatalf("GitHub manager = (%v, %v)", built, err)
	}
	if !reflect.DeepEqual(observed.discoverers, []int64{11, 22}) || !reflect.DeepEqual(observed.clients, []int64{11, 22}) {
		t.Fatalf("credential factory order = discoverers %#v clients %#v", observed.discoverers, observed.clients)
	}
}

type productionGitHubFactoryObservation struct {
	discoverers []int64
	clients     []int64
}

func productionGitHubFactoryDependencies() (productionDependencies, *productionGitHubFactoryObservation) {
	dependencies := newProductionDependencies()
	observed := &productionGitHubFactoryObservation{}
	dependencies.newDiscoverer = func(credentials transport.Credentials, _ http.RoundTripper) (transport.Discoverer, error) {
		observed.discoverers = append(observed.discoverers, credentials.AppID)
		return transport.Discoverer{}, nil
	}
	dependencies.newClient = func(_ transport.Repository, credentials transport.Credentials, _ http.RoundTripper) (*transport.Client, error) {
		observed.clients = append(observed.clients, credentials.AppID)
		return &transport.Client{}, nil
	}
	dependencies.newManager = func(
		discoverer githubsource.RepositoryDiscoverer,
		factory githubsource.ManagerClientFactoryFunc,
		repository githubsource.RegistrationRepository,
		observer githubsource.ManagerObserver,
	) (*githubsource.Manager, error) {
		if _, err := factory(context.Background(), transport.Repository{}); err != nil {
			return nil, err
		}
		return githubsource.NewManager(discoverer, factory, repository, observer)
	}
	dependencies.readFile = func(path string) ([]byte, error) { return []byte("key-for-" + path), nil }
	return dependencies, observed
}

func TestProductionGitHubManagerReportsBoundaryFailures(t *testing.T) {
	wantErr := errors.New("GitHub failed")
	store := newBuilderStoreFake()
	configured := builderConfig()
	configured.GitHub.Apps = []config.GitHubAppConfig{{AppID: 1, PrivateKeyFile: "key.pem"}}
	assertProductionDiscovererFailure(t, configured, store, wantErr)
	assertProductionClientFailure(t, configured, store, wantErr)
	assertProductionManagerFailure(t, configured, store, wantErr)
	dependencies := newProductionDependencies()
	dependencies.readFile = func(string) ([]byte, error) { return nil, wantErr }
	if _, err := buildProductionGitHubManagerWithDependencies(configured, store, nil, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("credentials error = %v", err)
	}
}

func TestProductionGitHubCoordinatorRejectsMissingAndInvalidAppScopes(t *testing.T) {
	dependencies := newProductionDependencies()
	dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	store := newBuilderStoreFake()
	if _, err := buildProductionGitHubManagerWithDependencies(config.Config{}, store, nil, dependencies); err == nil {
		t.Fatal("missing GitHub Apps were accepted")
	}
	if _, err := buildGitHubAppManagers([]config.GitHubAppCredentials{{AppID: 0, PrivateKey: "key"}},
		store, nil, dependencies); err == nil {
		t.Fatal("invalid GitHub App scope was accepted")
	}
}

func assertProductionDiscovererFailure(
	t *testing.T,
	configured config.Config,
	store githubsource.RegistrationRepository,
	wantErr error,
) {
	t.Helper()
	dependencies := newProductionDependencies()
	dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, wantErr
	}
	if _, err := buildProductionGitHubManagerWithDependencies(configured, store, nil, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("discoverer error = %v", err)
	}
}

func assertProductionClientFailure(
	t *testing.T,
	configured config.Config,
	store githubsource.RegistrationRepository,
	wantErr error,
) {
	t.Helper()
	dependencies := newProductionDependencies()
	dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	dependencies.newManager = func(
		_ githubsource.RepositoryDiscoverer,
		factory githubsource.ManagerClientFactoryFunc,
		_ githubsource.RegistrationRepository,
		_ githubsource.ManagerObserver,
	) (*githubsource.Manager, error) {
		_, err := factory(context.Background(), transport.Repository{})
		return nil, err
	}
	dependencies.newClient = func(transport.Repository, transport.Credentials, http.RoundTripper) (*transport.Client, error) {
		return nil, wantErr
	}
	if _, err := buildProductionGitHubManagerWithDependencies(configured, store, nil, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("client error = %v", err)
	}
}

func assertProductionManagerFailure(
	t *testing.T,
	configured config.Config,
	store githubsource.RegistrationRepository,
	wantErr error,
) {
	t.Helper()
	dependencies := newProductionDependencies()
	dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	dependencies.newManager = func(
		githubsource.RepositoryDiscoverer,
		githubsource.ManagerClientFactoryFunc,
		githubsource.RegistrationRepository,
		githubsource.ManagerObserver,
	) (*githubsource.Manager, error) {
		return nil, wantErr
	}
	if _, err := buildProductionGitHubManagerWithDependencies(configured, store, nil, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("manager error = %v", err)
	}
}

func TestTimedProductionGenerationBuildsWithInjectedTicker(t *testing.T) {
	dependencies := newProductionDependencies()
	dependencies.newTicker = func(time.Duration) cycleTicker {
		return &manualCycleTicker{ticks: make(chan time.Time)}
	}
	completed := make(chan string, 1)
	dependencies.maintenanceLaneCompleted = func(laneName string, _ cycleTicker) {
		completed <- laneName
	}
	dependencies.currentTime = builderTime
	store := newBuilderStoreFake()
	state := &productionState{store: store, registrationStore: store, demandStore: store, member: daemonTestID(1),
		newLeadership: func() leadershipRuntime { return permanentLeadership{} }}
	generation, err := buildTimedProductionGeneration(
		builderConfig(), state, &localLeadershipGate{}, nil,
		productionGenerationComponents{
			manager:   newBuilderManager(t, store),
			providers: map[core.RunnerPool]execution.RunnerLauncher{"linux": builderProviderFake{}},
			inventory: builderProviderFake{}, member: daemonTestID(1),
		}, dependencies,
	)
	if err != nil || generation == nil {
		t.Fatalf("timed generation = (%v, %v)", generation, err)
	}
	assertTimedProductionLaneObserver(t, generation, completed)
	generation.reportCycle(errors.New("ignored report"))
}

func assertTimedProductionLaneObserver(
	t *testing.T,
	generation *maintenanceGeneration,
	completed <-chan string,
) {
	t.Helper()
	generation.runCycle(context.Background(), scheduledMaintenanceLane{
		name: "source", ticker: &manualCycleTicker{ticks: make(chan time.Time)},
		run: func(context.Context) error { return nil },
	}, "scheduled", 0)
	if laneName := <-completed; laneName != "source" {
		t.Fatalf("completed maintenance lane = %q, want source", laneName)
	}
}

func TestTimedProductionGenerationStopsTickerWhenCompositionFails(t *testing.T) {
	dependencies := newProductionDependencies()
	dependencies.newTicker = func(time.Duration) cycleTicker {
		return &manualCycleTicker{ticks: make(chan time.Time)}
	}
	store := newBuilderStoreFake()
	state := &productionState{store: store, registrationStore: store, demandStore: store, member: daemonTestID(1),
		newLeadership: func() leadershipRuntime { return permanentLeadership{} }}
	if _, err := buildTimedProductionGeneration(
		config.Config{}, state, &localLeadershipGate{}, nil,
		productionGenerationComponents{manager: newBuilderManager(t, store), member: daemonTestID(1)}, dependencies,
	); err == nil {
		t.Fatal("invalid timed generation was accepted")
	}
}

func TestBuildProductionGenerationComposesInjectedBoundaries(t *testing.T) {
	store := newBuilderStoreFake()
	dependencies := productionGenerationDependencies()
	configured := builderConfig()
	configured.GitHub.Apps = []config.GitHubAppConfig{{AppID: 1, PrivateKeyFile: "key.pem"}}
	dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
	state := &productionState{store: store, registrationStore: store, demandStore: store, member: daemonTestID(1),
		newLeadership: func() leadershipRuntime { return permanentLeadership{} }}
	generation, err := buildProductionGenerationWithDependencies(
		context.Background(), configured, state, &localLeadershipGate{}, func(error) {}, dependencies,
	)
	if err != nil || generation == nil {
		t.Fatalf("production generation = (%v, %v)", generation, err)
	}
}

func productionGenerationDependencies() productionDependencies {
	dependencies := newProductionDependencies()
	dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	dependencies.newManager = func(
		discoverer githubsource.RepositoryDiscoverer,
		factory githubsource.ManagerClientFactoryFunc,
		repository githubsource.RegistrationRepository,
		observer githubsource.ManagerObserver,
	) (*githubsource.Manager, error) {
		return githubsource.NewManager(discoverer, factory, repository, observer)
	}
	dependencies.buildProviderSet = func(
		context.Context,
		config.Config,
		core.MemberID,
		productionDependencies,
	) (map[core.RunnerPool]execution.RunnerLauncher, execution.AggregateRunnerInventory, error) {
		provider := builderProviderFake{}
		inventory := execution.NewAggregateRunnerInventory(provider)
		return map[core.RunnerPool]execution.RunnerLauncher{"linux": provider}, inventory, nil
	}
	dependencies.newTicker = func(time.Duration) cycleTicker {
		return &manualCycleTicker{ticks: make(chan time.Time)}
	}
	dependencies.currentTime = builderTime
	return dependencies
}

func TestBuildProductionGenerationReportsCompositionFailures(t *testing.T) {
	fixture := newProductionGenerationFailureFixture(t)
	fixture.assertManagerFailure(t)
	fixture.assertProviderFailure(t)
}

type productionGenerationFailureFixture struct {
	configured   config.Config
	state        *productionState
	manager      *githubsource.Manager
	wantErr      error
	dependencies productionDependencies
}

func newProductionGenerationFailureFixture(t *testing.T) productionGenerationFailureFixture {
	t.Helper()
	configured := builderConfig()
	configured.GitHub.Apps = []config.GitHubAppConfig{{AppID: 1, PrivateKeyFile: "key.pem"}}
	store := newBuilderStoreFake()
	return productionGenerationFailureFixture{
		configured: configured,
		state:      &productionState{store: store, registrationStore: store, member: daemonTestID(1)},
		manager:    newBuilderManager(t, store),
		wantErr:    errors.New("composition failed"), dependencies: func() productionDependencies {
			dependencies := newProductionDependencies()
			dependencies.readFile = func(string) ([]byte, error) { return []byte("key"), nil }
			return dependencies
		}(),
	}
}

func (fixture productionGenerationFailureFixture) assertManagerFailure(t *testing.T) {
	t.Helper()
	dependencies := fixture.dependencies
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, fixture.wantErr
	}
	if _, err := fixture.build(dependencies); !errors.Is(err, fixture.wantErr) {
		t.Fatalf("manager composition error = %v", err)
	}
}

func (fixture productionGenerationFailureFixture) managerDependencies() productionDependencies {
	dependencies := fixture.dependencies
	dependencies.newDiscoverer = func(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
		return transport.Discoverer{}, nil
	}
	dependencies.newManager = func(
		discoverer githubsource.RepositoryDiscoverer,
		factory githubsource.ManagerClientFactoryFunc,
		repository githubsource.RegistrationRepository,
		observer githubsource.ManagerObserver,
	) (*githubsource.Manager, error) {
		return githubsource.NewManager(discoverer, factory, repository, observer)
	}
	return dependencies
}

func (fixture productionGenerationFailureFixture) assertProviderFailure(t *testing.T) {
	t.Helper()
	dependencies := fixture.managerDependencies()
	dependencies.buildProviderSet = func(
		context.Context,
		config.Config,
		core.MemberID,
		productionDependencies,
	) (map[core.RunnerPool]execution.RunnerLauncher, execution.AggregateRunnerInventory, error) {
		return nil, execution.AggregateRunnerInventory{}, fixture.wantErr
	}
	if _, err := fixture.build(dependencies); !errors.Is(err, fixture.wantErr) {
		t.Fatalf("provider composition error = %v", err)
	}
}

func (fixture productionGenerationFailureFixture) build(dependencies productionDependencies) (*maintenanceGeneration, error) {
	return buildProductionGenerationWithDependencies(
		context.Background(), fixture.configured, fixture.state, &localLeadershipGate{}, nil, dependencies,
	)
}

func TestOpenProductionRuntimeUsesLoggingAndStateBoundaries(t *testing.T) {
	dependencies := newProductionDependencies()
	dependencies.logDestinationFactory = memoryLogFactory{}
	dependencies.openState = func(context.Context, config.Config, productionDependencies) (*productionState, error) {
		return &productionState{}, nil
	}
	dependencies.buildGeneration = func(context.Context, config.Config, *productionState, *localLeadershipGate, func(error), productionDependencies) (*maintenanceGeneration, error) {
		return newMaintenanceGeneration(
			&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
		), nil
	}
	runtime, err := openProductionRuntimeWithDependencies(context.Background(), config.Config{}, dependencies)
	if err != nil || runtime == nil || runtime.report == nil {
		t.Fatalf("production runtime = (%v, %v)", runtime, err)
	}
	runtime.report(errors.New("reported"))
	if _, err = runtime.Build(context.Background(), config.Config{}); err != nil {
		t.Fatalf("build production runtime: %v", err)
	}
}

func TestOpenProductionRuntimeReportsBoundaryFailures(t *testing.T) {
	wantErr := errors.New("runtime setup failed")
	dependencies := newProductionDependencies()
	dependencies.logDestinationFactory = failingLogFactory{err: wantErr}
	if _, err := openProductionRuntimeWithDependencies(context.Background(), config.Config{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("logging setup error = %v", err)
	}
	dependencies.logDestinationFactory = memoryLogFactory{}
	dependencies.openState = func(context.Context, config.Config, productionDependencies) (*productionState, error) {
		return nil, wantErr
	}
	if _, err := openProductionRuntimeWithDependencies(context.Background(), config.Config{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("state setup error = %v", err)
	}
}

func TestProductionRuntimeBuildReportsFailures(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	wantErr := errors.New("generation failed")
	runtime.build = func(context.Context, config.Config, *productionState, *localLeadershipGate, func(error)) (*maintenanceGeneration, error) {
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
var _ githubsource.RegistrationRepository = (*builderStoreFake)(nil)
