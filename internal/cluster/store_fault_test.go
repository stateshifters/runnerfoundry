package cluster

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestPublishSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		return store.Publish(context.Background(), "node", NodeHealthy, nil, time.Now())
	})
}

func TestScaleSetsSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.ScaleSets(context.Background(), time.Now(), time.Minute)
		return err
	})
}

func TestEnqueueSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		return store.Enqueue(context.Background(), Demand{AppID: 1, ScaleSet: "set", Repository: "repo", RequestID: 1, QueueTime: time.Now()})
	})
}

func TestAssignOldestSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.AssignOldest(context.Background(), "set", time.Now(), time.Minute)
		return err
	})
}

func TestClaimForStartSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.ClaimForStart(context.Background(), "node")
		return err
	})
}

func TestPendingAcquisitionsSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.PendingAcquisitions(context.Background())
		return err
	})
}

func TestStartingSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.Starting(context.Background(), "node")
		return err
	})
}

func TestCompleteDemandSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		return store.CompleteDemand(context.Background(), "set", "repo", 1)
	})
}

func TestBeginAcquisitionSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error { return store.BeginAcquisition(context.Background(), 1, 2, 3) })
}

func TestConfirmAcquisitionSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error { return store.ConfirmAcquisition(context.Background(), 1) })
}

func TestConfirmRunningSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error { return store.ConfirmRunning(context.Background(), 1, "node") })
}

func TestCompleteAssignmentSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		return store.CompleteAssignment(context.Background(), 1, "node", AssignmentReserved)
	})
}

func TestDeferAssignmentSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		return store.DeferAssignment(context.Background(), 1, "node", AssignmentStarting, time.Now())
	})
}

func TestTryLeaderSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.TryLeader(context.Background())
		return err
	})
}

func TestWaitForLeaderSurfacesConnectionFailure(t *testing.T) {
	assertStoreConnectionFailure(t, func(store *Store) error {
		_, err := store.WaitForLeader(context.Background())
		return err
	})
}

func assertStoreConnectionFailure(t *testing.T, operation func(*Store) error) {
	t.Helper()
	connectionErr := errors.New("connection failed")
	store := NewStore(postgresconfig.Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, connectionErr }
	store.directConnector = func(context.Context) (leaderConnection, error) { return nil, connectionErr }
	if err := operation(store); !errors.Is(err, connectionErr) {
		t.Fatalf("operation error = %v, want connector failure", err)
	}
}

func TestPostgreSQLConnectorRedactsCredentialResolutionFailure(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	resolver := testConnectionResolver(nil)
	productionConnectionSettingResolver = resolver
	settings := settingsWithProofReference("file:///missing-postgresql-secret")
	assertPostgreSQLConnectorFailureRedacts(t, settings, nil, []string{"missing-postgresql-secret", "runnerfoundry"})
}

func TestPostgreSQLConnectorRedactsConnectionFailure(t *testing.T) {
	values := newConnectionFixtureValues()
	connectionURL, err := url.Parse(values.dsn("127.0.0.1", "1", "database"))
	if err != nil {
		t.Fatal(err)
	}
	query := connectionURL.Query()
	query.Set("connect_timeout", "1")
	connectionURL.RawQuery = query.Encode()
	settings := postgresconfig.Settings{DSN: connectionURL.String(), Role: "runnerfoundry"}
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	assertPostgreSQLConnectorFailureRedacts(t, settings, func(store *Store) {
		store.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) {
			return nil, errors.New("connection unavailable")
		}
	}, []string{values.identity, values.proof, "runnerfoundry", "postgres://"})
}

func assertPostgreSQLConnectorFailureRedacts(
	t *testing.T,
	settings postgresconfig.Settings,
	configure func(*Store),
	forbiddenValues []string,
) {
	t.Helper()
	store := NewStore(settings)
	if configure != nil {
		configure(store)
	}
	operations := []func() error{
		func() error { return store.Migrate(context.Background()) },
		func() error { _, err := store.MigrationStatus(context.Background()); return err },
		func() error { return store.MigrateDown(context.Background(), 0, time.Now(), time.Minute) },
	}
	for _, operation := range operations {
		err := operation()
		if err == nil {
			t.Fatal("expected PostgreSQL connection setup failure")
		}
		for _, forbidden := range forbiddenValues {
			if strings.Contains(err.Error(), forbidden) {
				t.Fatalf("connection error exposed forbidden value %q: %v", forbidden, err)
			}
		}
	}
}

func TestStoreDefaultConnectorsRejectInvalidSettings(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	store := NewStore(postgresconfig.Settings{Password: "env://MISSING_PRIMARY_PASSWORD", Role: "runnerfoundry"})
	if _, err := store.connectPostgreSQL(context.Background()); err == nil {
		t.Fatal("expected primary connector resolution failure")
	}
	if _, err := store.directConnector(context.Background()); err == nil {
		t.Fatal("expected direct connector resolution failure")
	}
}

func TestMigrationStatusReportsMetadataInspectionFailure(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{{err: databaseFailure()}}})
	if _, err := store.MigrationStatus(context.Background()); err == nil {
		t.Fatal("expected migration metadata inspection failure")
	}
}

func TestAssignmentTransitionErrorReportsExpectedState(t *testing.T) {
	err := &AssignmentTransitionError{AssignmentID: 7, Expected: AssignmentStarting}
	if !strings.Contains(err.Error(), "assignment 7 is not starting") || !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("transition error = %v", err)
	}
}

func TestEnqueueRejectsInvalidDemandBeforeConnecting(t *testing.T) {
	tests := []struct {
		name   string
		demand Demand
	}{
		{name: "empty scale set", demand: Demand{Repository: "repo", RequestID: 1, QueueTime: time.Now()}},
		{name: "blank scale set", demand: Demand{ScaleSet: " \t", Repository: "repo", RequestID: 1, QueueTime: time.Now()}},
		{name: "empty repository", demand: Demand{ScaleSet: "set", RequestID: 1, QueueTime: time.Now()}},
		{name: "blank repository", demand: Demand{ScaleSet: "set", Repository: " \t", RequestID: 1, QueueTime: time.Now()}},
		{name: "non-positive request", demand: Demand{ScaleSet: "set", Repository: "repo", QueueTime: time.Now()}},
		{name: "missing queue time", demand: Demand{ScaleSet: "set", Repository: "repo", RequestID: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewStore(postgresconfig.Settings{})
			store.connector = connectorThatMustNotRun(t)
			if err := store.Enqueue(context.Background(), test.demand); !errors.Is(err, ErrInvalidDemand) {
				t.Fatalf("Enqueue() error = %v, want %v", err, ErrInvalidDemand)
			}
		})
	}
}

func TestAssignOldestRejectsInvalidScaleSetBeforeConnecting(t *testing.T) {
	store := NewStore(postgresconfig.Settings{})
	store.connector = connectorThatMustNotRun(t)
	if _, err := store.AssignOldest(context.Background(), " \t", time.Now(), time.Minute); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("AssignOldest() error = %v, want %v", err, ErrInvalidDemand)
	}
}

func TestCompleteDemandRejectsInvalidRepositoryBeforeConnecting(t *testing.T) {
	store := NewStore(postgresconfig.Settings{})
	store.connector = connectorThatMustNotRun(t)
	if err := store.CompleteDemand(context.Background(), "set", " \t", 1); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("CompleteDemand() error = %v, want %v", err, ErrInvalidDemand)
	}
}

func connectorThatMustNotRun(t *testing.T) func(context.Context) (*databaseConnection, error) {
	t.Helper()
	return func(context.Context) (*databaseConnection, error) {
		t.Fatal("invalid demand reached the database connector")
		return nil, errors.New("unreachable connector")
	}
}

func TestPublishRejectsInvalidContributionsBeforeConnecting(t *testing.T) {
	tests := []struct {
		name         string
		nodeID       NodeID
		contribution Contribution
	}{
		{name: "blank node", nodeID: " \t", contribution: Contribution{ScaleSet: "set", RunnerGroup: "group"}},
		{name: "blank scale set", nodeID: "node", contribution: Contribution{ScaleSet: " \t", RunnerGroup: "group"}},
		{name: "blank runner group", nodeID: "node", contribution: Contribution{ScaleSet: "set", RunnerGroup: " \t"}},
		{name: "negative capacity", nodeID: "node", contribution: Contribution{ScaleSet: "set", RunnerGroup: "group", MaxRunners: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewStore(postgresconfig.Settings{})
			store.connector = func(context.Context) (*databaseConnection, error) {
				t.Fatal("invalid publication reached the database connector")
				return nil, errors.New("unreachable connector")
			}
			err := store.Publish(context.Background(), test.nodeID, NodeHealthy, []Contribution{test.contribution}, time.Now())
			if !errors.Is(err, ErrInvalidPublication) {
				t.Fatalf("Publish() error = %v, want %v", err, ErrInvalidPublication)
			}
		})
	}
}

func TestPublishSurfacesDatabaseBeginFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{beginErr: databaseFailure()}, func(store *Store) error {
		return store.Publish(context.Background(), "node", NodeHealthy, nil, time.Now())
	})
}

func TestScaleSetsSurfacesDatabaseQueryFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{queryErr: databaseFailure()}, func(store *Store) error {
		_, err := store.ScaleSets(context.Background(), time.Now(), time.Minute)
		return err
	})
}

func TestEnqueueSurfacesDatabaseExecutionFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{execErrors: []error{databaseFailure()}}, func(store *Store) error {
		return store.Enqueue(context.Background(), Demand{AppID: 1, ScaleSet: "set", Repository: "repo", RequestID: 1, QueueTime: time.Now()})
	})
}

func TestAssignOldestSurfacesDatabaseBeginFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{beginErr: databaseFailure()}, func(store *Store) error {
		_, err := store.AssignOldest(context.Background(), "set", time.Now(), time.Minute)
		return err
	})
}

func TestClaimForStartSurfacesDatabaseQueryFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{rowResults: []fakeRow{{err: databaseFailure()}}}, func(store *Store) error {
		_, err := store.ClaimForStart(context.Background(), "node")
		return err
	})
}

func TestPendingAcquisitionsSurfacesDatabaseQueryFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{queryErr: databaseFailure()}, func(store *Store) error {
		_, err := store.PendingAcquisitions(context.Background())
		return err
	})
}

func TestCompleteDemandSurfacesDatabaseBeginFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{beginErr: databaseFailure()}, func(store *Store) error {
		return store.CompleteDemand(context.Background(), "set", "repo", 1)
	})
}

func TestConfirmAcquisitionSurfacesDatabaseExecutionFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{execErrors: []error{databaseFailure()}}, func(store *Store) error {
		return store.ConfirmAcquisition(context.Background(), 1)
	})
}

func TestTryLeaderSurfacesDatabaseQueryFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{rowResults: []fakeRow{{err: databaseFailure()}}}, func(store *Store) error {
		_, err := store.TryLeader(context.Background())
		return err
	})
}

func TestWaitForLeaderSurfacesDatabaseExecutionFailure(t *testing.T) {
	assertDatabaseOperationFailure(t, &fakeDatabaseConnection{execErrors: []error{databaseFailure()}}, func(store *Store) error {
		_, err := store.WaitForLeader(context.Background())
		return err
	})
}

func assertDatabaseOperationFailure(t *testing.T, connection *fakeDatabaseConnection, operation func(*Store) error) {
	t.Helper()
	if err := operation(storeWithConnection(connection)); err == nil {
		t.Fatal("expected database operation failure")
	}
}

func databaseFailure() error {
	return errors.New("database failed")
}

func TestFakeDatabaseOperationsFollowScriptedCallOrder(t *testing.T) {
	firstErr := errors.New("first database operation failed")
	secondErr := errors.New("second database operation failed")
	transaction := &fakeDatabaseTransaction{
		execErrors: []error{firstErr, secondErr},
		rowResults: []fakeRow{
			{err: firstErr},
			{err: secondErr},
		},
	}
	for _, expectedErr := range []error{firstErr, secondErr} {
		if _, err := transaction.Exec(context.Background(), "unchanged statement"); !errors.Is(err, expectedErr) {
			t.Fatalf("Exec() error = %v, want %v", err, expectedErr)
		}
	}
	for _, expectedErr := range []error{firstErr, secondErr} {
		if err := transaction.QueryRow(context.Background(), "unchanged statement").Scan(); !errors.Is(err, expectedErr) {
			t.Fatalf("QueryRow().Scan() error = %v, want %v", err, expectedErr)
		}
	}
}

func TestWaitForLeaderReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connection := &fakeDatabaseConnection{execErrors: []error{errors.New("database failed")}}
	store := storeWithConnection(connection)
	if _, err := store.WaitForLeader(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
	}
	if !errors.Is(connection.closeContextErr, context.Canceled) {
		t.Fatalf("close context error = %v, want canceled", connection.closeContextErr)
	}
}

func TestPublishReportsEveryTransactionalFailure(t *testing.T) {
	databaseErr := errors.New("database failed")
	contribution := []Contribution{{ScaleSet: "set", RunnerGroup: "group", MaxRunners: 1, Healthy: true}}
	tests := []struct {
		name       string
		execErrors []error
		commitErr  error
	}{
		{name: "node", execErrors: []error{databaseErr}},
		{name: "replacement", execErrors: []error{nil, databaseErr}},
		{name: "contribution", execErrors: []error{nil, nil, databaseErr}},
		{name: "withdrawn demand", execErrors: []error{nil, nil, nil, databaseErr}},
		{name: "commit", commitErr: databaseErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{execErrors: test.execErrors, commitErr: test.commitErr}
			store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
			if err := store.Publish(context.Background(), "node", NodeHealthy, contribution, time.Now()); err == nil {
				t.Fatal("expected publication failure")
			}
		})
	}
}

func TestCompleteDemandReportsEveryTransactionalFailure(t *testing.T) {
	databaseErr := errors.New("database failed")
	tests := []struct {
		name       string
		execErrors []error
		commitErr  error
	}{
		{name: "assignment completion", execErrors: []error{databaseErr}},
		{name: "demand removal", execErrors: []error{nil, databaseErr}},
		{name: "commit", commitErr: databaseErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{execErrors: test.execErrors, commitErr: test.commitErr}
			store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
			if err := store.CompleteDemand(context.Background(), "set", "repo", 1); err == nil {
				t.Fatal("expected demand completion failure")
			}
		})
	}
}

func TestAssignmentReportsUnavailableReleaseFailure(t *testing.T) {
	assertAssignmentFailure(t, &fakeDatabaseTransaction{execErrors: []error{databaseFailure()}}, nil)
}

func TestAssignmentReportsAbsentOldestDemand(t *testing.T) {
	assertAssignmentFailure(t, &fakeDatabaseTransaction{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}, ErrNoAssignment)
}

func TestAssignmentReportsOldestDemandQueryFailure(t *testing.T) {
	assertAssignmentFailure(t, &fakeDatabaseTransaction{rowResults: []fakeRow{{err: databaseFailure()}}}, nil)
}

func TestAssignmentReportsAbsentContributor(t *testing.T) {
	oldest, _, _ := assignmentRows(t)
	assertAssignmentFailure(t, &fakeDatabaseTransaction{rowResults: []fakeRow{oldest, {err: pgx.ErrNoRows}}}, ErrNoAssignment)
}

func TestAssignmentReportsContributorQueryFailure(t *testing.T) {
	oldest, _, _ := assignmentRows(t)
	assertAssignmentFailure(t, &fakeDatabaseTransaction{rowResults: []fakeRow{oldest, {err: databaseFailure()}}}, nil)
}

func TestAssignmentReportsInsertFailure(t *testing.T) {
	oldest, contributor, _ := assignmentRows(t)
	assertAssignmentFailure(t, &fakeDatabaseTransaction{
		rowResults: []fakeRow{oldest, contributor, {err: databaseFailure()}},
	}, nil)
}

func TestAssignmentReportsCommitFailure(t *testing.T) {
	oldest, contributor, identifier := assignmentRows(t)
	assertAssignmentFailure(t, &fakeDatabaseTransaction{
		rowResults: []fakeRow{oldest, contributor, identifier}, commitErr: databaseFailure(),
	}, nil)
}

func assertAssignmentFailure(t *testing.T, transaction *fakeDatabaseTransaction, expected error) {
	t.Helper()
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	_, err := store.AssignOldest(context.Background(), "set", time.Now(), time.Minute)
	if expected != nil && !errors.Is(err, expected) {
		t.Fatalf("error = %v, want %v", err, expected)
	}
	if expected == nil && err == nil {
		t.Fatal("expected assignment failure")
	}
}

func assignmentRows(t *testing.T) (oldest fakeRow, contributor fakeRow, identifier fakeRow) {
	t.Helper()
	oldest = fakeRow{scan: func(destinations ...any) {
		*destination[int64](t, destinations[0]) = 1
		*destination[string](t, destinations[1]) = "repo"
		*destination[int64](t, destinations[2]) = 2
		*destination[time.Time](t, destinations[3]) = time.Now()
		*destination[string](t, destinations[4]) = "job_available"
		*destination[int64](t, destinations[5]) = 0
		*destination[githubapp.InstallationID](t, destinations[6]) = 0
		*destination[int](t, destinations[7]) = 0
		*destination[int64](t, destinations[8]) = 0
	}}
	contributor = fakeRow{scan: func(destinations ...any) { *destination[NodeID](t, destinations[0]) = "node" }}
	identifier = fakeRow{scan: func(destinations ...any) { *destination[AssignmentID](t, destinations[0]) = 3 }}
	return oldest, contributor, identifier
}

func TestScaleSetStatusReportsRowFailures(t *testing.T) {
	databaseErr := errors.New("database failed")
	if _, err := scanScaleSetStatuses(newDatabaseRows(&fakeDatabaseRows{scanErr: databaseErr, remaining: 1})); err == nil {
		t.Fatal("expected scale-set scan failure")
	}
	if _, err := scanScaleSetStatuses(newDatabaseRows(&fakeDatabaseRows{rowsErr: databaseErr})); err == nil {
		t.Fatal("expected scale-set row iteration failure")
	}
}

func TestTryLeaderReportsAContendedLease(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{
		{scan: func(destinations ...any) {
			*destination[bool](t, destinations[0]) = false
		}},
	}})
	if _, err := store.TryLeader(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("leader result = %v", err)
	}
}

func TestClaimForStartReportsAnEmptyReadyQueue(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{{err: pgx.ErrNoRows}}})
	if _, err := store.ClaimForStart(context.Background(), "node"); !errors.Is(err, ErrNoReadyAssignment) {
		t.Fatalf("empty ready queue result = %v", err)
	}
}
func TestLeaderLeaseReflectsConnectionHealthAndClosure(t *testing.T) {
	databaseErr := errors.New("database failed")
	connection := &fakeDatabaseConnection{pingErr: databaseErr}
	lease := &LeaderLease{connection: connection}
	if lease.Alive(context.Background()) {
		t.Fatal("failed ping reported a live lease")
	}
	connection.pingErr = nil
	if !lease.Alive(context.Background()) {
		t.Fatal("healthy lease reported unavailable")
	}
	connection.closed = true
	if lease.Alive(context.Background()) {
		t.Fatal("closed lease reported alive")
	}
	connection.closeErr = databaseErr
	if err := lease.Close(context.Background()); err == nil {
		t.Fatal("expected lease close failure")
	}
}

func TestStoreReturnsConnectionCloseFailure(t *testing.T) {
	closeErr := errors.New("close failed")
	store := storeWithConnection(&fakeDatabaseConnection{closeErr: closeErr})
	err := store.Enqueue(context.Background(), Demand{
		AppID: 1, ScaleSet: "set", Repository: "repo", RequestID: 1, QueueTime: time.Now(),
	})
	if !errors.Is(err, closeErr) {
		t.Fatalf("enqueue error = %v, want connection close failure", err)
	}
}

func TestStoreJoinsTransactionRollbackFailure(t *testing.T) {
	operationErr := errors.New("publish failed")
	rollbackErr := errors.New("rollback failed")
	transaction := &fakeDatabaseTransaction{
		execErrors:  []error{operationErr},
		rollbackErr: rollbackErr,
	}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	err := store.Publish(context.Background(), "node", NodeHealthy, nil, time.Now())
	if !errors.Is(err, operationErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("publish error = %v, want operation and rollback failures", err)
	}
}

func TestStoreDoesNotRollbackAfterCommitBegins(t *testing.T) {
	commitErr := errors.New("commit failed")
	transaction := &fakeDatabaseTransaction{commitErr: commitErr}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	err := store.Publish(context.Background(), "node", NodeHealthy, nil, time.Now())
	if !errors.Is(err, commitErr) {
		t.Fatalf("publish error = %v, want commit failure", err)
	}
	if transaction.rollbackCalls != 0 {
		t.Fatalf("rollback calls = %d, want zero after commit begins", transaction.rollbackCalls)
	}
}

func TestEmptyLeaderLeaseIsClosedAndSafeToClose(t *testing.T) {
	if (&LeaderLease{}).Alive(context.Background()) || (*LeaderLease)(nil).Alive(context.Background()) {
		t.Fatal("empty lease reported alive")
	}
	if err := (*LeaderLease)(nil).Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAssignmentRecoveryReportsRowFailures(t *testing.T) {
	databaseErr := errors.New("database failed")
	tests := []struct {
		name string
		rows rowIterator
	}{
		{name: "scan", rows: &fakeDatabaseRows{remaining: 1, scanErr: databaseErr}},
		{name: "iteration", rows: &fakeDatabaseRows{rowsErr: databaseErr}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := storeWithConnection(&fakeDatabaseConnection{rowsResult: test.rows})
			if _, err := store.Starting(context.Background(), "node"); err == nil {
				t.Fatal("expected assignment recovery read failure")
			}
		})
	}
}

func TestAssignmentTransitionRejectsMissingOrStaleRows(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{execTag: pgconn.NewCommandTag("UPDATE 0")})
	err := store.ConfirmAcquisition(context.Background(), 17)
	if !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("ConfirmAcquisition() error = %v, want %v", err, ErrAssignmentConflict)
	}
	var transitionErr *AssignmentTransitionError
	if !errors.As(err, &transitionErr) || transitionErr.AssignmentID != 17 || transitionErr.Expected != AssignmentAcquiring {
		t.Fatalf("ConfirmAcquisition() error details = %#v", transitionErr)
	}
}

func storeWithConnection(connection *fakeDatabaseConnection) *Store {
	store := NewStore(postgresconfig.Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return connection.databaseConnection(), nil }
	store.directConnector = func(context.Context) (leaderConnection, error) { return connection, nil }
	return store
}

func (connection *fakeDatabaseConnection) databaseConnection() *databaseConnection {
	return &databaseConnection{
		begin:    connection.Begin,
		close:    connection.Close,
		exec:     connection.Exec,
		query:    connection.Query,
		queryRow: connection.QueryRow,
	}
}

type fakeDatabaseConnection struct {
	transaction     *fakeDatabaseTransaction
	beginErr        error
	execErrors      []error
	execTag         pgconn.CommandTag
	executions      []fakeDatabaseExecution
	queries         []fakeDatabaseExecution
	queryErr        error
	queryErrors     []error
	rowsResult      rowIterator
	rowsResults     []rowIterator
	rowResults      []fakeRow
	closeErr        error
	closeContextErr error
	pingErr         error
	closed          bool
}

func (connection *fakeDatabaseConnection) Begin(context.Context) (*databaseTransaction, error) {
	if connection.beginErr != nil {
		return nil, connection.beginErr
	}
	if connection.transaction == nil {
		connection.transaction = &fakeDatabaseTransaction{}
	}
	return connection.transaction.databaseTransaction(), nil
}

func (connection *fakeDatabaseConnection) Close(ctx context.Context) error {
	connection.closed = true
	connection.closeContextErr = ctx.Err()
	return connection.closeErr
}

func (connection *fakeDatabaseConnection) Exec(_ context.Context, statement string, arguments ...any) (pgconn.CommandTag, error) {
	connection.executions = append(connection.executions, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	if err := nextScriptedError(&connection.execErrors); err != nil {
		return pgconn.CommandTag{}, err
	}
	if connection.execTag.String() != "" {
		return connection.execTag, nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (connection *fakeDatabaseConnection) IsClosed() bool { return connection.closed }

func (connection *fakeDatabaseConnection) Ping(context.Context) error { return connection.pingErr }

func (connection *fakeDatabaseConnection) Query(context.Context, string, ...any) (*databaseRows, error) {
	queryErr := connection.queryErr
	if len(connection.queryErrors) > 0 {
		queryErr = nextScriptedError(&connection.queryErrors)
	}
	if queryErr != nil {
		return nil, queryErr
	}
	if len(connection.rowsResults) > 0 {
		rows := connection.rowsResults[0]
		connection.rowsResults = connection.rowsResults[1:]
		return newDatabaseRows(rows), nil
	}
	if connection.rowsResult == nil {
		connection.rowsResult = &fakeDatabaseRows{}
	}
	return newDatabaseRows(connection.rowsResult), nil
}

func (connection *fakeDatabaseConnection) QueryRow(_ context.Context, statement string, arguments ...any) databaseRow {
	connection.queries = append(connection.queries, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	return databaseRow{scanner: nextScriptedRow(&connection.rowResults)}
}

type fakeRow struct {
	err  error
	scan func(destinations ...any)
}

func (row fakeRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if row.scan != nil {
		row.scan(destinations...)
	}
	return nil
}

type fakeDatabaseRows struct {
	remaining int
	scanErr   error
	rowsErr   error
}

func (*fakeDatabaseRows) Close() {}

func (rows *fakeDatabaseRows) Err() error { return rows.rowsErr }

func (rows *fakeDatabaseRows) Next() bool {
	if rows.remaining == 0 {
		return false
	}
	rows.remaining--
	return true
}

func (rows *fakeDatabaseRows) Scan(...any) error { return rows.scanErr }

func nextScriptedError(scriptedErrors *[]error) error {
	if len(*scriptedErrors) == 0 {
		return nil
	}
	err := (*scriptedErrors)[0]
	*scriptedErrors = (*scriptedErrors)[1:]
	return err
}

func nextScriptedRow(scriptedRows *[]fakeRow) fakeRow {
	if len(*scriptedRows) == 0 {
		return fakeRow{err: pgx.ErrNoRows}
	}
	row := (*scriptedRows)[0]
	*scriptedRows = (*scriptedRows)[1:]
	return row
}

func destination[valueType any](t *testing.T, destinationValue any) *valueType {
	t.Helper()
	typedDestination, ok := destinationValue.(*valueType)
	if !ok {
		t.Fatalf("unexpected scan destination type %T", destinationValue)
	}
	return typedDestination
}
