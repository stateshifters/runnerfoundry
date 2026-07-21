package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestStoreSurfacesConnectionFailuresWithoutCredentials(t *testing.T) {
	connectionErr := errors.New("connection failed")
	store := NewStore(ConnectionSettings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, connectionErr }
	store.directConnector = func(context.Context) (leaderConnection, error) { return nil, connectionErr }
	ctx := context.Background()
	operations := []func() error{
		func() error { return store.Publish(ctx, "node", NodeHealthy, nil, time.Now()) },
		func() error { _, err := store.ScaleSets(ctx, time.Now(), time.Minute); return err },
		func() error {
			return store.Enqueue(ctx, Demand{ScaleSet: "set", Repository: "repo", RequestID: 1, QueueTime: time.Now()})
		},
		func() error { _, err := store.AssignOldest(ctx, "set", time.Now(), time.Minute); return err },
		func() error { _, err := store.ClaimForStart(ctx, "node"); return err },
		func() error { _, err := store.PendingAcquisitions(ctx); return err },
		func() error { _, err := store.Starting(ctx, "node"); return err },
		func() error { return store.CompleteDemand(ctx, "set", "repo", 1) },
		func() error { return store.BeginAcquisition(ctx, 1, 2, 3) },
		func() error { return store.ConfirmAcquisition(ctx, 1) },
		func() error { return store.ConfirmRunning(ctx, 1, "node") },
		func() error { return store.CompleteAssignment(ctx, 1, "node", AssignmentReserved) },
		func() error { _, err := store.TryLeader(ctx); return err },
		func() error { _, err := store.WaitForLeader(ctx); return err },
	}
	for operationIndex, operation := range operations {
		if !errors.Is(operation(), connectionErr) {
			t.Fatalf("operation %d did not return connector failure", operationIndex)
		}
	}
}

func TestPostgreSQLConnectorRedactsResolutionAndConnectionFailures(t *testing.T) {
	for _, test := range []struct {
		settings  ConnectionSettings
		forbidden []string
	}{
		{
			settings:  ConnectionSettings{Password: "file:///missing-postgresql-secret", Role: "github_scaler"},
			forbidden: []string{"missing-postgresql-secret", "github_scaler"},
		},
		{
			settings: ConnectionSettings{
				DSN: "postgres://secret-user:secret-password@127.0.0.1:1/database?connect_timeout=1", Role: "github_scaler",
			},
			forbidden: []string{"secret-user", "secret-password", "github_scaler", "postgres://"},
		},
	} {
		store := NewStore(test.settings)
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
			for _, forbidden := range test.forbidden {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("connection error exposed forbidden value %q: %v", forbidden, err)
				}
			}
		}
	}
}

func TestStoreDefaultConnectorsRejectInvalidSettings(t *testing.T) {
	store := NewStore(ConnectionSettings{Password: "env://MISSING_PRIMARY_PASSWORD", Role: "github_scaler"})
	if _, err := store.connectPostgreSQL(context.Background()); err == nil {
		t.Fatal("expected primary connector resolution failure")
	}
	if _, err := store.directConnector(context.Background()); err == nil {
		t.Fatal("expected direct connector resolution failure")
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
			store := NewStore(ConnectionSettings{})
			store.connector = func(context.Context) (*databaseConnection, error) {
				t.Fatal("invalid demand reached the database connector")
				return nil, errors.New("unreachable connector")
			}
			if err := store.Enqueue(context.Background(), test.demand); !errors.Is(err, ErrInvalidDemand) {
				t.Fatalf("Enqueue() error = %v, want %v", err, ErrInvalidDemand)
			}
		})
	}
	store := NewStore(ConnectionSettings{})
	store.connector = func(context.Context) (*databaseConnection, error) {
		t.Fatal("invalid demand operation reached the database connector")
		return nil, errors.New("unreachable connector")
	}
	if _, err := store.AssignOldest(context.Background(), " \t", time.Now(), time.Minute); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("AssignOldest() error = %v, want %v", err, ErrInvalidDemand)
	}
	if err := store.CompleteDemand(context.Background(), "set", " \t", 1); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("CompleteDemand() error = %v, want %v", err, ErrInvalidDemand)
	}
}

func TestPublishRejectsInvalidContributionsBeforeConnecting(t *testing.T) {
	tests := []struct {
		name         string
		nodeID       string
		contribution Contribution
	}{
		{name: "blank node", nodeID: " \t", contribution: Contribution{ScaleSet: "set", RunnerGroup: "group"}},
		{name: "blank scale set", nodeID: "node", contribution: Contribution{ScaleSet: " \t", RunnerGroup: "group"}},
		{name: "blank runner group", nodeID: "node", contribution: Contribution{ScaleSet: "set", RunnerGroup: " \t"}},
		{name: "negative capacity", nodeID: "node", contribution: Contribution{ScaleSet: "set", RunnerGroup: "group", MaxRunners: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewStore(ConnectionSettings{})
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

func TestStoreSurfacesDatabaseOperationFailures(t *testing.T) {
	ctx := context.Background()
	databaseErr := errors.New("database failed")
	tests := []struct {
		name       string
		connection *fakeDatabaseConnection
		operation  func(store *Store) error
	}{
		{name: "publish begin", connection: &fakeDatabaseConnection{beginErr: databaseErr}, operation: func(store *Store) error { return store.Publish(ctx, "node", NodeHealthy, nil, time.Now()) }},
		{name: "scale sets query", connection: &fakeDatabaseConnection{queryErr: databaseErr}, operation: func(store *Store) error { _, err := store.ScaleSets(ctx, time.Now(), time.Minute); return err }},
		{name: "enqueue", connection: &fakeDatabaseConnection{execErrors: []error{databaseErr}}, operation: func(store *Store) error {
			return store.Enqueue(ctx, Demand{ScaleSet: "set", Repository: "repo", RequestID: 1, QueueTime: time.Now()})
		}},
		{name: "assignment begin", connection: &fakeDatabaseConnection{beginErr: databaseErr}, operation: func(store *Store) error {
			_, err := store.AssignOldest(ctx, "set", time.Now(), time.Minute)
			return err
		}},
		{name: "claim ready", connection: &fakeDatabaseConnection{rowResults: []fakeRow{{err: databaseErr}}}, operation: func(store *Store) error { _, err := store.ClaimForStart(ctx, "node"); return err }},
		{name: "load recovery", connection: &fakeDatabaseConnection{queryErr: databaseErr}, operation: func(store *Store) error { _, err := store.PendingAcquisitions(ctx); return err }},
		{name: "complete demand", connection: &fakeDatabaseConnection{beginErr: databaseErr}, operation: func(store *Store) error { return store.CompleteDemand(ctx, "set", "repo", 1) }},
		{name: "assignment update", connection: &fakeDatabaseConnection{execErrors: []error{databaseErr}}, operation: func(store *Store) error { return store.ConfirmAcquisition(ctx, 1) }},
		{name: "leader query", connection: &fakeDatabaseConnection{rowResults: []fakeRow{{err: databaseErr}}}, operation: func(store *Store) error { _, err := store.TryLeader(ctx); return err }},
		{name: "leader wait", connection: &fakeDatabaseConnection{execErrors: []error{databaseErr}}, operation: func(store *Store) error { _, err := store.WaitForLeader(ctx); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := storeWithConnection(test.connection)
			if err := test.operation(store); err == nil {
				t.Fatal("expected database operation failure")
			}
		})
	}
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
	store := storeWithConnection(&fakeDatabaseConnection{execErrors: []error{errors.New("database failed")}})
	if _, err := store.WaitForLeader(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
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

func TestAssignmentTransactionReportsEveryFailure(t *testing.T) {
	databaseErr := errors.New("database failed")
	now := time.Now()
	oldest := fakeRow{scan: func(destinations ...any) {
		*destination[int64](t, destinations[0]) = 1
		*destination[string](t, destinations[1]) = "repo"
		*destination[int64](t, destinations[2]) = 2
		*destination[time.Time](t, destinations[3]) = now
	}}
	contributor := fakeRow{scan: func(destinations ...any) { *destination[string](t, destinations[0]) = "node" }}
	identifier := fakeRow{scan: func(destinations ...any) { *destination[int64](t, destinations[0]) = 3 }}
	tests := []struct {
		name        string
		execErrors  []error
		rowResults  []fakeRow
		commitErr   error
		expectedErr error
	}{
		{name: "release unavailable", execErrors: []error{databaseErr}},
		{name: "oldest absent", rowResults: []fakeRow{{err: pgx.ErrNoRows}}, expectedErr: ErrNoAssignment},
		{name: "oldest query", rowResults: []fakeRow{{err: databaseErr}}},
		{name: "contributor absent", rowResults: []fakeRow{oldest, {err: pgx.ErrNoRows}}, expectedErr: ErrNoAssignment},
		{name: "contributor query", rowResults: []fakeRow{oldest, {err: databaseErr}}},
		{name: "assignment insert", rowResults: []fakeRow{oldest, contributor, {err: databaseErr}}},
		{name: "commit", rowResults: []fakeRow{oldest, contributor, identifier}, commitErr: databaseErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{execErrors: test.execErrors, rowResults: test.rowResults, commitErr: test.commitErr}
			store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
			_, err := store.AssignOldest(context.Background(), "set", now, time.Minute)
			if test.expectedErr != nil && !errors.Is(err, test.expectedErr) {
				t.Fatalf("error = %v, want %v", err, test.expectedErr)
			}
			if test.expectedErr == nil && err == nil {
				t.Fatal("expected assignment failure")
			}
		})
	}
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
		ScaleSet: "set", Repository: "repo", RequestID: 1, QueueTime: time.Now(),
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
	store := NewStore(ConnectionSettings{})
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
	transaction *fakeDatabaseTransaction
	beginErr    error
	execErrors  []error
	execTag     pgconn.CommandTag
	queryErr    error
	rowsResult  rowIterator
	rowResults  []fakeRow
	closeErr    error
	pingErr     error
	closed      bool
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

func (connection *fakeDatabaseConnection) Close(context.Context) error {
	connection.closed = true
	return connection.closeErr
}

func (connection *fakeDatabaseConnection) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
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
	if connection.queryErr != nil {
		return nil, connection.queryErr
	}
	if connection.rowsResult == nil {
		connection.rowsResult = &fakeDatabaseRows{}
	}
	return newDatabaseRows(connection.rowsResult), nil
}

func (connection *fakeDatabaseConnection) QueryRow(context.Context, string, ...any) databaseRow {
	return databaseRow{scanner: nextScriptedRow(&connection.rowResults)}
}

type fakeDatabaseTransaction struct {
	execErrors    []error
	rowResults    []fakeRow
	commitErr     error
	rollbackErr   error
	rollbackCalls int
}

func (transaction *fakeDatabaseTransaction) databaseTransaction() *databaseTransaction {
	return &databaseTransaction{
		commit:   transaction.Commit,
		exec:     transaction.Exec,
		queryRow: transaction.QueryRow,
		rollback: transaction.Rollback,
	}
}

func (transaction *fakeDatabaseTransaction) Commit(context.Context) error {
	return transaction.commitErr
}

func (transaction *fakeDatabaseTransaction) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	if err := nextScriptedError(&transaction.execErrors); err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (transaction *fakeDatabaseTransaction) QueryRow(context.Context, string, ...any) databaseRow {
	return databaseRow{scanner: nextScriptedRow(&transaction.rowResults)}
}

func (transaction *fakeDatabaseTransaction) Rollback(context.Context) error {
	transaction.rollbackCalls++
	return transaction.rollbackErr
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
