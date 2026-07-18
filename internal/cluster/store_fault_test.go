package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestStoreSurfacesConnectionFailuresWithoutCredentials(t *testing.T) {
	connectionErr := errors.New("connection failed")
	store := NewStore(ConnectionSettings{})
	store.connector = func(context.Context) (databaseConnection, error) { return nil, connectionErr }
	store.directConnector = store.connector
	ctx := context.Background()
	operations := []func() error{
		func() error { return store.Publish(ctx, "node", true, nil, time.Now()) },
		func() error { _, err := store.ScaleSets(ctx, time.Now(), time.Minute); return err },
		func() error { return store.Enqueue(ctx, Demand{}) },
		func() error { _, err := store.AssignOldest(ctx, "set", time.Now(), time.Minute); return err },
		func() error { _, err := store.ClaimReady(ctx, "node"); return err },
		func() error { return store.RemoveDemand(ctx, "set", "repo", 1) },
		func() error { return store.Ready(ctx, 1, 2, 3) },
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
	for _, source := range []ConnectionSettings{
		{Password: "file:///missing-postgresql-secret", Role: "github_scaler"},
		{DSN: "postgres://secret-user:secret-password@127.0.0.1:1/database?connect_timeout=1", Role: "github_scaler"},
	} {
		store := NewStore(source)
		if err := store.Migrate(context.Background()); err == nil {
			t.Fatal("expected PostgreSQL connection setup failure")
		}
		if _, err := store.MigrationStatus(context.Background()); err == nil {
			t.Fatal("expected PostgreSQL status connection setup failure")
		}
		if err := store.MigrateDown(context.Background(), 0, time.Now(), time.Minute); err == nil {
			t.Fatal("expected PostgreSQL rollback connection setup failure")
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

func TestStoreSurfacesDatabaseOperationFailures(t *testing.T) {
	ctx := context.Background()
	databaseErr := errors.New("database failed")
	tests := []struct {
		name       string
		connection *fakeDatabaseConnection
		operation  func(store *Store) error
	}{
		{name: "publish begin", connection: &fakeDatabaseConnection{beginErr: databaseErr}, operation: func(store *Store) error { return store.Publish(ctx, "node", true, nil, time.Now()) }},
		{name: "scale sets query", connection: &fakeDatabaseConnection{queryErr: databaseErr}, operation: func(store *Store) error { _, err := store.ScaleSets(ctx, time.Now(), time.Minute); return err }},
		{name: "enqueue", connection: &fakeDatabaseConnection{execErrors: []error{databaseErr}}, operation: func(store *Store) error { return store.Enqueue(ctx, Demand{}) }},
		{name: "assignment begin", connection: &fakeDatabaseConnection{beginErr: databaseErr}, operation: func(store *Store) error {
			_, err := store.AssignOldest(ctx, "set", time.Now(), time.Minute)
			return err
		}},
		{name: "claim ready", connection: &fakeDatabaseConnection{rows: []databaseRow{fakeRow{err: databaseErr}}}, operation: func(store *Store) error { _, err := store.ClaimReady(ctx, "node"); return err }},
		{name: "remove demand", connection: &fakeDatabaseConnection{execErrors: []error{databaseErr}}, operation: func(store *Store) error { return store.RemoveDemand(ctx, "set", "repo", 1) }},
		{name: "assignment update", connection: &fakeDatabaseConnection{execErrors: []error{databaseErr}}, operation: func(store *Store) error { return store.Retry(ctx, 1) }},
		{name: "leader query", connection: &fakeDatabaseConnection{rows: []databaseRow{fakeRow{err: databaseErr}}}, operation: func(store *Store) error { _, err := store.TryLeader(ctx); return err }},
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
		{name: "commit", execErrors: []error{nil, nil, nil, nil}, commitErr: databaseErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{execErrors: test.execErrors, commitErr: test.commitErr}
			store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
			if err := store.Publish(context.Background(), "node", true, contribution, time.Now()); err == nil {
				t.Fatal("expected publication failure")
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
		rows        []databaseRow
		commitErr   error
		expectedErr error
	}{
		{name: "release unavailable", execErrors: []error{databaseErr}},
		{name: "oldest absent", execErrors: []error{nil}, rows: []databaseRow{fakeRow{err: pgx.ErrNoRows}}, expectedErr: ErrNoAssignment},
		{name: "oldest query", execErrors: []error{nil}, rows: []databaseRow{fakeRow{err: databaseErr}}},
		{name: "contributor absent", execErrors: []error{nil}, rows: []databaseRow{oldest, fakeRow{err: pgx.ErrNoRows}}, expectedErr: ErrNoAssignment},
		{name: "contributor query", execErrors: []error{nil}, rows: []databaseRow{oldest, fakeRow{err: databaseErr}}},
		{name: "assignment insert", execErrors: []error{nil}, rows: []databaseRow{oldest, contributor, fakeRow{err: databaseErr}}},
		{name: "commit", execErrors: []error{nil}, rows: []databaseRow{oldest, contributor, identifier}, commitErr: databaseErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{execErrors: test.execErrors, rows: test.rows, commitErr: test.commitErr}
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

func TestStoreRowAndLeaseEdgeCases(t *testing.T) {
	databaseErr := errors.New("database failed")
	if _, err := scanScaleSetStatuses(&fakeDatabaseRows{scanErr: databaseErr, remaining: 1}); err == nil {
		t.Fatal("expected scale-set scan failure")
	}
	if _, err := scanScaleSetStatuses(&fakeDatabaseRows{rowsErr: databaseErr}); err == nil {
		t.Fatal("expected scale-set row iteration failure")
	}
	store := storeWithConnection(&fakeDatabaseConnection{rows: []databaseRow{fakeRow{scan: func(destinations ...any) {
		*destination[bool](t, destinations[0]) = false
	}}}})
	if _, err := store.TryLeader(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("leader result = %v", err)
	}
	store = storeWithConnection(&fakeDatabaseConnection{rows: []databaseRow{fakeRow{err: pgx.ErrNoRows}}})
	if _, err := store.ClaimReady(context.Background(), "node"); !errors.Is(err, ErrNoReadyAssignment) {
		t.Fatalf("empty ready queue result = %v", err)
	}

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
	if (&LeaderLease{}).Alive(context.Background()) || (*LeaderLease)(nil).Alive(context.Background()) {
		t.Fatal("empty lease reported alive")
	}
	if err := (*LeaderLease)(nil).Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAssignmentStateHelpersSucceed(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	store := storeWithConnection(connection)
	ctx := context.Background()
	for _, operation := range []func() error{
		func() error { return store.Retry(ctx, 1) },
		func() error { return store.Release(ctx, 1) },
		func() error { return store.Complete(ctx, 1) },
	} {
		if err := operation(); err != nil {
			t.Fatal(err)
		}
	}
}

func storeWithConnection(connection databaseConnection) *Store {
	store := NewStore(ConnectionSettings{})
	store.connector = func(context.Context) (databaseConnection, error) { return connection, nil }
	store.directConnector = store.connector
	return store
}

type fakeDatabaseConnection struct {
	transaction *fakeDatabaseTransaction
	beginErr    error
	execErrors  []error
	queryErr    error
	rowsResult  databaseRows
	rows        []databaseRow
	closeErr    error
	pingErr     error
	closed      bool
}

func (connection *fakeDatabaseConnection) Begin(context.Context) (databaseTransaction, error) { //nolint:ireturn // Test fake implements the production boundary.
	if connection.beginErr != nil {
		return nil, connection.beginErr
	}
	if connection.transaction == nil {
		connection.transaction = &fakeDatabaseTransaction{}
	}
	return connection.transaction, nil
}

func (connection *fakeDatabaseConnection) Close(context.Context) error {
	connection.closed = true
	return connection.closeErr
}

func (connection *fakeDatabaseConnection) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, popError(&connection.execErrors)
}

func (connection *fakeDatabaseConnection) IsClosed() bool { return connection.closed }

func (connection *fakeDatabaseConnection) Ping(context.Context) error { return connection.pingErr }

func (connection *fakeDatabaseConnection) Query(context.Context, string, ...any) (databaseRows, error) { //nolint:ireturn // Test fake implements the production boundary.
	if connection.queryErr != nil {
		return nil, connection.queryErr
	}
	if connection.rowsResult == nil {
		connection.rowsResult = &fakeDatabaseRows{}
	}
	return connection.rowsResult, nil
}

func (connection *fakeDatabaseConnection) QueryRow(context.Context, string, ...any) databaseRow { //nolint:ireturn // Test fake implements the production boundary.
	if len(connection.rows) == 0 {
		return fakeRow{err: pgx.ErrNoRows}
	}
	row := connection.rows[0]
	connection.rows = connection.rows[1:]
	return row
}

type fakeDatabaseTransaction struct {
	execErrors  []error
	rows        []databaseRow
	commitErr   error
	rollbackErr error
}

func (transaction *fakeDatabaseTransaction) Commit(context.Context) error {
	return transaction.commitErr
}

func (transaction *fakeDatabaseTransaction) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, popError(&transaction.execErrors)
}

func (transaction *fakeDatabaseTransaction) QueryRow(context.Context, string, ...any) databaseRow { //nolint:ireturn // Test fake implements the production boundary.
	if len(transaction.rows) == 0 {
		return fakeRow{err: pgx.ErrNoRows}
	}
	row := transaction.rows[0]
	transaction.rows = transaction.rows[1:]
	return row
}

func (transaction *fakeDatabaseTransaction) Rollback(context.Context) error {
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

func popError(errorsQueue *[]error) error {
	if len(*errorsQueue) == 0 {
		return nil
	}
	err := (*errorsQueue)[0]
	*errorsQueue = (*errorsQueue)[1:]
	return err
}

func destination[valueType any](t *testing.T, destinationValue any) *valueType {
	t.Helper()
	typedDestination, ok := destinationValue.(*valueType)
	if !ok {
		t.Fatalf("unexpected scan destination type %T", destinationValue)
	}
	return typedDestination
}
