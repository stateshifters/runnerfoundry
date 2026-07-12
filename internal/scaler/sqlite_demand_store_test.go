package scaler

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteDemandStorePreservesFIFOAcrossReopen(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	if err := store.Apply(context.Background(), "linux", []JobDemand{
		{Repository: "newer", RequestID: 2, QueueTime: queueTime.Add(time.Minute)},
		{Repository: "older", RequestID: 1, QueueTime: queueTime},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	ordered, err := reopened.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 2 || ordered[0].RequestID != 1 || ordered[1].RequestID != 2 {
		t.Fatalf("expected persisted FIFO demand, got %#v", ordered)
	}
}

func TestSQLiteDemandStoreReopensTwoHundredQueuedJobs(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	observed := make([]JobDemand, 200)
	for index := range observed {
		observed[index] = JobDemand{Repository: "repo", RequestID: int64(200 - index), QueueTime: queueTime.Add(time.Duration(200-index) * time.Second)}
	}
	if err := store.Apply(context.Background(), "linux", observed, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	ordered, err := reopened.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 200 || ordered[0].RequestID != 1 || ordered[199].RequestID != 200 {
		t.Fatalf("unexpected recovered queue boundaries: %d %#v %#v", len(ordered), ordered[0], ordered[len(ordered)-1])
	}
}

func TestSQLiteDemandStoreRejectsSecondOwner(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	first, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer first.Close()
	if _, err := OpenSQLiteDemandStore(databasePath); err == nil {
		t.Fatal("expected second state owner to be rejected")
	}
}

func TestSQLiteDemandStoreMutationsAreDurableAndIdempotent(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer store.Close()
	ctx := context.Background()
	demand := JobDemand{Repository: "repo", RequestID: 7, QueueTime: time.Now()}
	if err := store.Apply(ctx, "linux", []JobDemand{demand, demand}, nil); err != nil {
		t.Fatal(err)
	}
	demand.Acquired = true
	if err := store.Apply(ctx, "linux", []JobDemand{demand}, nil); err != nil {
		t.Fatal(err)
	}
	ordered, err := store.Ordered(ctx, "linux")
	if err != nil || len(ordered) != 1 || !ordered[0].Acquired {
		t.Fatalf("expected acquired deduplicated demand, got %#v, %v", ordered, err)
	}
	if err := store.Delete(ctx, "linux", "repo", 7); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{demand}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveRepository(ctx, "linux", "repo"); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{demand}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveScaleSet(ctx, "linux"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDemandStoreRejectsFutureAndCorruptDatabases(t *testing.T) {
	futurePath := filepath.Join(t.TempDir(), "future.db")
	database, err := sql.Open("sqlite", futurePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteDemandStore(futurePath); err == nil {
		t.Fatal("expected future schema rejection")
	}
	corruptPath := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(corruptPath, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteDemandStore(corruptPath); err == nil {
		t.Fatal("expected corrupt database rejection")
	}
}

func TestSQLiteDemandStoreReportsOperationsAfterClose(t *testing.T) {
	store, openErr := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err == nil {
		t.Fatal("expected apply error")
	}
	if _, err := store.Ordered(ctx, "linux"); err == nil {
		t.Fatal("expected query error")
	}
	if err := store.RemoveRepository(ctx, "linux", "repo"); err == nil {
		t.Fatal("expected repository removal error")
	}
	if err := store.RemoveScaleSet(ctx, "linux"); err == nil {
		t.Fatal("expected scale set removal error")
	}
	if err := store.Delete(ctx, "linux", "repo", 1); err == nil {
		t.Fatal("expected delete error")
	}
	if err := store.migrate(ctx); err == nil {
		t.Fatal("expected migration error")
	}
}

func TestSQLiteDemandStoreReportsMutationFailures(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if _, err := store.database.Exec(`CREATE TRIGGER reject_insert BEFORE INSERT ON demand BEGIN SELECT RAISE(FAIL, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err == nil {
		t.Fatal("expected insert failure")
	}
	if _, err := store.database.Exec("DROP TRIGGER reject_insert"); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`CREATE TRIGGER reject_delete BEFORE DELETE ON demand BEGIN SELECT RAISE(FAIL, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", nil, []DemandRemoval{{Repository: "repo", RequestID: 1}}); err == nil {
		t.Fatal("expected removal failure")
	}
	if _, err := store.database.Exec("DROP TRIGGER reject_delete"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec("PRAGMA user_version = 0"); err != nil {
		t.Fatal(err)
	}
	if err := store.migrate(ctx); err == nil {
		t.Fatal("expected duplicate table migration failure")
	}
}

func TestSQLiteDemandStoreRejectsUnavailableDirectory(t *testing.T) {
	if _, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "missing", "state.db")); err == nil {
		t.Fatal("expected lock path failure")
	}
}

func TestSQLiteDemandStoreReleasesLockWhenDatabaseOpenFails(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	if _, err := createSQLiteDemandStore(databasePath, func(string) (*sql.DB, error) { return nil, errors.New("open failed") }); err == nil {
		t.Fatal("expected database open failure")
	}
	store, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatalf("expected released lock, got %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDemandStoreReportsDatabaseProtocolFailures(t *testing.T) {
	t.Run("schema version read", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("PRAGMA user_version").WillReturnError(errors.New("read failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected schema read error")
		}
	})
	t.Run("migration begin", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		expectSchemaVersion(mock, 0)
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected migration begin error")
		}
	})
	t.Run("schema version write", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		expectSchemaVersion(mock, 0)
		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version").WillReturnError(errors.New("write failed"))
		mock.ExpectRollback()
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected schema write error")
		}
	})
	t.Run("migration commit", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		expectSchemaVersion(mock, 0)
		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected migration commit error")
		}
	})
	t.Run("demand commit", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		mock.ExpectBegin()
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		if err := store.Apply(context.Background(), "linux", nil, nil); err == nil {
			t.Fatal("expected demand commit error")
		}
	})
	t.Run("demand scan", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		rows := sqlmock.NewRows([]string{"repository", "request_id", "queue_time", "acquired", "observation_order"}).AddRow("repo", "invalid", 0, false, 1)
		mock.ExpectQuery("SELECT repository").WillReturnRows(rows)
		if _, err := store.Ordered(context.Background(), "linux"); err == nil {
			t.Fatal("expected scan error")
		}
	})
	t.Run("demand iteration", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		rows := sqlmock.NewRows([]string{"repository", "request_id", "queue_time", "acquired", "observation_order"}).AddRow("repo", 1, 0, false, 1).RowError(0, errors.New("iteration failed"))
		mock.ExpectQuery("SELECT repository").WillReturnRows(rows)
		if _, err := store.Ordered(context.Background(), "linux"); err == nil {
			t.Fatal("expected iteration error")
		}
	})
}

func mockDemandStore(t *testing.T) (*SQLiteDemandStore, sqlmock.Sqlmock) { //nolint:ireturn // sqlmock exposes its expectation API through this interface.
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	return &SQLiteDemandStore{database: database}, mock
}

func expectSchemaVersion(mock sqlmock.Sqlmock, version int) {
	mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(version))
}
