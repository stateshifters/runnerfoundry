package scaler

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func expectSQLiteIdentityMatch(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
}

func seedSQLiteGenericLifecycle(t *testing.T, database *sql.DB, runner RunnerLifecycle) {
	t.Helper()
	if runner.UpdatedAt.IsZero() {
		runner.UpdatedAt = runner.CreatedAt
	}
	if runner.WaitingSince.IsZero() && (runner.Phase == RunnerWaitingForJob || runner.Phase == RunnerBusy) {
		runner.WaitingSince = runner.CreatedAt
	}
	if _, err := database.Exec(`INSERT INTO runner_lifecycle(scale_set, repository, provider, runner_name,
		request_id, phase, created_at, waiting_since, updated_at, reap_reason) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runner.ScaleSet, runner.Repository, runner.Provider, runner.RunnerName, runner.RequestID,
		runner.Phase, runner.CreatedAt.UnixNano(), runner.WaitingSince.UnixNano(), runner.UpdatedAt.UnixNano(),
		runner.ReapReason); err != nil {
		t.Fatal(err)
	}
}
