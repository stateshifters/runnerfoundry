package scaler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestSQLiteRepositoryControllerReportsTransactionFailures(t *testing.T) {
	for name, expectations := range sqliteRepositoryControllerFailureCases {
		t.Run(name, func(t *testing.T) {
			fixture := mockDemandStore(t)
			expectations(fixture.mock)
			if _, err := fixture.store.StartController(context.Background(), 7); err == nil {
				t.Fatal("expected controller failure")
			}
			assertSQLExpectations(t, fixture.mock)
		})
	}
}

var sqliteRepositoryControllerFailureCases = map[string]func(sqlmock.Sqlmock){
	"begin": func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(errors.New("failed")) },
	"initialize": func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO repository_controls").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	},
	"increment": func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE repository_controls").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	},
	"snapshot": func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT app_id").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	},
	"commit": func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		expectSQLiteRepositorySnapshot(mock, 7, 0, 1, false)
		mock.ExpectCommit().WillReturnError(errors.New("failed"))
	},
}

func TestSQLiteRepositoryDiscoveryReportsTransactionFailures(t *testing.T) {
	for name, testCase := range sqliteRepositoryDiscoveryFailureCases {
		t.Run(name, func(t *testing.T) {
			fixture := mockDemandStore(t)
			testCase.expect(fixture.mock)
			if _, err := fixture.store.ApplyDiscovery(context.Background(), 7, 1,
				testCase.installations, nil, nil, time.Unix(1, 0)); err == nil {
				t.Fatal("expected discovery failure")
			}
			assertSQLExpectations(t, fixture.mock)
		})
	}
}

type sqliteRepositoryDiscoveryFailureCase struct {
	expect        func(sqlmock.Sqlmock)
	installations []githubapp.InstallationID
}

var sqliteRepositoryDiscoveryFailureCases = map[string]sqliteRepositoryDiscoveryFailureCase{
	"begin": {expect: func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(errors.New("failed")) }},
	"lock": {expect: func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT app_id").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	}},
	"advance": {expect: func(mock sqlmock.Sqlmock) {
		expectSQLiteDiscoveryLock(mock)
		mock.ExpectExec("UPDATE repository_controls").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	}},
	"remove installations": {expect: func(mock sqlmock.Sqlmock) {
		expectSQLiteDiscoveryLock(mock)
		mock.ExpectExec("UPDATE repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM repository_installations").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	}},
	"remove repositories": {expect: func(mock sqlmock.Sqlmock) {
		expectSQLiteDiscoveryLock(mock)
		mock.ExpectExec("UPDATE repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO repository_installations").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM repository_installations").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM repositories").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	}, installations: []githubapp.InstallationID{11}},
	"snapshot": {expect: func(mock sqlmock.Sqlmock) {
		expectSQLiteDiscoveryLock(mock)
		mock.ExpectExec("UPDATE repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM repository_installations").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT app_id").WillReturnError(errors.New("failed"))
		mock.ExpectRollback()
	}},
	"commit": {expect: func(mock sqlmock.Sqlmock) {
		expectSQLiteDiscoveryLock(mock)
		mock.ExpectExec("UPDATE repository_controls").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM repository_installations").WillReturnResult(sqlmock.NewResult(0, 1))
		expectSQLiteRepositorySnapshot(mock, 7, 2, 1, true)
		mock.ExpectCommit().WillReturnError(errors.New("failed"))
	}},
}

func TestSQLiteRepositoryRegistrationReportsBeginAndCommitFailures(t *testing.T) {
	registration := repositorystate.Registration{RepositoryURL: "repo", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 9}
	for name, expectations := range map[string]func(sqlmock.Sqlmock){
		"begin": func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(errors.New("failed")) },
		"commit": func(mock sqlmock.Sqlmock) {
			expectSQLiteDiscoveryLock(mock)
			mock.ExpectExec("INSERT INTO repository_registrations").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(errors.New("failed"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := mockDemandStore(t)
			expectations(fixture.mock)
			if err := fixture.store.UpsertRegistration(context.Background(), 7, 1, registration); err == nil {
				t.Fatal("expected registration failure")
			}
			assertSQLExpectations(t, fixture.mock)
		})
	}
}

func TestSQLiteRepositoryRegistrationReportsDeleteFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSQLiteDiscoveryLock(fixture.mock)
	fixture.mock.ExpectExec("DELETE FROM repository_registrations").WillReturnError(errors.New("failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.DeleteRegistration(context.Background(), 7, 1, "repo", "linux"); err == nil {
		t.Fatal("expected registration delete failure")
	}
	assertSQLExpectations(t, fixture.mock)
}

func TestSQLiteRepositorySnapshotReportsQueryAndRowFailures(t *testing.T) {
	for name, expectations := range sqliteRepositorySnapshotFailureCases {
		t.Run(name, func(t *testing.T) {
			fixture := mockDemandStore(t)
			expectations(fixture.mock)
			if _, err := fixture.store.LoadSnapshot(context.Background(), 7); err == nil {
				t.Fatal("expected snapshot failure")
			}
			assertSQLExpectations(t, fixture.mock)
		})
	}
}

var sqliteRepositorySnapshotFailureCases = map[string]func(sqlmock.Sqlmock){
	"control": func(mock sqlmock.Sqlmock) { mock.ExpectQuery("SELECT app_id").WillReturnError(errors.New("failed")) },
	"repositories query": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		mock.ExpectQuery("SELECT app_id").WillReturnError(errors.New("failed"))
	},
	"repositories scan": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow(7))
	},
	"repositories close": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows([]string{"app_id"}).
			AddRow(7).AddRow(7).CloseError(errors.New("failed")))
	},
	"repositories rows": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows(
			[]string{"app_id", "repository_url", "installation_id", "last_seen_generation"},
		).AddRow(7, "repo", 11, 1).RowError(0, errors.New("failed")))
	},
	"registrations query": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		expectSQLiteEmptyRepositories(mock)
		mock.ExpectQuery("SELECT app_id").WillReturnError(errors.New("failed"))
	},
	"registrations scan": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		expectSQLiteEmptyRepositories(mock)
		mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow(7))
	},
	"registrations rows": func(mock sqlmock.Sqlmock) {
		expectSQLiteControl(mock, 7, 1, 1, true)
		expectSQLiteEmptyRepositories(mock)
		mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows(
			[]string{"app_id", "repository_url", "scale_set_name", "runner_group", "github_scale_set_id"},
		).AddRow(7, "repo", "linux", "Default", 9).RowError(0, errors.New("failed")))
	},
}

func expectSQLiteDiscoveryLock(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	expectSQLiteControl(mock, 7, 1, 1, true)
}

func expectSQLiteRepositorySnapshot(mock sqlmock.Sqlmock, appID int64, generation int64, epoch int64, initialized bool) {
	expectSQLiteControl(mock, appID, generation, epoch, initialized)
	expectSQLiteEmptyRepositories(mock)
	mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows(
		[]string{"app_id", "repository_url", "scale_set_name", "runner_group", "github_scale_set_id"},
	))
}

func expectSQLiteControl(mock sqlmock.Sqlmock, appID int64, generation int64, epoch int64, initialized bool) {
	mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows(
		[]string{"app_id", "discovery_generation", "last_successful_discovery", "controller_epoch", "initialized"},
	).AddRow(appID, generation, int64(1), epoch, initialized))
}

func expectSQLiteEmptyRepositories(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT app_id").WillReturnRows(sqlmock.NewRows(
		[]string{"app_id", "repository_url", "installation_id", "last_seen_generation"},
	))
}
