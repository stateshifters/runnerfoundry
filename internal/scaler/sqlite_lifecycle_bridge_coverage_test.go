package scaler

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteBulkMirrorReconciliationReportsScanFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT repository").WillReturnRows(sqlmock.NewRows(
		[]string{"repository", "provider", "runner_name", "request_id", "phase"},
	).AddRow("owner/repo", "libvirt", "linux-42", "invalid", "reap_pending"))
	if err := reconcileSQLiteBulkReapMirrors(
		context.Background(), transaction, "linux", "owner/repo", "expired",
	); err == nil {
		t.Fatal("expected bulk lifecycle scan failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteBulkMirrorReconciliationReportsRowFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT repository").WillReturnRows(sqlmock.NewRows(
		[]string{"repository", "provider", "runner_name", "request_id", "phase"},
	).AddRow("owner/repo", "libvirt", "linux-42", 42, "reap_pending").RowError(0, errors.New("row failed")))
	if err := reconcileSQLiteBulkReapMirrors(
		context.Background(), transaction, "linux", "owner/repo", "expired",
	); err == nil {
		t.Fatal("expected bulk lifecycle row failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteAuthoritativeRunnerRejectsInvalidIdentity(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	_, err := loadSQLiteAuthoritativeRunner(
		context.Background(), transaction, "linux", "owner/repo", RunnerIdentity{},
	)
	if !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("authoritative lookup error = %v, want invalid identity", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteMirrorReconciliationReportsInspectionFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnError(errors.New("inspect failed"))
	err := reconcileSQLiteMirrorForRunner(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", Provider: RunnerProviderLibvirt, RunnerName: "linux-42",
	})
	if err == nil {
		t.Fatal("expected mirror inspection failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteMirrorReconciliationRejectsConflictingRequest(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0),
	)
	fixture.mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"request_id"}).AddRow(43),
	)
	err := reconcileSQLiteMirrorForRunner(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", Provider: RunnerProviderLibvirt,
		RunnerName: "linux-42", RequestID: 42,
	})
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("mirror reconciliation error = %v, want lifecycle consistency", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteTerminalMirrorValidationReportsInspectionFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnError(errors.New("inspect failed"))
	err := validateSQLiteCompatibleMirror(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", Provider: RunnerProviderLibvirt, RunnerName: "linux-42",
	})
	if err == nil {
		t.Fatal("expected terminal mirror inspection failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteTerminalMirrorValidationReportsOwnershipFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnError(errors.New("inspect failed"))
	err := validateSQLiteCompatibleMirror(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", Provider: RunnerProviderLibvirt,
		RunnerName: "linux-42", RequestID: 42,
	})
	if err == nil {
		t.Fatal("expected terminal mirror ownership failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteTerminalMirrorValidationRejectsConflictingRequest(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	expectNoSQLiteMirrorRequestOwner(fixture.mock)
	fixture.mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"request_id"}).AddRow(43),
	)
	err := validateSQLiteCompatibleMirror(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", Provider: RunnerProviderLibvirt,
		RunnerName: "linux-42", RequestID: 42,
	})
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("terminal mirror error = %v, want lifecycle consistency", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteMirrorOwnershipReportsQueryFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnError(errors.New("inspect failed"))
	err := validateSQLiteMirrorRequestOwnership(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", RunnerName: "linux-42", RequestID: 42,
	})
	if err == nil {
		t.Fatal("expected mirror ownership query failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func expectNoSQLiteMirrorRequestOwner(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0),
	)
}
