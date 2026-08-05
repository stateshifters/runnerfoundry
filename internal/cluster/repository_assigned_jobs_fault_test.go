package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestRemovedInstallationAssignedCleanupReportsEveryWriteFailure(t *testing.T) {
	update := postgreSQLDiscoveryUpdate{appID: 7, generation: 2, completedAt: time.Unix(1, 0).UTC()}
	assertAssignedCleanupWriteFailures(t, 3, func(transaction *databaseTransaction) error {
		return withdrawRemovedInstallationAssignedObservations(context.Background(), transaction, update)
	})
}

func TestUndiscoveredRepositoryAssignedCleanupReportsEveryWriteFailure(t *testing.T) {
	assertAssignedCleanupWriteFailures(t, 3, func(transaction *databaseTransaction) error {
		return withdrawUndiscoveredAssignedObservations(
			context.Background(), transaction, 7, githubapp.InstallationID(11), 2, time.Unix(1, 0).UTC(),
		)
	})
}

func TestRegistrationAssignedCleanupReportsEveryWriteFailure(t *testing.T) {
	assertAssignedCleanupWriteFailures(t, 3, func(transaction *databaseTransaction) error {
		return withdrawRegistrationAssignedObservation(
			context.Background(), transaction, 7, "repo", "linux", time.Unix(1, 0).UTC(),
		)
	})
}

func assertAssignedCleanupWriteFailures(
	t *testing.T,
	executionCount int,
	operation func(*databaseTransaction) error,
) {
	t.Helper()
	failure := errors.New("failed")
	for failureIndex := range executionCount {
		t.Run(fmt.Sprintf("write %d", failureIndex), func(t *testing.T) {
			executionErrors := make([]error, executionCount)
			executionErrors[failureIndex] = failure
			if err := operation((&fakeDatabaseTransaction{execErrors: executionErrors}).databaseTransaction()); err == nil {
				t.Fatal("expected assigned cleanup write failure")
			}
		})
	}
}

func TestRepositoryReplacementReportsRecordFailure(t *testing.T) {
	update := repositoryReplacementFixture()
	update.repositories = []repositorystate.Repository{{URL: "repo", InstallationID: 11}}
	assertRepositoryReplacementFailure(t, 1, func(transaction *databaseTransaction) error {
		return replacePostgreSQLRepositories(context.Background(), transaction, update)
	})
}

func TestRepositoryReplacementReportsAssignedCleanupFailure(t *testing.T) {
	update := repositoryReplacementFixture()
	assertRepositoryReplacementFailure(t, 1, func(transaction *databaseTransaction) error {
		return replacePostgreSQLRepositories(context.Background(), transaction, update)
	})
}

func TestRepositoryReplacementReportsDeletionFailure(t *testing.T) {
	update := repositoryReplacementFixture()
	assertRepositoryReplacementFailure(t, 4, func(transaction *databaseTransaction) error {
		return replacePostgreSQLRepositories(context.Background(), transaction, update)
	})
}

func TestInstallationReplacementReportsDeletionFailure(t *testing.T) {
	update := repositoryReplacementFixture()
	update.installationIDs = nil
	assertRepositoryReplacementFailure(t, 4, func(transaction *databaseTransaction) error {
		return replacePostgreSQLInstallations(context.Background(), transaction, update)
	})
}

func repositoryReplacementFixture() postgreSQLDiscoveryUpdate {
	return postgreSQLDiscoveryUpdate{
		appID: 7, generation: 2, installationIDs: []githubapp.InstallationID{11}, completedAt: time.Unix(1, 0).UTC(),
	}
}

func assertRepositoryReplacementFailure(
	t *testing.T,
	executionCount int,
	operation func(*databaseTransaction) error,
) {
	t.Helper()
	executionErrors := make([]error, executionCount)
	executionErrors[executionCount-1] = errors.New("failed")
	if err := operation((&fakeDatabaseTransaction{execErrors: executionErrors}).databaseTransaction()); err == nil {
		t.Fatal("expected repository replacement failure")
	}
}
