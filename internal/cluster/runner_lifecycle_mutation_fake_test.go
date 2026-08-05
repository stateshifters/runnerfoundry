package cluster

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

var errHermeticLifecycleMutation = errors.New("injected lifecycle mutation failure")

type hermeticLifecycleMutationState struct {
	failureAt          int
	committedMutations int
}

func (state *hermeticLifecycleMutationState) connection() (*fakeDatabaseConnection, *fakeDatabaseTransaction) {
	candidateMutations := state.committedMutations
	transaction := &fakeDatabaseTransaction{}
	transaction.queryRowHook = func(statement string, _ ...any) databaseRow {
		if strings.Contains(statement, "cluster_terminal_lifecycle_authority") {
			return databaseRow{scanner: terminalAuthorityRow(testRunnerLifecycle(), 0, 0)}
		}
		return databaseRow{scanner: fakeRow{}}
	}
	transaction.execHook = func(_ string, _ ...any) (pgconn.CommandTag, error) {
		candidateMutations++
		if candidateMutations == state.failureAt {
			return pgconn.CommandTag{}, fmt.Errorf("mutation %d: %w", candidateMutations, errHermeticLifecycleMutation)
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	transaction.commitHook = func() error {
		state.committedMutations = candidateMutations
		return nil
	}
	return &fakeDatabaseConnection{transaction: transaction}, transaction
}

func terminalAuthorityRow(runner scaler.RunnerLifecycle, nameConflicts, requestOwners int64) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], runner.Provider)
		setFakeDestination(destinations[1], runner.RunnerName)
		setFakeDestination(destinations[2], runner.RequestID)
		setFakeDestination(destinations[3], nameConflicts)
		setFakeDestination(destinations[4], requestOwners)
	}}
}
