package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
	"github.com/meza/RunnerFoundry/internal/testsupport/runneridentitycontract"
)

func TestClusterRunnerIdentityContract(t *testing.T) {
	for _, contractCase := range runneridentitycontract.Cases() {
		t.Run(contractCase.Name, func(t *testing.T) {
			transaction := clusterRunnerIdentityContractTransaction(t, contractCase.Outcome)
			identity := scaler.RunnerIdentity{
				RequestID: contractCase.Identity.RequestID, RunnerName: contractCase.Identity.RunnerName,
			}
			err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).MarkRunnerBusy(
				context.Background(), "set", "repo", identity, 99, time.Unix(2, 0).UTC(),
			)
			want := map[runneridentitycontract.Outcome]error{
				runneridentitycontract.Invalid:   scaler.ErrInvalidRunnerIdentity,
				runneridentitycontract.NotFound:  scaler.ErrRunnerIdentityNotFound,
				runneridentitycontract.Mismatch:  scaler.ErrRunnerIdentityMismatch,
				runneridentitycontract.Invariant: scaler.ErrRunnerIdentityInvariant,
			}[contractCase.Outcome]
			if want == nil && err != nil {
				t.Fatal(err)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("contract error = %v, want %v", err, want)
			}
		})
	}
}

func clusterRunnerIdentityContractTransaction(
	t *testing.T,
	outcome runneridentitycontract.Outcome,
) *fakeDatabaseTransaction {
	t.Helper()
	switch outcome {
	case runneridentitycontract.Success, runneridentitycontract.Invalid:
		return &fakeDatabaseTransaction{}
	case runneridentitycontract.NotFound:
		return &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
	case runneridentitycontract.Mismatch:
		return &fakeDatabaseTransaction{
			execTags:   []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")},
			rowResults: []fakeRow{integerCountRow(t, 1), integerCountRow(t, 1)},
		}
	case runneridentitycontract.Invariant:
		return &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 2")}}
	}
	return &fakeDatabaseTransaction{}
}

func TestClusterRunnerIdentityTransitionsRejectEmptyIdentity(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{})
	operations := []func() error{
		func() error {
			return store.MarkRunnerBusy(context.Background(), "linux", "repo", scaler.RunnerIdentity{}, 1, time.Time{})
		},
		func() error {
			return store.BindRunnerIdentity(context.Background(), "linux", "repo", scaler.RunnerIdentity{}, 1, time.Time{})
		},
		func() error {
			return store.RequestRunnerReap(context.Background(), "linux", "repo", scaler.RunnerIdentity{}, "done", time.Time{})
		},
	}
	for index, operation := range operations {
		if err := operation(); !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("operation %d error = %v, want invalid identity", index, err)
		}
	}
}

func TestClusterRunnerIdentityTransitionsUseStrictModeSpecificPredicates(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		assertClusterIdentityPredicate(t, scaler.RunnerIdentity{RequestID: 42}, markClusterRunnerBusy,
			[]string{"request_id ="}, []string{"runner_name =", " OR "})
	})
	t.Run("name", func(t *testing.T) {
		assertClusterIdentityPredicate(t, scaler.RunnerIdentity{RunnerName: "linux-42"}, requestClusterRunnerReap,
			[]string{"runner_name ="}, []string{"request_id =", " OR "})
	})
	t.Run("pair", func(t *testing.T) {
		assertClusterIdentityPredicate(t, scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}, markClusterRunnerBusy,
			[]string{"request_id =", "runner_name =", " AND "}, []string{" OR "})
	})
}

func markClusterRunnerBusy(store *Store, identity scaler.RunnerIdentity) error {
	return store.MarkRunnerBusy(context.Background(), "linux", "repo", identity, 7, time.Unix(1, 0).UTC())
}

func requestClusterRunnerReap(store *Store, identity scaler.RunnerIdentity) error {
	return store.RequestRunnerReap(context.Background(), "linux", "repo", identity, "done", time.Unix(1, 0).UTC())
}

func assertClusterIdentityPredicate(
	t *testing.T,
	identity scaler.RunnerIdentity,
	operation func(*Store, scaler.RunnerIdentity) error,
	want, dontWant []string,
) {
	t.Helper()
	transaction := &fakeDatabaseTransaction{}
	if err := operation(storeWithConnection(&fakeDatabaseConnection{transaction: transaction}), identity); err != nil {
		t.Fatal(err)
	}
	statement := transaction.executions[0].statement
	assertStatementContains(t, statement, want)
	assertStatementOmits(t, statement, dontWant)
}

func assertStatementContains(t *testing.T, statement string, fragments []string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(statement, fragment) {
			t.Fatalf("statement %q does not contain %q", statement, fragment)
		}
	}
}

func assertStatementOmits(t *testing.T, statement string, fragments []string) {
	t.Helper()
	for _, fragment := range fragments {
		if strings.Contains(statement, fragment) {
			t.Fatalf("statement %q unexpectedly contains %q", statement, fragment)
		}
	}
}

func TestClusterRunnerIdentityTransitionClassifiesZeroAndMultipleMatches(t *testing.T) {
	pair := scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}
	t.Run("single not found", func(t *testing.T) {
		assertClusterTransitionFailure(t, updateCountTransaction(0), scaler.RunnerIdentity{RequestID: 42},
			scaler.ErrRunnerIdentityNotFound)
	})
	t.Run("pair not found", func(t *testing.T) {
		assertClusterTransitionFailure(t, unresolvedPairTransaction(t, 0, 0), pair, scaler.ErrRunnerIdentityNotFound)
	})
	t.Run("pair mismatch", func(t *testing.T) {
		assertClusterTransitionFailure(t, unresolvedPairTransaction(t, 1, 0), pair, scaler.ErrRunnerIdentityMismatch)
	})
	t.Run("multiple exact", func(t *testing.T) {
		assertClusterTransitionFailure(t, updateCountTransaction(2), pair, scaler.ErrRunnerIdentityInvariant)
	})
}

func updateCountTransaction(count int64) *fakeDatabaseTransaction {
	return &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", count))}}
}

func unresolvedPairTransaction(t *testing.T, requestMatches, nameMatches int64) *fakeDatabaseTransaction {
	t.Helper()
	return &fakeDatabaseTransaction{
		execTags:   []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")},
		rowResults: []fakeRow{integerCountRow(t, requestMatches), integerCountRow(t, nameMatches)},
	}
}

func assertClusterTransitionFailure(
	t *testing.T,
	transaction *fakeDatabaseTransaction,
	identity scaler.RunnerIdentity,
	want error,
) {
	t.Helper()
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).MarkRunnerBusy(
		context.Background(), "linux", "repo", identity, 7, time.Unix(1, 0).UTC(),
	)
	if !errors.Is(err, want) {
		t.Fatalf("transition error = %v, want %v", err, want)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func TestClusterBindRunnerIdentityBindsOnlyIdentityFreeNamedRunner(t *testing.T) {
	transaction := &fakeDatabaseTransaction{
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("UPDATE 0"), pgconn.NewCommandTag("UPDATE 1"), pgconn.NewCommandTag("UPDATE 1"),
		},
		rowResults: []fakeRow{integerCountRow(t, 0), integerCountRow(t, 1)},
	}
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).BindRunnerIdentity(
		context.Background(), "linux", "repo", identity, 7, time.Unix(1, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transaction.executions) != 3 {
		t.Fatalf("binding executions = %#v", transaction.executions)
	}
	bindStatement := transaction.executions[1].statement
	if !strings.Contains(bindStatement, "runner_name =") || !strings.Contains(bindStatement, "request_id = 0") ||
		strings.Contains(bindStatement, " OR ") {
		t.Fatalf("identity binding statement = %q", bindStatement)
	}
}

func TestClusterMessageStartIgnoresNotFoundButReturnsMismatch(t *testing.T) {
	update := validClusterMessageUpdate()
	missing := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
	if err := applyClusterMessageStart(context.Background(), missing.databaseTransaction(), update,
		scaler.RunnerMessageIdentity{RunnerIdentity: scaler.RunnerIdentity{RunnerName: "missing"}}); err != nil {
		t.Fatalf("missing message start error = %v", err)
	}

	mismatch := &fakeDatabaseTransaction{
		execTags:   []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")},
		rowResults: []fakeRow{integerCountRow(t, 1), integerCountRow(t, 1)},
	}
	err := applyClusterMessageStart(context.Background(), mismatch.databaseTransaction(), update,
		scaler.RunnerMessageIdentity{RunnerIdentity: scaler.RunnerIdentity{RequestID: 42, RunnerName: "other"}})
	if !errors.Is(err, scaler.ErrRunnerIdentityMismatch) {
		t.Fatalf("mismatched message start error = %v", err)
	}
}

func TestClusterBindRunnerIdentityClassifiesMirroredLifecycle(t *testing.T) {
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}
	t.Run("missing mirror is repaired", func(t *testing.T) {
		assertClusterMirrorClassification(t, identity, 1, nil)
	})
	t.Run("stale mirror is reconciled", func(t *testing.T) {
		assertClusterMirrorClassification(t, identity, 1, nil)
	})
	t.Run("mirror name owns another request", func(t *testing.T) {
		assertClusterMirrorClassification(t, identity, 0, scaler.ErrRunnerLifecycleConsistency)
	})
	t.Run("mirror request belongs to another name", func(t *testing.T) {
		assertClusterMirrorClassification(t, identity, 0, scaler.ErrRunnerLifecycleConsistency)
	})
}

func assertClusterMirrorClassification(
	t *testing.T,
	identity scaler.RunnerIdentity,
	mirrorChanges int64,
	want error,
) {
	t.Helper()
	transaction := clusterMirrorClassificationTransaction(t, identity, mirrorChanges)
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).BindRunnerIdentity(
		context.Background(), "linux", "repo", identity, 7, time.Unix(1, 0).UTC(),
	)
	assertClusterMirrorClassificationResult(t, transaction, err, want)
}

func clusterMirrorClassificationTransaction(
	t *testing.T,
	identity scaler.RunnerIdentity,
	mirrorChanges int64,
) *fakeDatabaseTransaction {
	t.Helper()
	transaction := &fakeDatabaseTransaction{
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("UPDATE 1"), pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", mirrorChanges)),
		},
	}
	if mirrorChanges == 0 {
		transaction.rowResults = []fakeRow{
			{scan: func(destinations ...any) {
				*destination[NodeID](t, destinations[0]) = "node"
				*destination[scaler.RunnerProvider](t, destinations[1]) = scaler.RunnerProviderLibvirt
				*destination[string](t, destinations[2]) = identity.RunnerName
			}},
			integerCountRow(t, 1),
		}
	}
	return transaction
}

func assertClusterMirrorClassificationResult(
	t *testing.T,
	transaction *fakeDatabaseTransaction,
	err error,
	want error,
) {
	t.Helper()
	if want == nil && err != nil {
		t.Fatal(err)
	}
	if want != nil && !errors.Is(err, want) {
		t.Fatalf("binding error = %v, want %v", err, want)
	}
	if want != nil && transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func integerCountRow(t *testing.T, count int64) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		*destination[int64](t, destinations[0]) = count
	}}
}
