package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestRunnerIdentityMatchCoverage(t *testing.T) {
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	scope := clusterRunnerIdentityScope{table: "runners", condition: "true"}
	transaction := (&fakeDatabaseTransaction{rowResults: []fakeRow{
		integerCountRow(t, 0), {err: errors.New("runner lookup")},
	}}).databaseTransaction()
	if err := requireClusterRunnerIdentityMatch(context.Background(), transaction, scope, identity, 0); err == nil {
		t.Fatal("expected component lookup failure")
	}
	if err := requireAtMostOneClusterRunnerIdentityMatch(identity, 2); !errors.Is(err, scaler.ErrRunnerIdentityInvariant) {
		t.Fatalf("invariant error = %v", err)
	}
}

func TestRunnerIdentityTransactionCoverage(t *testing.T) {
	t.Run("begin failure", func(t *testing.T) {
		store := storeWithConnection(&fakeDatabaseConnection{beginErr: errors.New("begin")})
		if err := store.runClusterRunnerIdentityTransaction(context.Background(), "test", func(*databaseTransaction) error {
			return nil
		}); err == nil {
			t.Fatal("expected begin failure")
		}
	})
	t.Run("commit failure", func(t *testing.T) {
		store := storeWithConnection(&fakeDatabaseConnection{transaction: &fakeDatabaseTransaction{commitErr: errors.New("commit")}})
		if err := store.runClusterRunnerIdentityTransaction(context.Background(), "test", func(*databaseTransaction) error {
			return nil
		}); err == nil {
			t.Fatal("expected commit failure")
		}
	})
}

func TestRunnerIdentityTransitionErrorCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	predicate, err := newClusterRunnerIdentityPredicate(identity, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("busy invalid", func(t *testing.T) {
		err := markClusterRunnerBusyInTransaction(ctx, (&fakeDatabaseTransaction{}).databaseTransaction(),
			"linux", "repo", scaler.RunnerIdentity{}, 0, time.Time{})
		if !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("bind invalid", func(t *testing.T) {
		err := bindClusterRunnerIdentityInTransaction(ctx, (&fakeDatabaseTransaction{}).databaseTransaction(),
			"linux", "repo", scaler.RunnerIdentity{}, 0, time.Time{})
		if !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("exact duplicate", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 2")}}).databaseTransaction()
		_, err := markExactClusterRunnerBusy(ctx, transaction, "linux", "repo", identity, 0, time.Time{}, predicate)
		if !errors.Is(err, scaler.ErrRunnerIdentityInvariant) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestRunnerIdentityReapAndBindingErrorCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	predicate, err := newClusterRunnerIdentityPredicate(identity, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("reap duplicate", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 2")}}).databaseTransaction()
		err := requestClusterRunnerReapInTransaction(ctx, transaction, "linux", "repo", identity, "done", time.Time{}, predicate)
		if !errors.Is(err, scaler.ErrRunnerIdentityInvariant) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("binding inspection failure", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{rowResults: []fakeRow{{err: errors.New("lookup")}}}).databaseTransaction()
		err := bindClusterRunnerIdentity(ctx, transaction, clusterRunnerIdentityScope{table: "runners", condition: "true"},
			"linux", "repo", identity, 0, time.Time{})
		if err == nil {
			t.Fatal("expected lookup failure")
		}
	})
}

func TestRunnerIdentityBindingCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	scope := clusterRunnerIdentityScope{table: "runners", condition: "true"}
	t.Run("second inspection failure", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{rowResults: []fakeRow{
			integerCountRow(t, 0), {err: errors.New("runner lookup")},
		}}).databaseTransaction()
		if _, err := inspectClusterBindingCandidates(ctx, transaction, scope, identity); err == nil {
			t.Fatal("expected runner lookup failure")
		}
	})
	for name, matches := range map[string]clusterBindingCandidates{
		"duplicate": {request: 2},
		"not found": {},
	} {
		t.Run(name, func(t *testing.T) {
			if err := requireBindableClusterRunner(identity, matches); err == nil {
				t.Fatal("expected binding classification error")
			}
		})
	}
}

func TestRunnerIdentityBindingUpdateCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	t.Run("update failure", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{execErrors: []error{errors.New("update")}}).databaseTransaction()
		if err := updateBoundClusterRunner(ctx, transaction, "linux", "repo", identity, 0, time.Time{}); err == nil {
			t.Fatal("expected update failure")
		}
	})
	for name, tag := range map[string]string{"duplicate": "UPDATE 2", "lost candidate": "UPDATE 0"} {
		t.Run(name, func(t *testing.T) {
			transaction := (&fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag(tag)}}).databaseTransaction()
			if err := updateBoundClusterRunner(ctx, transaction, "linux", "repo", identity, 0, time.Time{}); err == nil {
				t.Fatal("expected update classification error")
			}
		})
	}
}

func TestClusterCompletionCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	completion := scaler.JobCompletion{ScaleSet: "linux", Repository: "repo", RunnerIdentity: identity}
	t.Run("invalid completion", func(t *testing.T) {
		if err := (&Store{}).CompleteRunnerLifecycleJob(ctx, scaler.JobCompletion{}, time.Time{}); !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("connector failure", func(t *testing.T) {
		store := &Store{connector: func(context.Context) (*databaseConnection, error) {
			return nil, errors.New("connect")
		}}
		if err := store.CompleteRunnerLifecycleJob(ctx, completion, time.Time{}); err == nil {
			t.Fatal("expected connector failure")
		}
	})
	t.Run("begin failure", func(t *testing.T) {
		store := storeWithConnection(&fakeDatabaseConnection{beginErr: errors.New("begin")})
		if err := store.CompleteRunnerLifecycleJob(ctx, completion, time.Time{}); err == nil {
			t.Fatal("expected begin failure")
		}
	})
	t.Run("commit failure", func(t *testing.T) {
		store := storeWithConnection(&fakeDatabaseConnection{transaction: &fakeDatabaseTransaction{commitErr: errors.New("commit")}})
		if err := store.CompleteRunnerLifecycleJob(ctx, completion, time.Time{}); err == nil {
			t.Fatal("expected commit failure")
		}
	})
}

func TestClusterCompletionTransactionCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	completion := scaler.JobCompletion{ScaleSet: "linux", Repository: "repo", RunnerIdentity: identity}
	t.Run("invalid transaction completion", func(t *testing.T) {
		if err := completeRunnerLifecycleJobInTransaction(ctx, (&fakeDatabaseTransaction{}).databaseTransaction(), scaler.JobCompletion{}); !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("assignment duplicate", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 2")}}).databaseTransaction()
		if err := completeClusterRequestDemand(ctx, transaction, completion); !errors.Is(err, scaler.ErrRunnerIdentityInvariant) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestClusterCompletionDeleteAndConfirmationCoverage(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	completion := scaler.JobCompletion{ScaleSet: "linux", Repository: "repo", RunnerIdentity: identity}
	t.Run("demand delete failure", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{execErrors: []error{nil, errors.New("delete")}}).databaseTransaction()
		if err := completeClusterRequestDemand(ctx, transaction, completion); err == nil {
			t.Fatal("expected demand delete failure")
		}
	})
	t.Run("assignment update failure", func(t *testing.T) {
		transaction := (&fakeDatabaseTransaction{execErrors: []error{errors.New("update")}}).databaseTransaction()
		if err := completeClusterRequestDemand(ctx, transaction, completion); err == nil {
			t.Fatal("expected assignment update failure")
		}
	})
	t.Run("invalid internal confirmation", func(t *testing.T) {
		if err := confirmClusterRunnerReapedInTransaction(ctx, (&fakeDatabaseTransaction{}).databaseTransaction(), scaler.RunnerLifecycle{}); !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("invalid generic confirmation", func(t *testing.T) {
		if err := (&Store{}).ConfirmRunnerReaped(ctx, scaler.RunnerLifecycle{}); !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
			t.Fatalf("error = %v", err)
		}
	})
}
