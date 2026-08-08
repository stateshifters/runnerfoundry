package sqlite

import (
	"context"
	"errors"
	"testing"

	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestReconcileAssignedDemandRejectsInvalidDemandBeforeBeginning(t *testing.T) {
	t.Parallel()
	beginCalls := 0
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) {
		beginCalls++
		return Transaction{}, nil
	}})
	if _, err := store.ReconcileAssignedDemand(context.Background(), githubsource.AssignedDemand{}); err == nil {
		t.Fatal("invalid demand was accepted")
	}
	if beginCalls != 0 {
		t.Fatalf("begin calls = %d", beginCalls)
	}
}

func TestQueueAggregateAssignedDemandRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	if _, err := queueAggregateAssignedDemand(context.Background(), Transaction{}, githubsource.AssignedDemand{}, 0); err == nil {
		t.Fatal("invalid aggregate identity was accepted")
	}
}

func TestReconcileAssignedDemandReportsLookupProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	demand := validAssignedDemand(0)
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).
		ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, protocolErr) {
		t.Fatalf("begin error = %v", err)
	}
	for _, failure := range namedFailingListTransactions(protocolErr) {
		t.Run(failure.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewStore(Executor{begin: transactionBeginning(failure.transaction)}).
				ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestReconcileAssignedDemandRollsBackQueueFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{
		query:   func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		execute: failingExecute(protocolErr), rollback: successfulCompletion,
	}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), validAssignedDemand(1)); !errors.Is(err, protocolErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestReconcileAssignedDemandRollsBackRemovalProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	demand := validAssignedDemand(0)
	source, err := githubsource.AggregateDemandSource(validAssignedDemand(1), 1)
	if err != nil {
		t.Fatalf("aggregate source: %v", err)
	}
	values := [][]any{{testJobIDText, string(source.Adapter), string(source.ID),
		"linux", fixedTime().UnixNano(), false}}
	for _, failure := range assignedDemandRemovalFailures(protocolErr) {
		t.Run(failure.name, func(t *testing.T) {
			t.Parallel()
			transaction := Transaction{query: func(context.Context, string, ...any) (Rows, error) {
				return rowsFromValues(values), nil
			}, execute: failure.execute, rollback: successfulCompletion}
			if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				ReconcileAssignedDemand(context.Background(), demand); err == nil {
				t.Fatal("removal failure was ignored")
			}
		})
	}
}

type assignedDemandRemovalFailure struct {
	name    string
	execute func(context.Context, string, ...any) (Result, error)
}

func assignedDemandRemovalFailures(protocolErr error) []assignedDemandRemovalFailure {
	return []assignedDemandRemovalFailure{
		{name: "delete", execute: failingExecute(protocolErr)},
		{name: "result", execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}},
		{name: "unexpected count", execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
		}},
	}
}

func validAssignedDemand(desiredJobs int) githubsource.AssignedDemand {
	return githubsource.AssignedDemand{Repository: "octo/repo", RunnerPool: "linux",
		ObservationID: "observation-1", DesiredJobs: desiredJobs, ObservedAt: fixedTime()}
}
