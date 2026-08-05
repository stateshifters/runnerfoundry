package scaler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/testsupport/runneridentitycontract"
)

type runnerIdentityMutationStore interface {
	RegisterRunner(ctx context.Context, runner RunnerLifecycle) error
	MarkRunnerBusy(
		ctx context.Context,
		scaleSet string,
		repository string,
		identity RunnerIdentity,
		githubRunnerID int64,
		startedAt time.Time,
	) error
	RunnerLifecycles(ctx context.Context, scaleSet string, repository string) ([]RunnerLifecycle, error)
}

type runnerLifecycleIdentityStore interface {
	RegisterRunner(ctx context.Context, runner RunnerLifecycle) error
	RequestRunnerReap(
		ctx context.Context,
		scaleSet string,
		repository string,
		identity RunnerIdentity,
		reason string,
		requestedAt time.Time,
	) error
	CompleteRunnerLifecycleJob(ctx context.Context, completion JobCompletion, completedAt time.Time) error
	ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) error
	RunnerLifecycles(ctx context.Context, scaleSet string, repository string) ([]RunnerLifecycle, error)
}

type runnerIdentityBindingStore interface {
	RegisterRunner(ctx context.Context, runner RunnerLifecycle) error
	BindRunnerIdentity(
		ctx context.Context,
		scaleSet string,
		repository string,
		identity RunnerIdentity,
		githubRunnerID int64,
		startedAt time.Time,
	) error
	RunnerLifecycles(ctx context.Context, scaleSet string, repository string) ([]RunnerLifecycle, error)
}

func TestRunnerIdentityContractAcrossMemoryAndSQLite(t *testing.T) {
	runRunnerIdentityContract(t, "memory", openMemoryIdentityContractStore)
	runRunnerIdentityContract(t, "SQLite", openSQLiteIdentityContractStore)
}

func TestRunnerIdentityCompletionAndReapConfirmationAreExact(t *testing.T) {
	for _, provider := range []RunnerProvider{RunnerProviderDocker, RunnerProviderLibvirt} {
		t.Run(string(provider)+"/memory", func(t *testing.T) {
			assertRunnerCompletionAndReapAreExact(t, NewMemoryDemandStore(), provider)
		})
		t.Run(string(provider)+"/SQLite", func(t *testing.T) {
			assertRunnerCompletionAndReapAreExact(t, openSQLiteCapacityStore(t), provider)
		})
	}
}

func TestRunnerIdentityBindingRejectsMirroredRequestOwner(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		assertRunnerBindingRejectsRequestOwner(t, NewMemoryDemandStore())
	})
	t.Run("SQLite", func(t *testing.T) {
		assertRunnerBindingRejectsRequestOwner(t, openSQLiteCapacityStore(t))
	})
}

func runRunnerIdentityContract[Store runnerIdentityMutationStore](
	t *testing.T,
	backend string,
	openStore func(*testing.T) Store,
) {
	t.Helper()
	t.Run(backend, func(t *testing.T) {
		for _, contractCase := range runneridentitycontract.Cases() {
			t.Run(contractCase.Name, func(t *testing.T) {
				runRunnerIdentityContractCase(t, openStore(t), contractCase)
			})
		}
	})
}

func openMemoryIdentityContractStore(t *testing.T) *MemoryDemandStore {
	t.Helper()
	return NewMemoryDemandStore()
}

func openSQLiteIdentityContractStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	return openSQLiteCapacityStore(t)
}

func runRunnerIdentityContractCase(
	t *testing.T,
	store runnerIdentityMutationStore,
	contractCase runneridentitycontract.Case,
) {
	t.Helper()
	seedRunnerIdentityContractCase(t, store, contractCase.Runners)
	identity := RunnerIdentity{
		RequestID:  contractCase.Identity.RequestID,
		RunnerName: contractCase.Identity.RunnerName,
	}
	err := store.MarkRunnerBusy(context.Background(), "set", "repo", identity, 99, time.Unix(2, 0).UTC())
	assertRunnerIdentityContractOutcome(t, err, contractCase.Outcome)
	assertBusyRunnerCount(t, store, expectedBusyRunnerCount(contractCase.Outcome))
}

func seedRunnerIdentityContractCase(
	t *testing.T,
	store runnerIdentityMutationStore,
	runners []runneridentitycontract.Identity,
) {
	t.Helper()
	for _, identity := range runners {
		runner := RunnerLifecycle{
			ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
			RequestID: identity.RequestID, RunnerName: identity.RunnerName,
			Phase: RunnerWaitingForJob, CreatedAt: time.Unix(1, 0).UTC(),
		}
		if sqliteStore, ok := store.(*SQLiteDemandStore); ok {
			seedSQLiteGenericLifecycle(t, sqliteStore.database, runner)
			continue
		}
		if err := store.RegisterRunner(context.Background(), runner); err != nil {
			t.Fatal(err)
		}
	}
}

func assertBusyRunnerCount(t *testing.T, store runnerIdentityMutationStore, expected int) {
	t.Helper()
	runners, err := store.RunnerLifecycles(context.Background(), "set", "repo")
	if err != nil {
		t.Fatal(err)
	}
	actual := 0
	for _, runner := range runners {
		if runner.Phase == RunnerBusy {
			actual++
		}
	}
	if actual != expected {
		t.Fatalf("busy runners = %d, want %d", actual, expected)
	}
}

func expectedBusyRunnerCount(outcome runneridentitycontract.Outcome) int {
	if outcome == runneridentitycontract.Success {
		return 1
	}
	return 0
}

func assertRunnerIdentityContractOutcome(t *testing.T, err error, outcome runneridentitycontract.Outcome) {
	t.Helper()
	wantedErrors := map[runneridentitycontract.Outcome]error{
		runneridentitycontract.Invalid:   ErrInvalidRunnerIdentity,
		runneridentitycontract.NotFound:  ErrRunnerIdentityNotFound,
		runneridentitycontract.Mismatch:  ErrRunnerIdentityMismatch,
		runneridentitycontract.Invariant: ErrRunnerIdentityInvariant,
	}
	wantedError := wantedErrors[outcome]
	if wantedError == nil && err != nil {
		t.Fatalf("mutation error = %v", err)
	}
	if wantedError != nil && !errors.Is(err, wantedError) {
		t.Fatalf("mutation error = %v, want %v", err, wantedError)
	}
}

func assertRunnerCompletionAndReapAreExact(
	t *testing.T,
	store runnerLifecycleIdentityStore,
	provider RunnerProvider,
) {
	t.Helper()
	seedGenericIdentityRunners(t, store, provider)
	completion := JobCompletion{
		ScaleSet: "set", Repository: "repo",
		RunnerIdentity: RunnerIdentity{RequestID: 1, RunnerName: "runner-two"},
	}
	err := store.CompleteRunnerLifecycleJob(context.Background(), completion, time.Unix(2, 0).UTC())
	assertIdentityError(t, err, ErrRunnerIdentityMismatch, "completion")
	requestFirstRunnerReap(t, store)
	confirmMismatchedRunnerReap(t, store)
	assertRunnerLifecycleCount(t, store, 2)
}

func seedGenericIdentityRunners(t *testing.T, store runnerLifecycleIdentityStore, provider RunnerProvider) {
	t.Helper()
	for _, identity := range []RunnerIdentity{
		{RequestID: 1, RunnerName: "runner-one"},
		{RequestID: 2, RunnerName: "runner-two"},
	} {
		runner := RunnerLifecycle{
			ScaleSet: "set", Repository: "repo", Provider: provider,
			RequestID: identity.RequestID, RunnerName: identity.RunnerName,
			Phase: RunnerWaitingForJob, CreatedAt: time.Unix(1, 0).UTC(),
		}
		if err := store.RegisterRunner(context.Background(), runner); err != nil {
			t.Fatal(err)
		}
	}
}

func requestFirstRunnerReap(t *testing.T, store runnerLifecycleIdentityStore) {
	t.Helper()
	err := store.RequestRunnerReap(
		context.Background(), "set", "repo", RunnerIdentity{RequestID: 1}, "test", time.Unix(2, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func confirmMismatchedRunnerReap(t *testing.T, store runnerLifecycleIdentityStore) {
	t.Helper()
	candidate := RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RequestID: 1, RunnerName: "runner-two", Phase: RunnerReapPending,
	}
	err := store.ConfirmRunnerReaped(context.Background(), candidate)
	assertIdentityError(t, err, ErrRunnerIdentityMismatch, "reap confirmation")
}

func assertRunnerLifecycleCount(t *testing.T, store runnerLifecycleIdentityStore, expected int) {
	t.Helper()
	runners, err := store.RunnerLifecycles(context.Background(), "set", "repo")
	if err != nil || len(runners) != expected {
		t.Fatalf("runners after rejected mutations = %#v, %v", runners, err)
	}
}

func assertRunnerBindingRejectsRequestOwner(t *testing.T, store runnerIdentityBindingStore) {
	t.Helper()
	seedLibvirtRunnerLifecycle(t, store, RunnerIdentity{RunnerName: "runner-one"})
	seedLibvirtRunnerLifecycle(t, store, RunnerIdentity{RequestID: 42, RunnerName: "runner-two"})
	conflict := RunnerIdentity{RequestID: 42, RunnerName: "runner-one"}
	err := store.BindRunnerIdentity(context.Background(), "set", "repo", conflict, 99, time.Unix(2, 0).UTC())
	assertIdentityError(t, err, ErrRunnerIdentityMismatch, "BindRunnerIdentity()")
	assertIdentityFreeRunnerRemainsWaiting(t, store)
}

func seedLibvirtRunnerLifecycle(t *testing.T, store runnerIdentityBindingStore, identity RunnerIdentity) {
	t.Helper()
	runner := RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderLibvirt,
		RequestID: identity.RequestID, RunnerName: identity.RunnerName,
		Phase: RunnerWaitingForJob, CreatedAt: time.Unix(1, 0).UTC(),
	}
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func assertIdentityFreeRunnerRemainsWaiting(t *testing.T, store runnerIdentityBindingStore) {
	t.Helper()
	runners, err := store.RunnerLifecycles(context.Background(), "set", "repo")
	if err != nil {
		t.Fatal(err)
	}
	for _, runner := range runners {
		if runner.RunnerName == "runner-one" &&
			(runner.RequestID != 0 || runner.Phase != RunnerWaitingForJob) {
			t.Fatalf("identity-free runner mutated after mirror conflict: %#v", runner)
		}
	}
}

func assertIdentityError(t *testing.T, err error, expected error, operation string) {
	t.Helper()
	if !errors.Is(err, expected) {
		t.Fatalf("%s error = %v, want %v", operation, err, expected)
	}
}
