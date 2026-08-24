//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
	"github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

func TestProductContractRegistrationConvergenceSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	store, err := sqlite.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("open SQLite convergence state: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close SQLite convergence state: %v", closeErr)
		}
	})
	exerciseRegistrationConvergence(t, store)
}

func TestProductContractRegistrationConvergencePostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	configured := config.Config{State: config.StateConfig{Postgres: &settings}}
	dependencies := newProductionDependencies()
	dependencies.readHostname = func() (string, error) { return "e2e-convergence-member", nil }
	state, err := openProductionStateWithDependencies(context.Background(), configured, dependencies)
	if err != nil {
		t.Fatalf("open PostgreSQL convergence state: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
		defer cancel()
		if closeErr := closeRuntimeResources(closeContext, state.closers); closeErr != nil {
			t.Errorf("close PostgreSQL convergence state: %v", closeErr)
		}
	})
	store, ok := state.store.(registrationConvergenceStore)
	if !ok {
		t.Fatalf("PostgreSQL state %T does not expose declaration persistence", state.store)
	}
	exerciseRegistrationConvergence(t, store)
}

func TestProductContractPostgresRunnerGroupConflictIsolatedToRunnerPool(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	matching := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
		{RunnerPool: "windows", RunnerGroup: "default"},
	}}
	manager := establishRunnerGroupConflictFixture(t, now, matching)
	ctx := context.Background()
	conflicting := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "special"},
		{RunnerPool: "windows", RunnerGroup: "default"},
	}}
	if err := manager.PublishRunnerPoolDeclarations(ctx, "conflict-member-b", conflicting); err != nil {
		t.Fatalf("publish conflicting declarations: %v", err)
	}
	declarations := assertRunnerGroupConflict(ctx, t, manager, now)
	reconciliation := githubsource.ManagerConfig{
		Prune: true, RunnerPools: declarations.RunnerPools, PreserveRunnerPools: declarations.ConflictedRunnerPools,
	}
	if _, err := manager.Reconcile(ctx, reconciliation); err != nil {
		t.Fatalf("reconcile unaffected Runner Pool during conflict: %v", err)
	}
	assertRegistrationRunnerPools(ctx, t, manager, []core.RunnerPool{"linux", "windows"})
}

func establishRunnerGroupConflictFixture(
	t *testing.T,
	now time.Time,
	matching githubsource.ManagerConfig,
) *githubsource.Manager {
	t.Helper()
	store := openPostgresConvergenceStore(t, "e2e-runner-group-conflict")
	ctx := context.Background()
	for _, memberID := range []core.MemberID{"conflict-member-a", "conflict-member-b"} {
		if err := store.PublishMemberState(ctx, memberID, now, nil); err != nil {
			t.Fatalf("publish conflict member %q: %v", memberID, err)
		}
	}
	host := newE2EGitHost()
	manager, err := githubsource.NewManager(host, host.Client, e2eAppRegistrations(t, store), githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("build conflict manager: %v", err)
	}
	for _, memberID := range []core.MemberID{"conflict-member-a", "conflict-member-b"} {
		if err = manager.PublishRunnerPoolDeclarations(ctx, memberID, matching); err != nil {
			t.Fatalf("publish matching declarations for %q: %v", memberID, err)
		}
	}
	if _, err = manager.Reconcile(ctx, matching); err != nil {
		t.Fatalf("establish registrations before conflict: %v", err)
	}
	assertRegistrationRunnerPools(ctx, t, manager, []core.RunnerPool{"linux", "windows"})
	return manager
}

func assertRunnerGroupConflict(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	now time.Time,
) githubsource.RunnerPoolDeclarationSnapshot {
	t.Helper()
	declarations, err := manager.ClusterRunnerPools(ctx, now.Add(-runtimeMemberTTL))
	if err != nil || len(declarations.RunnerPools) != 1 || declarations.RunnerPools[0].RunnerPool != "windows" ||
		len(declarations.ConflictedRunnerPools) != 1 || declarations.ConflictedRunnerPools[0] != "linux" {
		t.Fatalf("conflicted declaration snapshot = %+v, error = %v", declarations, err)
	}
	return declarations
}

func openPostgresConvergenceStore(
	t *testing.T,
	memberName string,
) *postgresstate.Store {
	t.Helper()
	fixture := e2ePostgresStart(t)
	ctx := context.Background()
	settings := fixture.Settings()
	configured := config.Config{State: config.StateConfig{Postgres: &settings}}
	dependencies := newProductionDependencies()
	dependencies.readHostname = func() (string, error) { return memberName, nil }
	state, err := openProductionStateWithDependencies(ctx, configured, dependencies)
	if err != nil {
		t.Fatalf("open PostgreSQL convergence state: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
		defer cancel()
		if closeErr := closeRuntimeResources(closeContext, state.closers); closeErr != nil {
			t.Errorf("close PostgreSQL convergence state: %v", closeErr)
		}
	})
	store, valid := state.store.(*postgresstate.Store)
	if !valid {
		t.Fatalf("PostgreSQL state %T does not expose declaration persistence", state.store)
	}
	return store
}

type registrationConvergenceStore interface {
	unifiedStore
	githubsource.RunnerPoolDeclarationRepository
}

func exerciseRegistrationConvergence(t *testing.T, store registrationConvergenceStore) {
	t.Helper()
	ctx := context.Background()
	fixture := newRegistrationConvergenceFixture(ctx, t, store)
	fixture.establishInitialState(ctx, t)
	fixture.convergeRemovedRunnerPool(ctx, t)
}

type registrationConvergenceFixture struct {
	memberID core.MemberID
	now      time.Time
	host     *e2eGitHost
	manager  *githubsource.Manager
}

func newRegistrationConvergenceFixture(
	ctx context.Context,
	t *testing.T,
	store registrationConvergenceStore,
) registrationConvergenceFixture {
	t.Helper()
	fixture := registrationConvergenceFixture{
		memberID: "e2e-convergence-member",
		now:      time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC), host: newE2EGitHost(),
	}
	if err := store.PublishMemberState(ctx, fixture.memberID, fixture.now, nil); err != nil {
		t.Fatalf("publish convergence member: %v", err)
	}
	manager, err := githubsource.NewManager(fixture.host, fixture.host.Client, e2eAppRegistrations(t, store), githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("build convergence manager: %v", err)
	}
	fixture.manager = manager
	return fixture
}

func (fixture registrationConvergenceFixture) establishInitialState(ctx context.Context, t *testing.T) {
	t.Helper()
	initial := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
		{RunnerPool: "windows", RunnerGroup: "default"},
	}}
	if err := fixture.manager.PublishRunnerPoolDeclarations(ctx, fixture.memberID, initial); err != nil {
		t.Fatalf("publish initial declarations: %v", err)
	}
	assertConvergedRunnerPools(ctx, t, fixture.manager, fixture.now, []core.RunnerPool{"linux", "windows"})
	if _, err := fixture.manager.Reconcile(ctx, initial); err != nil {
		t.Fatalf("establish initial registrations: %v", err)
	}
	assertRegistrationRunnerPools(ctx, t, fixture.manager, []core.RunnerPool{"linux", "windows"})
}

func (fixture registrationConvergenceFixture) convergeRemovedRunnerPool(ctx context.Context, t *testing.T) {
	t.Helper()
	fixture.host.scriptPruneRemoval([]transport.OwnedRunnerPool{{ID: 30, Name: "windows", RunnerGroupID: 11}})
	reduced := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
	}}
	if err := fixture.manager.PublishRunnerPoolDeclarations(ctx, fixture.memberID, reduced); err != nil {
		t.Fatalf("replace declarations: %v", err)
	}
	assertConvergedRunnerPools(ctx, t, fixture.manager, fixture.now, []core.RunnerPool{"linux"})
	if _, err := fixture.manager.Reconcile(ctx, reduced); err != nil {
		t.Fatalf("prune removed registration: %v", err)
	}
	assertRegistrationRunnerPools(ctx, t, fixture.manager, []core.RunnerPool{"linux"})
	assertPruneObservedOwnership(t, fixture.host.recordedPruneCalls())
}

func assertConvergedRunnerPools(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	now time.Time,
	want []core.RunnerPool,
) {
	t.Helper()
	snapshot, err := manager.ClusterRunnerPools(ctx, now.Add(-runtimeMemberTTL))
	if err != nil || len(snapshot.ConflictedRunnerPools) != 0 || len(snapshot.RunnerPools) != len(want) {
		t.Fatalf("cluster declarations = %+v, error = %v", snapshot, err)
	}
	for index, runnerPool := range want {
		if snapshot.RunnerPools[index].RunnerPool != runnerPool {
			t.Fatalf("cluster declaration %d = %+v, want Runner Pool %q", index, snapshot.RunnerPools[index], runnerPool)
		}
	}
}

func assertRegistrationRunnerPools(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	want []core.RunnerPool,
) {
	t.Helper()
	snapshot, err := manager.RegistrationSnapshot(ctx)
	if err != nil || len(snapshot.Active) != len(want) || len(snapshot.Owned) != len(want) {
		t.Fatalf("registration snapshot = %+v, error = %v", snapshot, err)
	}
	for index, runnerPool := range want {
		if snapshot.Active[index].RunnerPool != runnerPool || snapshot.Owned[index].RunnerPool != runnerPool {
			t.Fatalf("registration %d = active %+v owned %+v, want Runner Pool %q",
				index, snapshot.Active[index], snapshot.Owned[index], runnerPool)
		}
	}
}

func assertPruneObservedOwnership(t *testing.T, calls []e2ePruneCall) {
	t.Helper()
	if len(calls) != 1 || calls[0].runnerGroup != 11 || len(calls[0].current) != 1 ||
		calls[0].current[0].Name != "linux" || len(calls[0].owned) != 2 {
		t.Fatalf("prune calls = %+v, want current Linux and both durable ownership records", calls)
	}
}

func TestProductContractDuplicateInventoryEndpointIsInspectedOnceSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	sharedEndpoint := execution.InventoryEndpointID("e2e:shared-provider")
	harness := newE2EHarnessWithProviderEndpoints(
		t,
		config.StateConfig{Path: ":memory:"},
		"e2e-deduplicated-inventory-member",
		false,
		sharedEndpoint,
		sharedEndpoint,
	)
	initialInspections := harness.provider.inspectCount()
	if initialInspections == 0 || harness.windowsProvider.inspectCount() != 0 {
		t.Fatalf("initial duplicate endpoint inspections = linux %d windows %d, want first provider only",
			initialInspections, harness.windowsProvider.inspectCount())
	}
	harness.completeMaintenanceCycle(harness.execution)
	if harness.provider.inspectCount() <= initialInspections || harness.windowsProvider.inspectCount() != 0 {
		t.Fatalf("repeated duplicate endpoint inspections = linux %d windows %d, want one physical inventory",
			harness.provider.inspectCount(), harness.windowsProvider.inspectCount())
	}
}
