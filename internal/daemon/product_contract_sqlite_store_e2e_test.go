//go:build e2e

package daemon

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

func TestProductContractSQLiteRejectsConcurrentOwners(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	fixture := newSQLiteOwnerFixture(t)
	assertSQLiteExternalReaderBlocked(t, fixture.reader)
	contender, err := sqlite.Open(context.Background(), fixture.databasePath)
	if err == nil {
		if closeErr := contender.Close(); closeErr != nil {
			t.Errorf("close unexpected second SQLite owner: %v", closeErr)
		}
		t.Fatal("second SQLite owner opened the active state database")
	}
	fixture.closeOwner(t)
	assertSQLiteExternalReaderAvailable(t, fixture.reader)
}

type sqliteOwnerFixture struct {
	databasePath string
	owner        *sqlite.Store
	reader       *sql.DB
	ownerOpen    bool
}

func newSQLiteOwnerFixture(t *testing.T) *sqliteOwnerFixture {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "runnerfoundry.db")
	owner, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("open owning SQLite store: %v", err)
	}
	fixture := &sqliteOwnerFixture{
		databasePath: databasePath, owner: owner,
		reader: openSQLiteExternalReader(t, databasePath), ownerOpen: true,
	}
	t.Cleanup(func() {
		fixture.closeOwner(t)
		if closeErr := fixture.reader.Close(); closeErr != nil {
			t.Errorf("close external SQLite reader: %v", closeErr)
		}
	})
	return fixture
}

func (fixture *sqliteOwnerFixture) closeOwner(t *testing.T) {
	t.Helper()
	if !fixture.ownerOpen {
		return
	}
	if err := fixture.owner.Close(); err != nil {
		t.Errorf("close owning SQLite store: %v", err)
	}
	fixture.ownerOpen = false
}

func TestProductContractSQLiteStateSurvivesRestart(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	databasePath := filepath.Join(t.TempDir(), "runnerfoundry.db")
	state := persistSQLiteDurabilityState(t, databasePath)
	reopened := reopenSQLiteDurabilityStore(t, databasePath)
	assertSQLiteDurabilityState(t, reopened, state)
	assertSQLiteRecoveryOrdering(t, reopened, state)
}

func openSQLiteExternalReader(t *testing.T, databasePath string) *sql.DB {
	t.Helper()
	connection, err := sql.Open(
		"sqlite",
		"file:"+filepath.ToSlash(databasePath)+"?mode=ro&_pragma=busy_timeout(0)",
	)
	if err != nil {
		t.Fatalf("open external read-only SQLite connection: %v", err)
	}
	connection.SetMaxOpenConns(1)
	return connection
}

func assertSQLiteExternalReaderBlocked(t *testing.T, connection *sql.DB) {
	t.Helper()
	var tableCount int
	if err := connection.QueryRowContext(
		context.Background(), "SELECT COUNT(*) FROM sqlite_master",
	).Scan(&tableCount); err == nil {
		t.Fatalf("external SQLite reader accessed the live database and found %d tables", tableCount)
	}
}

func assertSQLiteExternalReaderAvailable(t *testing.T, connection *sql.DB) {
	t.Helper()
	var tableCount int
	if err := connection.QueryRowContext(
		context.Background(), "SELECT COUNT(*) FROM sqlite_master",
	).Scan(&tableCount); err != nil {
		t.Fatalf("external SQLite reader after owner close: %v", err)
	}
	if tableCount == 0 {
		t.Fatal("external SQLite reader found no schema after owner close")
	}
}

type sqliteDurabilityState struct {
	memberID          core.MemberID
	runnerPool        core.RunnerPool
	publishedCapacity int
	publishedAt       time.Time
	processingRequest core.JobRequest
	queuedRequest     core.JobRequest
	claimed           core.ProcessingJob
	queued            core.Job
	registration      githubsource.Registration
}

func persistSQLiteDurabilityState(t *testing.T, databasePath string) sqliteDurabilityState {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.Open(ctx, databasePath)
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	state := newSQLiteDurabilityState()
	publishSQLiteDurabilityMember(t, store, state)
	state.claimed = persistSQLiteProcessing(t, store, state)
	state.queued = persistSQLiteQueuedDemand(t, store, state)
	persistSQLiteRegistration(t, store, state.registration)
	if err = store.Close(); err != nil {
		t.Fatalf("close SQLite store before restart: %v", err)
	}
	return state
}

func newSQLiteDurabilityState() sqliteDurabilityState {
	state := sqliteDurabilityState{
		memberID:          "durability-member",
		runnerPool:        "linux",
		publishedCapacity: 3,
		publishedAt:       time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC),
	}
	state.processingRequest = core.JobRequest{
		Source:     core.SourceReference{Adapter: "github", ID: "durable-processing"},
		RunnerPool: state.runnerPool,
		EnqueuedAt: state.publishedAt.Add(-2 * time.Minute),
	}
	state.queuedRequest = core.JobRequest{
		Source:     core.SourceReference{Adapter: "github", ID: "durable-queued"},
		RunnerPool: state.runnerPool,
		EnqueuedAt: state.publishedAt,
	}
	state.registration = githubsource.Registration{
		RepositoryURL: "https://github.com/example/repository", RunnerPool: state.runnerPool,
		InstallationID: 11, RunnerGroupID: 22, ScaleSetID: 33,
	}
	return state
}

func publishSQLiteDurabilityMember(t *testing.T, store *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	if err := store.PublishMemberState(context.Background(), state.memberID, state.publishedAt, []core.RunnerPoolCapacity{{
		RunnerPool: state.runnerPool,
		Capacity:   state.publishedCapacity,
	}}); err != nil {
		t.Fatalf("publish member state: %v", err)
	}
}

func persistSQLiteProcessing(t *testing.T, store *sqlite.Store, state sqliteDurabilityState) core.ProcessingJob {
	t.Helper()
	ctx := context.Background()
	queuedProcessing, err := store.Queue(ctx, state.processingRequest)
	if err != nil || queuedProcessing.Outcome != core.QueueQueued {
		t.Fatalf("queue processing candidate = %+v, error = %v", queuedProcessing, err)
	}
	claim := core.ProcessingClaim{
		MemberID:            state.memberID,
		ClaimedAt:           state.publishedAt.Add(-time.Minute),
		InventoryEndpointID: "e2e:provider",
	}
	claimed, err := store.ClaimOldest(ctx, state.runnerPool, claim)
	if err != nil || claimed.Outcome != core.ClaimClaimed {
		t.Fatalf("claim processing candidate = %+v, error = %v", claimed, err)
	}
	return claimed.Processing
}

func persistSQLiteQueuedDemand(t *testing.T, store *sqlite.Store, state sqliteDurabilityState) core.Job {
	t.Helper()
	queued, err := store.Queue(context.Background(), state.queuedRequest)
	if err != nil || queued.Outcome != core.QueueQueued {
		t.Fatalf("queue durable job = %+v, error = %v", queued, err)
	}
	return queued.Job
}

func persistSQLiteRegistration(t *testing.T, store *sqlite.Store, registration githubsource.Registration) {
	t.Helper()
	if err := store.UpsertRegistration(context.Background(), registration); err != nil {
		t.Fatalf("persist GitHub registration: %v", err)
	}
}

func reopenSQLiteDurabilityStore(t *testing.T, databasePath string) *sqlite.Store {
	t.Helper()
	reopened, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("reopen SQLite store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Errorf("close reopened SQLite store: %v", closeErr)
		}
	})
	return reopened
}

func assertSQLiteDurabilityState(t *testing.T, reopened *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	assertSQLiteDurableCapacity(t, reopened, state)
	assertSQLiteDurableDemand(t, reopened, state)
	assertSQLiteDurableProcessing(t, reopened, state)
	assertSQLiteDurableRegistrations(t, reopened, state)
}

func assertSQLiteDurableCapacity(t *testing.T, reopened *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	capacity, err := reopened.AvailableRunnerPoolCapacity(
		context.Background(),
		state.runnerPool,
		state.publishedAt.Add(-time.Nanosecond),
	)
	const persistedDemand = 2
	wantAvailableCapacity := state.publishedCapacity - persistedDemand
	if err != nil || capacity != wantAvailableCapacity {
		t.Fatalf("durable member capacity after restart = %d, error = %v, want %d",
			capacity, err, wantAvailableCapacity)
	}
}

func assertSQLiteDurableDemand(t *testing.T, reopened *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	queuedAgain, err := reopened.Queue(context.Background(), state.queuedRequest)
	if err != nil || queuedAgain.Outcome != core.QueueAlreadyQueued || queuedAgain.Job.ID != state.queued.ID {
		t.Fatalf("durable queued job after restart = %+v, error = %v", queuedAgain, err)
	}
	processingAgain, err := reopened.Queue(context.Background(), state.processingRequest)
	if err != nil || processingAgain.Outcome != core.QueueProcessing {
		t.Fatalf("durable processing classification after restart = %+v, error = %v", processingAgain, err)
	}
}

func assertSQLiteDurableProcessing(t *testing.T, reopened *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	processingJobs, err := reopened.ProcessingByMember(context.Background(), state.memberID, state.runnerPool)
	if err != nil || len(processingJobs) != 1 {
		t.Fatalf("durable processing claim after restart = %+v, error = %v", processingJobs, err)
	}
	assertDurableProcessingClaim(t, processingJobs[0], state.claimed)
}

func assertSQLiteDurableRegistrations(t *testing.T, reopened *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	registrations, err := reopened.Registrations(context.Background())
	if err != nil || len(registrations) != 1 || registrations[0] != state.registration {
		t.Fatalf("durable active registration after restart = %+v, error = %v", registrations, err)
	}
	ownedRegistrations, err := reopened.OwnedRegistrations(context.Background())
	if err != nil || len(ownedRegistrations) != 1 || ownedRegistrations[0] != state.registration {
		t.Fatalf("durable registration ownership after restart = %+v, error = %v", ownedRegistrations, err)
	}
}

func assertSQLiteRecoveryOrdering(t *testing.T, reopened *sqlite.Store, state sqliteDurabilityState) {
	t.Helper()
	ctx := context.Background()
	recoveryTime := state.publishedAt.Add(10 * time.Minute)
	provider := newE2EProvider()
	orchestrator := runtimeengine.NewOrchestrator(
		reopened,
		durabilityBootstrapGenerator{},
		provider,
		provider,
		"recovery-leader",
		func() time.Time { return recoveryTime },
	)
	recovery, err := orchestrator.RecoverForeignPool(ctx, state.runnerPool)
	if err != nil || recovery.RemovedProcessing != 1 || recovery.RemainingProcessing != 0 {
		t.Fatalf("recover durable processing after restart = %+v, error = %v", recovery, err)
	}
	claimRecoveredJob(t, reopened, state, provider, recoveryTime.Add(time.Minute), state.queued.ID, "first")
	claimRecoveredJob(t, reopened, state, provider, recoveryTime.Add(2*time.Minute), state.claimed.Job.ID, "second")
}

func claimRecoveredJob(
	t *testing.T,
	store *sqlite.Store,
	state sqliteDurabilityState,
	provider *e2eProvider,
	claimedAt time.Time,
	want core.JobID,
	position string,
) {
	t.Helper()
	claimed, err := store.ClaimOldest(context.Background(), state.runnerPool, core.ProcessingClaim{
		MemberID: state.memberID, ClaimedAt: claimedAt, InventoryEndpointID: provider.EndpointID(),
	})
	if err != nil || claimed.Outcome != core.ClaimClaimed || claimed.Processing.Job.ID != want {
		t.Fatalf("%s claim after restart recovery = %+v, error = %v", position, claimed, err)
	}
}

func assertDurableProcessingClaim(t *testing.T, actual, expected core.ProcessingJob) {
	t.Helper()
	if actual.Job.ID != expected.Job.ID || actual.Job.Source != expected.Job.Source ||
		actual.Job.RunnerPool != expected.Job.RunnerPool || actual.RunnerName != expected.RunnerName ||
		actual.MemberID != expected.MemberID || !actual.ClaimedAt.Equal(expected.ClaimedAt) ||
		actual.InventoryEndpointID != expected.InventoryEndpointID ||
		actual.RunnerStarted != expected.RunnerStarted || actual.SourceTerminal != expected.SourceTerminal {
		t.Fatalf("durable processing claim = %+v, want contract fields from %+v", actual, expected)
	}
}

type durabilityBootstrapGenerator struct{}

func (durabilityBootstrapGenerator) GenerateBootstrap(
	context.Context,
	core.Job,
	core.RunnerName,
) (execution.Bootstrap, error) {
	return execution.NewBootstrap(nil), nil
}

func (durabilityBootstrapGenerator) CleanupBootstrap(context.Context, core.Job, core.RunnerName) error {
	return nil
}
