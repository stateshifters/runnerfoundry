//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubtransport "github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

type migrationRepresentativeState struct {
	processingJobID core.JobID
	registration    githubsource.Registration
}

func populateCurrentPostgresState(
	ctx context.Context,
	t *testing.T,
	clusterStore *cluster.Store,
) migrationRepresentativeState {
	t.Helper()
	pool, err := clusterStore.OpenPool(ctx)
	if err != nil {
		t.Fatalf("open current PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	store := postgresstate.NewConnectionStore(postgresstate.NewPGXConnection(pool))
	heartbeat := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	if err = store.PublishMemberState(ctx, "upgrade-member", heartbeat,
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 3}}); err != nil {
		t.Fatalf("populate current PostgreSQL member state: %v", err)
	}
	processingJobID := populateMigrationDemand(ctx, t, store, heartbeat)
	registration := populateMigrationGitHubState(ctx, t, store, heartbeat)
	return migrationRepresentativeState{processingJobID: processingJobID, registration: registration}
}

func populateMigrationDemand(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	heartbeat time.Time,
) core.JobID {
	t.Helper()
	processingJobID := populateAcceptedMigrationDemand(ctx, t, store, heartbeat)
	populateTerminalMigrationDemand(ctx, t, store, heartbeat)
	return processingJobID
}

func populateAcceptedMigrationDemand(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	heartbeat time.Time,
) core.JobID {
	t.Helper()
	processing := populateMigrationQueuePair(ctx, t, store, heartbeat)
	claimAndAcceptMigrationDemand(ctx, t, store, heartbeat, processing.Job.ID)
	return processing.Job.ID
}

func populateMigrationQueuePair(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	heartbeat time.Time,
) core.EnqueueResult {
	t.Helper()
	processing := queueRepresentativeMigrationDemand(ctx, t, store, core.JobRequest{
		Source:     core.SourceReference{Adapter: "e2e", ID: "upgrade-processing"},
		RunnerPool: "linux", EnqueuedAt: heartbeat.Add(-2 * time.Hour),
	})
	queueRepresentativeMigrationDemand(ctx, t, store, core.JobRequest{
		Source:     core.SourceReference{Adapter: "e2e", ID: "upgrade-queued"},
		RunnerPool: "linux", EnqueuedAt: heartbeat.Add(-time.Hour),
	})
	return processing
}

func claimAndAcceptMigrationDemand(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	heartbeat time.Time,
	processingJobID core.JobID,
) {
	t.Helper()
	claimed, err := store.ClaimOldest(ctx, "linux", core.ProcessingClaim{
		MemberID: "upgrade-member", ClaimedAt: heartbeat.Add(-30 * time.Minute),
		InventoryEndpointID: "e2e:provider",
	})
	if err != nil {
		t.Fatalf("populate current PostgreSQL processing claim: %v", err)
	}
	if claimed.Outcome != core.ClaimClaimed || claimed.Processing.Job.ID != processingJobID {
		t.Fatalf("populate current PostgreSQL processing claim = %+v, error = %v", claimed, err)
	}
	started, err := store.MarkRunnerStarted(ctx, core.JobIdentity{JobID: processingJobID})
	if err != nil {
		t.Fatalf("populate current PostgreSQL accepted processing: %v", err)
	}
	if started.Outcome != core.ObservationApplied {
		t.Fatalf("populate current PostgreSQL accepted processing = %+v, error = %v", started, err)
	}
}

func populateTerminalMigrationDemand(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	heartbeat time.Time,
) {
	t.Helper()
	terminalSource := core.SourceReference{Adapter: "e2e", ID: "upgrade-terminal"}
	queueRepresentativeMigrationDemand(ctx, t, store, core.JobRequest{
		Source: terminalSource, RunnerPool: "linux", EnqueuedAt: heartbeat,
	})
	terminalized, err := store.Terminalize(ctx, core.JobIdentity{Source: terminalSource})
	if err != nil {
		t.Fatalf("populate current PostgreSQL terminal observation: %v", err)
	}
	if terminalized.Outcome != core.ObservationApplied {
		t.Fatalf("populate current PostgreSQL terminal observation = %+v, error = %v", terminalized, err)
	}
}

func queueRepresentativeMigrationDemand(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	request core.JobRequest,
) core.EnqueueResult {
	t.Helper()
	result, err := store.Queue(ctx, request)
	if err != nil {
		t.Fatalf("populate current PostgreSQL %s demand: %v", request.Source.ID, err)
	}
	if result.Outcome != core.QueueQueued {
		t.Fatalf("populate current PostgreSQL %s demand = %+v", request.Source.ID, result)
	}
	return result
}

func populateMigrationGitHubState(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	heartbeat time.Time,
) githubsource.Registration {
	t.Helper()
	registration := githubsource.Registration{
		GitHubAppID: 7, RepositoryURL: "https://github.example/upgrade", RunnerPool: "linux",
		InstallationID: githubtransport.InstallationID(1), RunnerGroupID: githubtransport.RunnerGroupID(2),
		ScaleSetID: githubtransport.ScaleSetID(3),
	}
	if err := store.UpsertRegistration(ctx, registration); err != nil {
		t.Fatalf("populate current PostgreSQL registration: %v", err)
	}
	if err := store.ReplaceGitHubRunnerPoolDeclarations(ctx, "upgrade-member", []githubsource.RunnerPoolDeclaration{{
		RunnerPool: "linux", RunnerGroup: "upgrade-group",
	}}); err != nil {
		t.Fatalf("populate current PostgreSQL GitHub declaration: %v", err)
	}
	declarations, err := store.LiveGitHubRunnerPoolDeclarations(ctx, heartbeat.Add(-time.Minute))
	if err != nil {
		t.Fatalf("read current PostgreSQL GitHub declarations: %v", err)
	}
	wantDeclaration := githubsource.MemberRunnerPoolDeclaration{
		MemberID: "upgrade-member", RunnerPool: "linux", RunnerGroup: "upgrade-group",
	}
	if len(declarations) != 1 || declarations[0] != wantDeclaration {
		t.Fatalf("current PostgreSQL GitHub declarations = %+v, error = %v", declarations, err)
	}
	return registration
}
