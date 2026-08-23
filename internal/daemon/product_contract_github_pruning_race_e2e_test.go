//go:build e2e

package daemon

import (
	"context"
	"net/http"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestProductContractGitHubStalePruneResultCannotDeleteNewerRegistrationSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseGitHubStalePruneResultRace(t, githubPruningSQLiteState(t))
}

func TestProductContractGitHubStalePruneResultCannotDeleteNewerRegistrationPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseGitHubStalePruneResultRace(t, githubPruningPostgresState(t))
}

func exerciseGitHubStalePruneResultRace(t *testing.T, stateConfig config.StateConfig) {
	t.Helper()
	ctx, race := startGitHubStalePruneResultRace(t, stateConfig)
	awaitGitHubPruningSignal(t, "stale GitHub delete request", race.deleteBarrier.started)
	newer := race.stale
	newer.InstallationID = 72
	newer.RunnerGroupID = 12
	if err := race.state.registrations.UpsertRegistration(ctx, newer); err != nil {
		t.Fatalf("commit newer GitHub registration during stale prune: %v", err)
	}
	race.deleteBarrier.Release()
	if err := awaitGitHubPruningResult(t, race.reconcileResult); err != nil {
		t.Fatalf("complete stale GitHub prune: %v", err)
	}
	assertNewerGitHubPruningRegistration(ctx, t, race.manager, race.host, newer)
}

type githubStalePruneResultRace struct {
	state           *githubPruningSafetyState
	host            *githubPruningHTTPBoundary
	manager         *githubsource.Manager
	deleteBarrier   *githubPruningDeleteBarrier
	reconcileResult <-chan error
	stale           githubsource.Registration
}

func startGitHubStalePruneResultRace(
	t *testing.T,
	stateConfig config.StateConfig,
) (context.Context, githubStalePruneResultRace) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state := openGitHubPruningSafetyStore(ctx, t, stateConfig)
	exact := upstream.RunnerScaleSet{ID: 29, Name: "linux", RunnerGroupID: 11}
	host := newGitHubPruningHTTPBoundary(githubPruningSafetyScenario{
		listed: []upstream.RunnerScaleSet{exact}, deleteStatus: http.StatusNoContent,
	})
	deleteBarrier := host.blockNextDelete(t)
	manager := newProductionTransportPruningManager(t, state.registrations, host)
	stale := githubPruningRegistration()
	seedGitHubPruningOwnership(ctx, t, state.registrations, stale)
	if err := state.registrations.DeleteActiveRegistration(ctx, stale); err != nil {
		t.Fatalf("make raced GitHub ownership stale: %v", err)
	}

	reconcileResult := make(chan error, 1)
	go func() {
		_, err := manager.Reconcile(ctx, githubsource.ManagerConfig{Prune: true})
		reconcileResult <- err
	}()
	return ctx, githubStalePruneResultRace{
		state: state, host: host, manager: manager, deleteBarrier: deleteBarrier,
		reconcileResult: reconcileResult, stale: stale,
	}
}

func awaitGitHubPruningSignal(t *testing.T, description string, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitGitHubPruningResult(t *testing.T, result <-chan error) error {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal("timed out waiting for stale GitHub prune result")
		return nil
	}
}

func assertNewerGitHubPruningRegistration(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	host *githubPruningHTTPBoundary,
	want githubsource.Registration,
) {
	t.Helper()
	snapshot, err := manager.RegistrationSnapshot(ctx)
	if err != nil {
		t.Fatalf("read registration after stale GitHub prune: %v", err)
	}
	if len(snapshot.Active) != 1 || snapshot.Active[0] != want ||
		len(snapshot.Owned) != 1 || snapshot.Owned[0] != want {
		t.Fatalf("registration after stale GitHub prune = %+v, want newer active and ownership %+v", snapshot, want)
	}
	deleteAttempts, unexpected := host.observations()
	if deleteAttempts != 1 || len(unexpected) != 0 {
		t.Fatalf("stale GitHub prune HTTP observations = deletes %d unexpected %v, want one exact delete",
			deleteAttempts, unexpected)
	}
}
