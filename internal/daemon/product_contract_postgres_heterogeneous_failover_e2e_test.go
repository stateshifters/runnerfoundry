//go:build e2e

package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubpostgres "github.com/stateshifters/runnerfoundry/internal/source/github/persistence/postgres"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestProductContractHeterogeneousPostgresFollowerRestoresCommittedSourceSessionsBeforeDiscovery(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	clustered := e2eStartHeterogeneousPostgresApplications(t)
	e2ePostgresStopApplication(t, clustered.leaderRun, "heterogeneous PostgreSQL leader")
	clustered.follower.tickers.cycle(t, "source")
	awaitSignal(t, "replacement-leader blocked discovery", clustered.discovery.started)
	e2eAssertRestoredSourceSessions(t, clustered.follower, "while discovery is blocked")
	e2eAssertCommittedRegistrationsUnchanged(t, clustered.settings, clustered.committed)

	clustered.discovery.unblock()
	awaitSignal(t, "replacement-leader failed discovery completion", clustered.discovery.finished)
	clustered.follower.tickers.cycle(t, "source")
	e2eAssertRestoredSourceSessions(t, clustered.follower, "after failed discovery")
	e2eAssertCommittedRegistrationsUnchanged(t, clustered.settings, clustered.committed)
	e2ePostgresStopApplication(t, clustered.followerRun, "heterogeneous PostgreSQL replacement leader")
}

type e2eHeterogeneousPostgresApplications struct {
	settings    postgresconfig.Settings
	committed   githubsource.RegistrationSnapshot
	leaderRun   e2eApplicationRun
	follower    *e2eApplication
	followerRun e2eApplicationRun
	discovery   *e2eDiscoveryGate
}

func e2eStartHeterogeneousPostgresApplications(t *testing.T) e2eHeterogeneousPostgresApplications {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}

	leader := newE2EApplicationWithState(t, state, "e2e-postgres-heterogeneous-leader")
	leader.enableSource(LeaveActiveRunners)
	leaderRun := e2ePostgresStartApplication(t, leader, "heterogeneous PostgreSQL leader")
	leader.establishSource(t)
	committed := e2ePostgresCommittedRegistrations(t, settings)
	e2eAssertCommittedRunnerPools(t, committed, []core.RunnerPool{"linux", "windows"})

	follower := newE2EApplicationWithState(t, state, "e2e-postgres-heterogeneous-follower")
	e2eConfigureOnlyRunnerPool(t, follower, "linux")
	follower.advanceClock(2 * time.Minute)
	follower.enableSource(LeaveActiveRunners)
	discoveryFailure := errors.New("scripted replacement-leader discovery failure")
	discovery := follower.host.blockNextDiscovery(discoveryFailure)
	t.Cleanup(discovery.unblock)
	followerRun := e2ePostgresStartApplication(t, follower, "heterogeneous PostgreSQL follower")
	follower.tickers.cycle(t, "membership")
	follower.tickers.cycle(t, "source")
	e2eAssertNoSourceSessions(t, follower)
	return e2eHeterogeneousPostgresApplications{
		settings: settings, committed: committed, leaderRun: leaderRun,
		follower: follower, followerRun: followerRun, discovery: discovery,
	}
}

func e2eAssertRestoredSourceSessions(t *testing.T, application *e2eApplication, phase string) {
	t.Helper()
	for _, runnerPool := range []string{"linux", "windows"} {
		awaitSignal(t, "replacement-leader restored session for "+runnerPool, application.host.sessionOK[runnerPool])
		if opens := application.host.sessionOpenCount(runnerPool); opens != 1 {
			t.Fatalf("replacement leader %s session opens %s = %d, want exactly 1", runnerPool, phase, opens)
		}
	}
}

func e2eAssertCommittedRegistrationsUnchanged(
	t *testing.T,
	settings postgresconfig.Settings,
	want githubsource.RegistrationSnapshot,
) {
	t.Helper()
	if after := e2ePostgresCommittedRegistrations(t, settings); !reflect.DeepEqual(after, want) {
		t.Fatalf("registrations after failover = %+v, want unchanged %+v", after, want)
	}
}

func e2eConfigureOnlyRunnerPool(t *testing.T, application *e2eApplication, retained core.RunnerPool) {
	t.Helper()
	application.mutex.Lock()
	defer application.mutex.Unlock()
	for _, runnerPool := range application.configured.RunnerPools {
		if core.RunnerPool(runnerPool.Name) == retained {
			application.configured.RunnerPools = []config.RunnerPoolConfig{runnerPool}
			return
		}
	}
	t.Fatalf("application configuration has no Runner Pool %q", retained)
}

func e2eAssertNoSourceSessions(t *testing.T, application *e2eApplication) {
	t.Helper()
	for _, runnerPool := range []string{"linux", "windows"} {
		if opens := application.host.sessionOpenCount(runnerPool); opens != 0 {
			t.Fatalf("follower %s session opens before leadership = %d, want 0", runnerPool, opens)
		}
	}
}

func e2ePostgresCommittedRegistrations(
	t *testing.T,
	settings postgresconfig.Settings,
) githubsource.RegistrationSnapshot {
	t.Helper()
	var snapshot githubsource.RegistrationSnapshot
	e2ePostgresWithStores(t, settings, func(_ *postgresstate.Store, store *githubpostgres.Store) {
		active, err := store.Registrations(context.Background())
		if err != nil {
			t.Fatalf("load active PostgreSQL registrations: %v", err)
		}
		owned, err := store.OwnedRegistrations(context.Background())
		if err != nil {
			t.Fatalf("load owned PostgreSQL registrations: %v", err)
		}
		snapshot = githubsource.RegistrationSnapshot{Active: active, Owned: owned}
	})
	return snapshot
}

func e2eAssertCommittedRunnerPools(
	t *testing.T,
	snapshot githubsource.RegistrationSnapshot,
	want []core.RunnerPool,
) {
	t.Helper()
	if len(snapshot.Active) != len(want) || len(snapshot.Owned) != len(want) {
		t.Fatalf("committed registrations = %+v, want %d active and owned records", snapshot, len(want))
	}
	for index, runnerPool := range want {
		if snapshot.Active[index].RunnerPool != runnerPool || snapshot.Owned[index].RunnerPool != runnerPool {
			t.Fatalf("committed registration %d = active %+v owned %+v, want Runner Pool %q",
				index, snapshot.Active[index], snapshot.Owned[index], runnerPool)
		}
	}
}
