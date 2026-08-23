//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestProductContractPostgresApplicationLifecycle(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	application := newE2EApplicationWithState(
		t, config.StateConfig{Postgres: &settings}, "e2e-postgres-application",
	)
	application.provider.setInspectionError(errors.New("scripted initial PostgreSQL application inventory failure"))
	running := startE2EApplication(t, application)

	application.awaitReady(t, "PostgreSQL application readiness")
	awaitConfiguredBuild(t, application.builtConfigs)
	application.tickers.awaitInitialGeneration(t)
	application.awaitHealth(t, healthDegraded)
	e2ePostgresAssertMigrationsCurrent(t, settings)

	application.provider.setInspectionError(nil)
	application.tickers.cycle(t, "membership")
	application.tickers.cycle(t, "source")
	application.tickers.cycle(t, "execution")
	application.awaitHealth(t, healthOperational)
	application.applyValidReload(t)

	running.cancel()
	if err := awaitApplicationResult(t, running.result); err != nil {
		t.Fatalf("run PostgreSQL production daemon application: %v", err)
	}
	e2ePostgresAssertCapacity(t, settings, application.now(), 0, "standalone application shutdown")
}

func TestProductContractTwoMemberPostgresApplicationsFailOverWithoutLosingDemand(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	clustered := e2ePostgresStartApplicationCluster(t)
	e2ePostgresAssertApplicationFollower(t, clustered)

	owner, firstStart := e2ePostgresApplicationRunLifecycle(
		t, clustered.first, []*e2eApplication{clustered.first, clustered.second}, clustered.settings, 1001,
	)
	e2ePostgresAssertSingleApplicationClaim(
		t, clustered.settings, []*e2eApplication{clustered.first, clustered.second}, firstStart.JobID,
	)
	e2ePostgresCompleteApplicationJob(t, clustered.first, owner, firstStart, 1001)
	e2ePostgresTransferApplicationLeadership(t, clustered)

	attemptsBeforeFailoverLifecycle := clustered.second.provider.attemptCount()
	_, secondStart := e2ePostgresApplicationRunLifecycle(
		t, clustered.second, []*e2eApplication{clustered.second}, clustered.settings, 1002,
	)
	if attempts := clustered.second.provider.attemptCount() - attemptsBeforeFailoverLifecycle; attempts != 1 {
		t.Fatalf("replacement leader provider attempts = %d, want exactly 1", attempts)
	}
	e2ePostgresCompleteApplicationJob(t, clustered.second, clustered.second, secondStart, 1002)
	e2ePostgresStopApplication(t, clustered.secondRun, "replacement PostgreSQL application leader")
	e2ePostgresAssertCapacity(t, clustered.settings, clustered.second.now(), 0, "cluster application shutdown")
}

type e2ePostgresApplicationCluster struct {
	settings  postgresconfig.Settings
	first     *e2eApplication
	second    *e2eApplication
	firstRun  e2eApplicationRun
	secondRun e2eApplicationRun
}

func e2ePostgresStartApplicationCluster(t *testing.T) e2ePostgresApplicationCluster {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}
	first := newE2EApplicationWithState(t, state, "e2e-postgres-application-a")
	first.enableSource(LeaveActiveRunners)
	firstRun := e2ePostgresStartApplication(t, first, "first PostgreSQL cluster member")
	first.establishSource(t)

	second := newE2EApplicationWithState(t, state, "e2e-postgres-application-b")
	second.advanceClock(2 * time.Minute)
	second.enableSource(LeaveActiveRunners)
	secondRun := e2ePostgresStartApplication(t, second, "second PostgreSQL cluster member")
	second.tickers.cycle(t, "membership")
	second.tickers.cycle(t, "source")

	return e2ePostgresApplicationCluster{
		settings: settings, first: first, second: second, firstRun: firstRun, secondRun: secondRun,
	}
}

func e2ePostgresAssertApplicationFollower(t *testing.T, clustered e2ePostgresApplicationCluster) {
	t.Helper()
	e2ePostgresAssertCapacity(t, clustered.settings, clustered.first.now(), 2, "two live application members")
	for _, runnerPool := range []string{"linux", "windows"} {
		if opens := clustered.second.host.sessionOpenCount(runnerPool); opens != 0 {
			t.Fatalf("follower application %s GitHub sessions = %d, want 0", runnerPool, opens)
		}
	}
}

func e2ePostgresTransferApplicationLeadership(t *testing.T, clustered e2ePostgresApplicationCluster) {
	t.Helper()
	e2ePostgresStopApplication(t, clustered.firstRun, "PostgreSQL application leader")
	e2ePostgresAssertCapacity(t, clustered.settings, clustered.second.now(), 1, "leader application withdrawal")
	clustered.second.establishSource(t)
	for _, runnerPool := range []string{"linux", "windows"} {
		if opens := clustered.second.host.sessionOpenCount(runnerPool); opens != 1 {
			t.Fatalf("replacement leader %s GitHub sessions = %d, want 1", runnerPool, opens)
		}
	}
}

func e2ePostgresStopApplication(t *testing.T, running e2eApplicationRun, description string) {
	t.Helper()
	running.cancel()
	if err := awaitApplicationResult(t, running.result); err != nil {
		t.Fatalf("shut down %s: %v", description, err)
	}
}

func e2ePostgresStartApplication(
	t *testing.T,
	application *e2eApplication,
	description string,
) e2eApplicationRun {
	t.Helper()
	running := startE2EApplication(t, application)
	application.awaitReady(t, description+" readiness")
	awaitConfiguredBuild(t, application.builtConfigs)
	application.tickers.awaitInitialGeneration(t)

	return running
}

func e2ePostgresApplicationRunLifecycle(
	t *testing.T,
	leader *e2eApplication,
	members []*e2eApplication,
	settings postgresconfig.Settings,
	requestID int64,
) (*e2eApplication, execution.StartRequest) {
	t.Helper()
	leader.deliver(t, "linux", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: leader.now().Add(-time.Hour),
		}}},
	})
	owner, started := e2ePostgresAwaitApplicationStart(t, members)
	e2ePostgresAssertProcessing(t, settings, started.JobID, 1)

	return owner, started
}

func e2ePostgresCompleteApplicationJob(
	t *testing.T,
	leader *e2eApplication,
	owner *e2eApplication,
	started execution.StartRequest,
	requestID int64,
) {
	t.Helper()
	leader.deliver(t, "linux", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	leader.deliver(t, "linux", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(started.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	drainSignals(owner.provider.inspected)
	owner.tickers.cycle(t, "execution")
	awaitRunnerInstance(t, "application-level PostgreSQL terminal cleanup", owner.provider.destroys)
	owner.tickers.cycle(t, "execution")
	e2ePostgresAssertProcessing(t, *leader.configured.State.Postgres, started.JobID, 0)
}

func e2ePostgresAwaitApplicationStart(
	t *testing.T,
	members []*e2eApplication,
) (*e2eApplication, execution.StartRequest) {
	t.Helper()
	if len(members) == 1 {
		return members[0], awaitProviderStart(t, "PostgreSQL application provider start", members[0].provider.starts)
	}
	if len(members) != 2 {
		t.Fatalf("PostgreSQL application tracer members = %d, want one or two", len(members))
	}
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case started := <-members[0].provider.starts:
		return members[0], started
	case started := <-members[1].provider.starts:
		return members[1], started
	case <-timer.C:
		t.Fatal("timed out waiting for PostgreSQL application provider start")
		return nil, execution.StartRequest{}
	}
}

func e2ePostgresAssertSingleApplicationClaim(
	t *testing.T,
	settings postgresconfig.Settings,
	members []*e2eApplication,
	jobID core.JobID,
) {
	t.Helper()
	e2ePostgresAssertProcessing(t, settings, jobID, 1)
	attempts := 0
	for _, member := range members {
		attempts += member.provider.attemptCount()
	}
	if attempts != 1 {
		t.Fatalf("cluster application provider attempts = %d, want exactly 1", attempts)
	}
}

func e2ePostgresAssertProcessing(
	t *testing.T,
	settings postgresconfig.Settings,
	jobID core.JobID,
	wantCount int,
) {
	t.Helper()
	e2ePostgresWithStateStore(t, settings, func(store *postgresstate.Store) {
		processing, err := store.ProcessingByPool(context.Background(), "linux")
		if err != nil || len(processing) != wantCount {
			t.Fatalf("PostgreSQL application processing = %+v, error = %v, want count %d", processing, err, wantCount)
		}
		if wantCount == 1 && processing[0].Job.ID != jobID {
			t.Fatalf("PostgreSQL application processing JobID = %s, want %s", processing[0].Job.ID, jobID)
		}
	})
}

func e2ePostgresAssertMigrationsCurrent(t *testing.T, settings postgresconfig.Settings) {
	t.Helper()
	status, err := cluster.NewStore(settings).MigrationStatus(context.Background())
	if err != nil || status.Current == 0 || status.Current != status.Target {
		t.Fatalf("PostgreSQL application migration status = %+v, error = %v", status, err)
	}
}

func e2ePostgresAssertCapacity(
	t *testing.T,
	settings postgresconfig.Settings,
	now time.Time,
	want int,
	description string,
) {
	t.Helper()
	e2ePostgresWithStateStore(t, settings, func(store *postgresstate.Store) {
		capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", now.Add(-runtimeMemberTTL))
		if err != nil || capacity != want {
			t.Fatalf("%s capacity = %d, error = %v, want %d", description, capacity, err, want)
		}
	})
}

func e2ePostgresWithStateStore(
	t *testing.T,
	settings postgresconfig.Settings,
	inspect func(*postgresstate.Store),
) {
	t.Helper()
	pool, err := cluster.NewStore(settings).OpenPool(context.Background())
	if err != nil {
		t.Fatalf("open PostgreSQL application assertion pool: %v", err)
	}
	defer pool.Close()
	store := postgresstate.NewConnectionStore(postgresstate.NewPGXConnection(pool))
	inspect(store)
}
