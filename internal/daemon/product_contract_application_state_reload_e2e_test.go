//go:build e2e

package daemon

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestProductContractApplicationRejectsSQLiteToPostgresReload(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	postgresFixture := e2ePostgresStart(t)
	postgresSettings := postgresFixture.Settings()
	beforeTarget := e2ePostgresMigrationStatus(t, postgresSettings)
	if beforeTarget.Current != 0 {
		t.Fatalf("unused PostgreSQL reload target migration version = %d, want 0", beforeTarget.Current)
	}

	initialState := config.StateConfig{Path: t.TempDir() + "/state-reload-source.db"}
	application, running, openedStates := e2eStartStateReloadApplication(t, initialState, "sqlite-to-postgres")
	registrations := e2eApplicationRegistrationSnapshot(t, application)
	oldTickers := e2eApplicationGenerationTickers(t, application.tickers)
	e2eRequestStateBackendReload(t, application, config.StateConfig{Postgres: &postgresSettings})
	e2eAssertStateBackendReloadRejected(t, application, openedStates, oldTickers, initialState)

	started := e2eStartReloadJobOnOriginalGeneration(t, application, oldTickers[1], 1701)
	e2eAssertApplicationProcessing(t, application, started.JobID)
	e2eAssertApplicationRegistrationSnapshot(t, application, registrations)
	e2ePostgresStopApplication(t, running, "SQLite application after rejected PostgreSQL reload")
	if afterTarget := e2ePostgresMigrationStatus(t, postgresSettings); afterTarget != beforeTarget {
		t.Fatalf("PostgreSQL reload target migration status = %+v, want unchanged %+v", afterTarget, beforeTarget)
	}
}

func TestProductContractApplicationRejectsPostgresToSQLiteReload(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	postgresFixture := e2ePostgresStart(t)
	postgresSettings := postgresFixture.Settings()
	initialState := config.StateConfig{Postgres: &postgresSettings}
	application, running, openedStates := e2eStartStateReloadApplication(t, initialState, "postgres-to-sqlite")
	registrations := e2eApplicationRegistrationSnapshot(t, application)
	oldTickers := e2eApplicationGenerationTickers(t, application.tickers)
	targetPath := t.TempDir() + "/state-reload-target.db"
	e2eAssertPathAbsent(t, targetPath)

	e2eRequestStateBackendReload(t, application, config.StateConfig{Path: targetPath})
	e2eAssertStateBackendReloadRejected(t, application, openedStates, oldTickers, initialState)
	started := e2eStartReloadJobOnOriginalGeneration(t, application, oldTickers[1], 1702)
	e2ePostgresAssertProcessing(t, postgresSettings, started.JobID, 1)
	e2eAssertApplicationRegistrationSnapshot(t, application, registrations)
	e2eAssertPathAbsent(t, targetPath)
	e2ePostgresStopApplication(t, running, "PostgreSQL application after rejected SQLite reload")
	e2eAssertPathAbsent(t, targetPath)
}

func TestProductContractApplicationRejectsPostgresSettingsReload(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	postgresFixture := e2ePostgresStart(t)
	postgresSettings := postgresFixture.Settings()
	initialState := config.StateConfig{Postgres: &postgresSettings}
	application, running, openedStates := e2eStartStateReloadApplication(t, initialState, "postgres-settings")
	registrations := e2eApplicationRegistrationSnapshot(t, application)
	oldTickers := e2eApplicationGenerationTickers(t, application.tickers)

	replacementSettings := postgresSettings
	replacementSettings.Host = "unreachable.invalid"
	e2eRequestStateBackendReload(t, application, config.StateConfig{Postgres: &replacementSettings})
	e2eAssertStateBackendReloadRejected(t, application, openedStates, oldTickers, initialState)
	started := e2eStartReloadJobOnOriginalGeneration(t, application, oldTickers[1], 1703)
	e2ePostgresAssertProcessing(t, postgresSettings, started.JobID, 1)
	e2eAssertApplicationRegistrationSnapshot(t, application, registrations)
	e2ePostgresStopApplication(t, running, "PostgreSQL application after rejected settings reload")
}

func e2eStartStateReloadApplication(
	t *testing.T,
	state config.StateConfig,
	direction string,
) (*e2eApplication, e2eApplicationRun, <-chan config.StateConfig) {
	t.Helper()
	application := newE2EApplicationWithState(t, state, "e2e-state-reload-"+direction)
	application.enableSource(LeaveActiveRunners)
	openedStates := make(chan config.StateConfig, 2)
	running := e2eStartApplicationWithStateObserver(t, application, openedStates)
	application.awaitReady(t, direction+" application readiness")
	awaitConfiguredBuild(t, application.builtConfigs)
	application.tickers.awaitInitialGeneration(t)
	source := e2eAwaitGitHubSourceRuntime(t, direction+" source runtime", application.builtSources)
	e2eEstablishReloadSource(
		t, application, source, application.provider, application.windows, application.host,
	)
	opened := e2eAwaitOpenedState(t, openedStates)
	if !reflect.DeepEqual(opened, state) {
		t.Fatalf("initial state open = %+v, want %+v", opened, state)
	}
	return application, running, openedStates
}

func e2eStartApplicationWithStateObserver(
	t *testing.T,
	application *e2eApplication,
	openedStates chan<- config.StateConfig,
) e2eApplicationRun {
	t.Helper()
	dependencies := application.dependencies()
	openState := dependencies.production.openState
	dependencies.production.openState = func(
		ctx context.Context,
		configured config.Config,
		production productionDependencies,
	) (*productionState, error) {
		openedStates <- configured.State
		state, err := openState(ctx, configured, production)
		if err != nil {
			return nil, fmt.Errorf("open observed production state: %w", err)
		}
		return state, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	running := e2eApplicationRun{cancel: cancel, result: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		defer close(running.finished)
		running.result <- runDaemon(ctx, application.options(), dependencies)
	}()
	t.Cleanup(func() {
		cancel()
		awaitSignal(t, "state-reload application cleanup", running.finished)
	})
	return running
}

func e2eRequestStateBackendReload(
	t *testing.T,
	application *e2eApplication,
	replacement config.StateConfig,
) {
	t.Helper()
	application.mutex.Lock()
	application.configured.State = replacement
	application.mutex.Unlock()
	application.reloads <- struct{}{}
	application.awaitReloading(t, "state-backend reload notification")
	application.awaitReady(t, "readiness restoration after rejected state-backend reload")
}

func e2eAssertStateBackendReloadRejected(
	t *testing.T,
	application *e2eApplication,
	openedStates <-chan config.StateConfig,
	oldTickers []*e2eManualTicker,
	wantOpen config.StateConfig,
) {
	t.Helper()
	e2eAssertTickersRunning(t, oldTickers)
	e2eAssertPostgresReloadOldSourceRestored(t, application)
	select {
	case opened := <-openedStates:
		t.Fatalf("rejected reload opened replacement state %+v; initial state was %+v", opened, wantOpen)
	default:
	}
	select {
	case built := <-application.builtConfigs:
		t.Fatalf("rejected state-backend reload built replacement generation with state %+v", built.State)
	default:
	}
}

func e2eStartReloadJobOnOriginalGeneration(
	t *testing.T,
	application *e2eApplication,
	sourceTicker *e2eManualTicker,
	requestID int64,
) execution.StartRequest {
	t.Helper()
	application.deliverWithSourceTicker(
		t, "linux", e2eReloadAvailableMessage(requestID, application.now()), sourceTicker,
	)
	return awaitProviderStart(t, "provider start after rejected state-backend reload", application.provider.starts)
}

func e2eAwaitOpenedState(t *testing.T, openedStates <-chan config.StateConfig) config.StateConfig {
	t.Helper()
	select {
	case opened := <-openedStates:
		return opened
	case <-time.After(e2eWaitLimit):
		t.Fatal("timed out waiting for initial production state open")
		return config.StateConfig{}
	}
}

func e2eApplicationRegistrationSnapshot(
	t *testing.T,
	application *e2eApplication,
) githubsource.RegistrationSnapshot {
	t.Helper()
	active, err := application.runtime.state.registrationStore.Registrations(context.Background())
	if err != nil {
		t.Fatalf("load active application registrations: %v", err)
	}
	owned, err := application.runtime.state.registrationStore.OwnedRegistrations(context.Background())
	if err != nil {
		t.Fatalf("load owned application registrations: %v", err)
	}
	return githubsource.RegistrationSnapshot{Active: active, Owned: owned}
}

func e2eAssertApplicationRegistrationSnapshot(
	t *testing.T,
	application *e2eApplication,
	want githubsource.RegistrationSnapshot,
) {
	t.Helper()
	if got := e2eApplicationRegistrationSnapshot(t, application); !reflect.DeepEqual(got, want) {
		t.Fatalf("application registration ownership = %+v, want unchanged %+v", got, want)
	}
}

func e2eAssertApplicationProcessing(t *testing.T, application *e2eApplication, jobID core.JobID) {
	t.Helper()
	processing, err := application.runtime.state.store.processing.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].Job.ID != jobID {
		t.Fatalf("application processing after rejected state reload = %+v, error = %v", processing, err)
	}
}

func e2ePostgresMigrationStatus(t *testing.T, settings postgresconfig.Settings) cluster.MigrationStatus {
	t.Helper()
	services := newE2EPostgresMigrationServices(t, settings)
	status, err := services.migrator.Status(context.Background())
	if err != nil {
		t.Fatalf("inspect PostgreSQL reload target migration status: %v", err)
	}
	return status
}

func e2eAssertPathAbsent(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	if !os.IsNotExist(err) {
		t.Fatalf("replacement SQLite path %q exists or cannot be inspected: %v", path, err)
	}
}
