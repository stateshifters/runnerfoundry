//go:build e2e

package daemon

import (
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

func TestProductContractPostgresClusterReloadRecoversThenTerminatesObservedRunner(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	clustered := e2eStartPostgresReloadCluster(t)

	e2eRequestRejectedApplicationReload(t, clustered.reloader, 2)
	clustered.reloader.tickers.cycleTicker(
		t, clustered.oldSourceTicker, "reactivated PostgreSQL source maintenance",
	)
	e2eAssertPostgresReloadOldSourceRestored(t, clustered.reloader)
	started := e2eStartAcceptedWindowsJobWithTicker(
		t, clustered.reloader, clustered.oldSourceTicker, 1601,
	)
	clustered.reloader.tickers.cycleTicker(
		t, clustered.oldExecutionTicker, "PostgreSQL old-generation inventory evidence",
	)

	e2eRequestRunnerPoolRemovalReload(t, clustered.reloader, "windows")
	clustered.reloader.tickers.awaitInitialGeneration(t)
	e2eAssertTickersStopped(t, clustered.oldTickers)
	e2eAssertReloadFinalizerSelection(t, TerminateActiveRunners, clustered.finalizerRequests)
	e2eAssertReloadPolicyOutcome(t, clustered.reloader, TerminateActiveRunners, started, 1)
	e2eAssertPostgresReloadPeerDidNotClaim(t, clustered.peer)

	e2ePostgresStopApplication(t, clustered.reloaderRun, "PostgreSQL reloaded cluster member")
	e2ePostgresStopApplication(t, clustered.peerRun, "PostgreSQL reload peer")
}

type e2ePostgresReloadCluster struct {
	reloader           *e2eApplication
	peer               *e2eApplication
	reloaderRun        e2eApplicationRun
	peerRun            e2eApplicationRun
	oldTickers         []*e2eManualTicker
	oldSourceTicker    *e2eManualTicker
	oldExecutionTicker *e2eManualTicker
	finalizerRequests  chan []core.RunnerPool
}

func e2eStartPostgresReloadCluster(t *testing.T) e2ePostgresReloadCluster {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}
	finalizerRequests := make(chan []core.RunnerPool, 1)

	reloader := e2eNewPostgresReloadApplication(t, state, finalizerRequests)
	reloaderRun := e2eStartReloadApplication(t, reloader)
	e2eAssertReloadPolicyConfiguration(t, reloader, TerminateActiveRunners)
	oldTickers := e2eApplicationGenerationTickers(t, reloader.tickers)

	peer := newE2EApplicationWithState(t, state, "e2e-postgres-reload-peer")
	e2eConfigureOnlyRunnerPool(t, peer, "linux")
	peer.advanceClock(2 * time.Minute)
	peer.enableSource(LeaveActiveRunners)
	peerRun := e2ePostgresStartApplication(t, peer, "PostgreSQL reload peer")
	peer.tickers.cycle(t, "membership")
	peer.tickers.cycle(t, "source")
	e2eAssertNoSourceSessions(t, peer)
	e2ePostgresAssertCapacity(t, settings, reloader.now(), 2, "PostgreSQL reload cluster")

	return e2ePostgresReloadCluster{
		reloader: reloader, peer: peer,
		reloaderRun: reloaderRun, peerRun: peerRun, oldTickers: oldTickers,
		oldSourceTicker: oldTickers[1], oldExecutionTicker: oldTickers[2],
		finalizerRequests: finalizerRequests,
	}
}

func e2eNewPostgresReloadApplication(
	t *testing.T,
	state config.StateConfig,
	finalizerRequests chan<- []core.RunnerPool,
) *e2eApplication {
	t.Helper()
	application := newE2EApplicationWithState(t, state, "e2e-postgres-reload-member")
	application.enableSource(TerminateActiveRunners)
	application.replacementHost = newE2EGitHost()
	application.replacementProvider = newE2EProviderForEndpoint("e2e:postgres-reload-linux")
	application.replacementWindows = newE2EProviderForEndpoint("e2e:postgres-reload-windows")
	application.generationDecorator = e2ePostgresReloadDecorator(finalizerRequests)
	return application
}

func e2ePostgresReloadDecorator(finalizerRequests chan<- []core.RunnerPool) func(int, generation) generation {
	return func(buildNumber int, built generation) generation {
		switch buildNumber {
		case 0:
			return &e2eReloadGeneration{generation: built, finalizerRequests: finalizerRequests}
		case 1:
			return &e2eReloadGeneration{
				generation: built, activateErr: errors.New("scripted PostgreSQL replacement activation failure"),
			}
		default:
			return built
		}
	}
}

func e2eAssertPostgresReloadOldSourceRestored(t *testing.T, application *e2eApplication) {
	t.Helper()
	for _, runnerPool := range []string{"linux", "windows"} {
		if opens := application.host.sessionOpenCount(runnerPool); opens != 1 {
			t.Fatalf("old PostgreSQL generation %s source session opens = %d, want 1", runnerPool, opens)
		}
	}
}

func e2eStartAcceptedWindowsJobWithTicker(
	t *testing.T,
	application *e2eApplication,
	sourceTicker *e2eManualTicker,
	requestID int64,
) execution.StartRequest {
	t.Helper()
	application.deliverWithSourceTicker(
		t, "windows", e2eReloadAvailableMessage(requestID, application.now()), sourceTicker,
	)
	started := awaitProviderStart(t, "PostgreSQL reload Windows provider start", application.windows.starts)
	application.deliverWithSourceTicker(t, "windows", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	}, sourceTicker)
	return started
}

func e2eAssertPostgresReloadPeerDidNotClaim(t *testing.T, peer *e2eApplication) {
	t.Helper()
	if linuxAttempts := peer.provider.attemptCount(); linuxAttempts != 0 {
		t.Fatalf("PostgreSQL reload peer Linux provider attempts = %d, want 0", linuxAttempts)
	}
	if windowsAttempts := peer.windows.attemptCount(); windowsAttempts != 0 {
		t.Fatalf("PostgreSQL reload peer Windows provider attempts = %d, want 0", windowsAttempts)
	}
}
