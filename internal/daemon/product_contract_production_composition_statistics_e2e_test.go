//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProductContractProductionCompositionReconcilesInitialSessionDemandBeforePolling(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	host := newProductionCompositionGitHub()
	host.setInitialAssignedDemand("docker-pool", 1)
	t.Cleanup(func() { host.assertExpected(t) })
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	runtime, generation, tickers := openProductionComposition(
		t, loadProductionCompositionConfig(t), host, dockerCommands, libvirtCommands,
	)
	defer closeProductionComposition(context.Background(), t, runtime, generation)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("production-composition initial-statistics logs:\n%s", tickers.logs.String())
		}
	})

	exerciseProductionCompositionInitialDemand(t, runtime, tickers, host, dockerCommands)
}

func exerciseProductionCompositionInitialDemand(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
) {
	t.Helper()
	host.guardPoll("docker-pool", func() error {
		if !tickers.logs.assignedDemandRecovered("docker-pool") {
			return errors.New("production GitHub polled docker-pool before durable initial-demand reconciliation")
		}
		return nil
	})
	establishProductionCompositionSessions(t, tickers, host)
	tickers.complete(t, tickers.source)
	started := awaitDockerCompositionStart(t, dockerCommands.starts)
	processing := assertProductionCompositionProcessing(t, runtime, "docker-pool")
	if string(processing.RunnerName) != started.RunnerName {
		t.Fatalf("initial assigned demand processing runner = %q, provider runner = %q",
			processing.RunnerName, started.RunnerName)
	}

	tickers.complete(t, tickers.source)
	awaitProductionCompositionPoolPoll(t, host, "docker-pool")
}

func TestProductContractProductionCompositionMissingInitialStatisticsPreventsPolling(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	host := newProductionCompositionGitHub()
	host.omitSessionStatistics("docker-pool")
	host.omitSessionStatistics("libvirt-pool")
	t.Cleanup(func() { host.assertExpected(t) })
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	runtime, generation, tickers := openProductionComposition(
		t, loadProductionCompositionConfig(t), host, dockerCommands, libvirtCommands,
	)
	defer closeProductionComposition(context.Background(), t, runtime, generation)

	establishProductionCompositionSessions(t, tickers, host)
	for attempt := 0; attempt < 2; attempt++ {
		tickers.complete(t, tickers.source)
		tickers.awaitPublishedPolls(t, "docker-pool", "libvirt-pool")
		assertNoProductionCompositionPoolPoll(t, host, "docker-pool")
		assertNoProductionCompositionPoolPoll(t, host, "libvirt-pool")
	}
	assertNoDockerCompositionStart(t, dockerCommands)
}

func assertNoProductionCompositionPoolPoll(
	t *testing.T,
	host *productionCompositionGitHub,
	runnerPool string,
) {
	t.Helper()
	host.mutex.Lock()
	_, polled := host.capacities[runnerPool]
	host.mutex.Unlock()
	if polled {
		t.Fatalf("production GitHub polled Runner Pool %q despite unresolved initial session statistics", runnerPool)
	}
}

func assertNoDockerCompositionStart(t *testing.T, commands *productionCompositionDockerCommands) {
	t.Helper()
	select {
	case started := <-commands.starts:
		t.Fatalf("production Docker started from missing initial statistics: %+v", started)
	default:
	}
}

func awaitProductionCompositionPoolPoll(
	t *testing.T,
	host *productionCompositionGitHub,
	runnerPool string,
) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		host.mutex.Lock()
		_, exists := host.capacities[runnerPool]
		changed := host.capacityChanged
		host.mutex.Unlock()
		if exists {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for production GitHub Runner Pool %q message poll", runnerPool)
		}
	}
}
