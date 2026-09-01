//go:build e2e

package daemon

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
)

func TestProductionCompositionHTTPTransportFailsClosed(t *testing.T) {
	transport := productionCompositionHTTPTransport(t, newProductionCompositionGitHub())
	assertProductionCompositionTransportIsClosed(t, transport)
	request, err := http.NewRequestWithContext(
		context.Background(), http.MethodGet, "http://unregistered.invalid/", http.NoBody,
	)
	if err != nil {
		t.Fatalf("build fail-closed HTTP request: %v", err)
	}
	response, err := transport.RoundTrip(request)
	if response != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Fatalf("close unexpected fail-closed HTTP response: %v", closeErr)
		}
	}
	if err == nil {
		t.Fatal("unregistered production-composition network route unexpectedly succeeded")
	}
}

func assertProductionCompositionTransportIsClosed(t *testing.T, transport *http.Transport) {
	t.Helper()
	if transport.Proxy != nil || transport.DialContext == nil || transport.DialTLSContext == nil ||
		transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 {
		t.Fatal("production-composition HTTP transport retains an external network path")
	}
	if transport.Protocols == nil || transport.Protocols.HTTP1() || transport.Protocols.HTTP2() ||
		transport.Protocols.UnencryptedHTTP2() || transport.ForceAttemptHTTP2 {
		t.Fatal("production-composition HTTP transport retains a fallback protocol path")
	}
}

func TestProductContractProductionCompositionRetriesAfterPartialProvisioningFailure(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	configured := loadProductionCompositionConfig(t)
	host := newProductionCompositionGitHub()
	t.Cleanup(func() { host.assertExpected(t) })
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	releaseFailure := dockerCommands.blockNextStartWithPartial(errors.New("scripted Docker start failure"))
	runtime, generation, tickers := openProductionComposition(t, configured, host, dockerCommands, libvirtCommands)
	defer closeProductionComposition(context.Background(), t, runtime, generation)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("production-composition retry logs:\n%s", tickers.logs.String())
		}
	})
	exerciseProductionCompositionProvisioningRetry(t, runtime, tickers, host, dockerCommands, releaseFailure)
}

func exerciseProductionCompositionProvisioningRetry(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
	releaseFailure chan<- struct{},
) {
	t.Helper()
	establishProductionCompositionSessions(t, tickers, host)
	host.queue("29", 301, time.Date(2020, time.August, 16, 10, 0, 0, 0, time.UTC))
	tickers.awaitPublishedPolls(t, "docker-pool")
	tickers.complete(t, tickers.source)
	awaitProductionCompositionValues(t, "failed-demand acknowledgement", host.acknowledged, 1)
	tickers.complete(t, tickers.source)
	firstAttempt := awaitDockerCompositionStart(t, dockerCommands.starts)
	firstJobID := assertProductionCompositionProcessing(t, runtime, "docker-pool").Job.ID
	close(releaseFailure)
	removed := awaitProductionCompositionValue(t, "partial Docker cleanup", dockerCommands.removals)
	if removed != productionCompositionDockerID(firstAttempt.RunnerName) {
		t.Fatalf("partial Docker cleanup = %q, want runner %q", removed, firstAttempt.RunnerName)
	}
	tickers.complete(t, tickers.execution)
	retried := awaitDockerCompositionStart(t, dockerCommands.starts)
	processing := assertProductionCompositionProcessing(t, runtime, "docker-pool")
	if processing.Job.ID != firstJobID || retried.RunnerName == firstAttempt.RunnerName {
		t.Fatalf("retry identity = job %s runner %q, want job %s and a fresh attempt after %q",
			processing.Job.ID, retried.RunnerName, firstJobID, firstAttempt.RunnerName)
	}
}

func awaitDockerCompositionStart(
	t *testing.T,
	values <-chan dockerprovider.ComposeRunnerRequest,
) dockerprovider.ComposeRunnerRequest {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for production Docker start")
		return dockerprovider.ComposeRunnerRequest{}
	}
}
