//go:build e2e

package daemon

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

func TestProductContractGitHubAcquisitionRejectionSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseGitHubAcquisitionRejection(t, newSQLiteE2EHarness(t))
}

func TestProductContractGitHubAcquisitionRejectionPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseGitHubAcquisitionRejection(t, newPostgresE2EHarness(t))
}

func exerciseGitHubAcquisitionRejection(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	assertPublishedCapacity(t, harness.host.sessions["linux"], 1, "acquisition initial Linux capacity")
	assertPublishedCapacity(t, harness.host.sessions["windows"], 1, "acquisition initial Windows capacity")

	exercisePartialGitHubAcquisition(t, harness)
	exerciseEmptyGitHubAcquisition(t, harness)
	exerciseFailedGitHubAcquisitionRedelivery(t, harness)
	exerciseAcquiredSentinel(t, harness)

	if attempts := harness.provider.attemptCount(); attempts != 3 {
		t.Fatalf("provider starts after rejected acquisitions = %d, want only three acquired requests", attempts)
	}
}

func exerciseAcquiredSentinel(t *testing.T, harness *e2eHarness) {
	t.Helper()
	session := harness.host.sessions["linux"]
	session.scriptAcquisition([]int64{831}, nil)
	harness.deliver(githubAvailableRequests(harness.currentTime, 1, 831))
	assertAcquisitionRequest(t, session, 831)
	started := awaitProviderStart(t, "post-rejection sentinel provider start", harness.provider.starts)
	assertProcessingSource(t, harness, "linux", started.JobID, exactSourceID(t, harness, "linux", 831))
	completeAcquiredGitHubRequest(t, harness, 831, started)
}

func exercisePartialGitHubAcquisition(t *testing.T, harness *e2eHarness) {
	t.Helper()
	session := harness.host.sessions["linux"]
	session.scriptAcquisition([]int64{802}, nil)
	harness.deliver(githubAvailableRequests(harness.currentTime, 1, 801, 802, 803))
	assertAcquisitionRequest(t, session, 801, 802, 803)

	started := awaitProviderStart(t, "partially acquired provider start", harness.provider.starts)
	assertProcessingSource(t, harness, "linux", started.JobID, exactSourceID(t, harness, "linux", 802))
	if attempts := harness.provider.attemptCount(); attempts != 1 {
		t.Fatalf("partial acquisition provider starts = %d, want 1", attempts)
	}
	completeAcquiredGitHubRequest(t, harness, 802, started)
}

func exerciseEmptyGitHubAcquisition(t *testing.T, harness *e2eHarness) {
	t.Helper()
	session := harness.host.sessions["linux"]
	session.scriptAcquisition(nil, nil)
	harness.deliver(githubAvailableRequests(harness.currentTime, 0, 811, 812))
	assertAcquisitionRequest(t, session, 811, 812)
	assertNoAcquiredProcessing(t, harness, 1, "empty acquisition")
}

func exerciseFailedGitHubAcquisitionRedelivery(t *testing.T, harness *e2eHarness) {
	t.Helper()
	session := harness.host.sessions["linux"]
	drainIntegers(session.ackAttempts)
	wantErr := errors.New("scripted GitHub acquisition failure")
	session.scriptAcquisition(nil, wantErr)
	message := githubAvailableRequests(harness.currentTime, 1, 821, 822)
	messageID := session.assignMessageID(message)
	session.messages <- message
	assertAcquisitionRequest(t, session, 821, 822)
	if result := awaitRunnerPoolPollResult(t, harness); !errors.Is(result.err, wantErr) {
		t.Fatalf("failed acquisition poll error = %v, want %v", result.err, wantErr)
	}
	assertNoInteger(t, "failed acquisition acknowledgement", session.ackAttempts)
	assertNoAcquiredProcessing(t, harness, 1, "failed acquisition")

	harness.completeMaintenanceCycle(harness.source)
	session.scriptAcquisition([]int64{821}, nil)
	session.messages <- message
	harness.awaitMessageAcknowledgement(session, messageID)
	harness.completeMaintenanceCycle(harness.source)
	assertAcquisitionRequest(t, session, 821, 822)
	started := awaitProviderStart(t, "redelivered acquisition provider start", harness.provider.starts)
	assertProcessingSource(t, harness, "linux", started.JobID, exactSourceID(t, harness, "linux", 821))
	completeAcquiredGitHubRequest(t, harness, 821, started)
}

func githubAvailableRequests(
	observedAt time.Time,
	assigned int,
	requestIDs ...int64,
) *upstream.RunnerScaleSetMessage {
	available := make([]*upstream.JobAvailable, 0, len(requestIDs))
	for index, requestID := range requestIDs {
		available = append(available, &upstream.JobAvailable{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: observedAt.AddDate(-10, 0, 0).Add(time.Duration(index) * time.Second),
		}})
	}
	return &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(assigned), JobAvailableMessages: available,
	}
}

func assertAcquisitionRequest(t *testing.T, session *e2eGitHubSession, want ...int64) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case acquired := <-session.acquisitions:
		if !slices.Equal(acquired, want) {
			t.Fatalf("GitHub acquisition request IDs = %v, want %v", acquired, want)
		}
	case <-timer.C:
		t.Fatalf("timed out waiting for GitHub acquisition request %v", want)
	}
}

func assertNoAcquiredProcessing(t *testing.T, harness *e2eHarness, wantAttempts int, phase string) {
	t.Helper()
	processing, err := harness.runtime.state.store.ProcessingByPool(context.Background(), core.RunnerPool("linux"))
	if err != nil || len(processing) != 0 {
		t.Fatalf("%s processing = %+v, error = %v, want empty", phase, processing, err)
	}
	if attempts := harness.provider.attemptCount(); attempts != wantAttempts {
		t.Fatalf("%s provider starts = %d, want %d", phase, attempts, wantAttempts)
	}
}

func completeAcquiredGitHubRequest(
	t *testing.T,
	harness *e2eHarness,
	requestID int64,
	started execution.StartRequest,
) {
	t.Helper()
	drainIntegers(harness.host.sessions["linux"].capacities)
	markAndAssertRunnerAccepted(t, harness, requestID, started)
	drainIntegers(harness.host.sessions["linux"].capacities)
	completeAndAssertCleanup(t, harness, requestID, started)
}
