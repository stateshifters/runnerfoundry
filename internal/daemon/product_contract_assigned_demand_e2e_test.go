//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestProductContractAuthoritativeAssignedDemandSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseAuthoritativeAssignedDemand(t, newSQLiteE2EHarness(t))
}

func TestProductContractAuthoritativeAssignedDemandPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseAuthoritativeAssignedDemand(t, newPostgresE2EHarness(t))
}

func TestProductContractMissingAssignedStatisticsRejectedSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseMissingAssignedStatisticsRejected(t, newSQLiteE2EHarness(t))
}

func TestProductContractMissingAssignedStatisticsRejectedPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseMissingAssignedStatisticsRejected(t, newPostgresE2EHarness(t))
}

func exerciseAuthoritativeAssignedDemand(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	linuxGate := harness.provider.blockNextStart(nil)
	statisticsOnly := deliverAssignedDemand(t, harness, "linux", nil, nil, 2)
	harness.await("statistics-only assigned demand", linuxGate.started)
	_ = awaitProviderStart(t, "statistics-only provider attempt", harness.provider.attempts)
	assertAssignedDemandSnapshot(t, harness, assignedDemand(harness, "linux", statisticsOnly), 2, 1, 1)

	windowsGate := harness.windowsProvider.blockNextStart(nil)
	exactAndStatistics := deliverAssignedDemand(t, harness, "windows", []int64{701}, nil, 2)
	harness.await("exact demand ordered before statistics", windowsGate.started)
	windowsStarted := awaitProviderStart(t, "exact Windows provider attempt", harness.windowsProvider.attempts)
	assertProcessingSource(t, harness, "windows", windowsStarted.JobID, exactSourceID(t, harness, "windows", 701))
	assertAssignedDemandSnapshot(t, harness, assignedDemand(harness, "windows", exactAndStatistics), 2, 1, 1)

	newerAggregate := deliverAssignedDemand(t, harness, "windows", nil, nil, 4)
	assertAssignedDemandSnapshot(t, harness, assignedDemand(harness, "windows", newerAggregate), 4, 3, 1)
	contracted := deliverAssignedDemand(t, harness, "windows", nil, nil, 2)
	assertAssignedDemandSnapshot(t, harness, assignedDemand(harness, "windows", contracted), 2, 1, 1)
	assertExactDemandStateForPool(t, harness, "windows", 701, core.QueueProcessing)
	assertAggregateState(t, harness, assignedDemand(harness, "windows", exactAndStatistics), 1, core.QueueAlreadyQueued)

	assigned := deliverAssignedDemand(t, harness, "linux", nil, []int64{702}, 3)
	assertAssignedDemandSnapshot(t, harness, assignedDemand(harness, "linux", assigned), 3, 2, 1)
	assertAggregateState(t, harness, assignedDemand(harness, "linux", assigned), 1, core.QueueAlreadyQueued)

	assertAcknowledgementRedelivery(t, harness)
	linuxGate.release <- struct{}{}
	windowsGate.release <- struct{}{}
}

func exerciseMissingAssignedStatisticsRejected(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	session := harness.host.sessions["linux"]
	message := &upstream.RunnerScaleSetMessage{
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 901, QueueTime: harness.currentTime.AddDate(-10, 0, 0),
		}}}}
	session.assignMessageID(message)
	session.messages <- message
	result := awaitRunnerPoolPollResult(t, harness)
	if result.err == nil || !strings.Contains(result.err.Error(), "missing statistics") {
		t.Fatalf("missing-statistics poll error = %v", result.err)
	}
	assertNoInteger(t, "missing-statistics acknowledgement", session.ackAttempts)
	assertAssignedDemandSnapshot(t, harness, githubsource.AssignedDemand{
		Repository: harness.host.repository.URL, RunnerPool: "linux", ObservationID: "missing-statistics-check",
		DesiredJobs: 0, ObservedAt: harness.now(),
	}, 0, 0, 0)
}

func deliverAssignedDemand(
	t *testing.T,
	harness *e2eHarness,
	runnerPool string,
	available []int64,
	assigned []int64,
	desired int,
) *upstream.RunnerScaleSetMessage {
	t.Helper()
	message := &upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(desired)}
	for _, requestID := range available {
		message.JobAvailableMessages = append(message.JobAvailableMessages, &upstream.JobAvailable{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID,
				QueueTime: harness.currentTime.AddDate(-10, 0, 0)},
		})
	}
	for _, requestID := range assigned {
		message.JobAssignedMessages = append(message.JobAssignedMessages, &upstream.JobAssigned{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		})
	}
	harness.deliverTo(runnerPool, message)
	return message
}

func assignedDemand(harness *e2eHarness, runnerPool string, message *upstream.RunnerScaleSetMessage) githubsource.AssignedDemand {
	return githubsource.AssignedDemand{
		Repository: harness.host.repository.URL, RunnerPool: core.RunnerPool(runnerPool),
		ObservationID: fmt.Sprintf("message:%d", message.MessageID),
		DesiredJobs:   message.Statistics.TotalAssignedJobs, ObservedAt: harness.now(),
	}
}

func assertAssignedDemandSnapshot(
	t *testing.T,
	harness *e2eHarness,
	demand githubsource.AssignedDemand,
	active int,
	queued int,
	processing int,
) {
	t.Helper()
	result, err := harness.runtime.state.demandStore.ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatalf("observe authoritative assigned demand: %v", err)
	}
	if result.ActiveJobs != active || result.QueuedJobs != queued || result.ProcessingJobs != processing ||
		result.JobsAdded != 0 || result.JobsRemoved != 0 {
		t.Fatalf("assigned demand snapshot = %+v, want active=%d queued=%d processing=%d without repair",
			result, active, queued, processing)
	}
}

func assertAggregateState(
	t *testing.T,
	harness *e2eHarness,
	demand githubsource.AssignedDemand,
	ordinal int,
	want core.QueueOutcome,
) {
	t.Helper()
	request, err := githubsource.AggregateDemandRequest(demand, ordinal)
	if err != nil {
		t.Fatalf("identify aggregate assigned demand: %v", err)
	}
	result, err := harness.runtime.state.store.Queue(context.Background(), request)
	if err != nil || result.Outcome != want {
		t.Fatalf("aggregate demand state = %+v, error = %v, want %s", result, err, want)
	}
}

func exactSourceID(t *testing.T, harness *e2eHarness, runnerPool string, requestID int64) core.SourceIdentifier {
	t.Helper()
	identity := githubsource.SourceIdentity{Repository: harness.host.repository.URL,
		RunnerPool: core.RunnerPool(runnerPool), RequestID: requestID}
	sourceID, err := githubsource.EncodeSourceID(identity)
	if err != nil {
		t.Fatalf("encode exact GitHub demand: %v", err)
	}
	return sourceID
}

func assertProcessingSource(
	t *testing.T,
	harness *e2eHarness,
	runnerPool string,
	jobID core.JobID,
	want core.SourceIdentifier,
) {
	t.Helper()
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, core.RunnerPool(runnerPool),
	)
	if err != nil {
		t.Fatalf("read processing source: %v", err)
	}
	for _, job := range processing {
		if job.Job.ID == jobID {
			if job.Job.Source.ID != want {
				t.Fatalf("processing source = %q, want %q", job.Job.Source.ID, want)
			}
			return
		}
	}
	t.Fatalf("processing job %q not found in %+v", jobID, processing)
}

func assertExactDemandStateForPool(
	t *testing.T,
	harness *e2eHarness,
	runnerPool string,
	requestID int64,
	want core.QueueOutcome,
) {
	t.Helper()
	identity := githubsource.SourceIdentity{Repository: harness.host.repository.URL,
		RunnerPool: core.RunnerPool(runnerPool), RequestID: requestID}
	scope, err := githubsource.DemandScope(identity)
	if err != nil {
		t.Fatalf("encode exact GitHub demand scope: %v", err)
	}
	result, err := harness.runtime.state.store.Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: githubsource.AdapterName,
			ID: exactSourceID(t, harness, runnerPool, requestID)},
		Scope: scope, RunnerPool: core.RunnerPool(runnerPool), EnqueuedAt: harness.currentTime,
	})
	if err != nil || result.Outcome != want {
		t.Fatalf("exact demand state = %+v, error = %v, want %s", result, err, want)
	}
}

func assertAcknowledgementRedelivery(t *testing.T, harness *e2eHarness) {
	t.Helper()
	session := harness.host.sessions["linux"]
	drainIntegers(session.ackAttempts)
	message := &upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(4)}
	messageID := session.assignMessageID(message)
	wantErr := errors.New("scripted GitHub acknowledgement failure")
	session.failNextAcknowledgement(wantErr)
	session.messages <- message
	if attempt := awaitInteger(t, "failed GitHub acknowledgement", session.ackAttempts); attempt != messageID {
		t.Fatalf("failed acknowledgement message = %d, want %d", attempt, messageID)
	}
	if result := awaitRunnerPoolPollResult(t, harness); !errors.Is(result.err, wantErr) {
		t.Fatalf("failed acknowledgement poll error = %v, want %v", result.err, wantErr)
	}
	harness.completeMaintenanceCycle(harness.source)
	session.messages <- message
	harness.awaitMessageAcknowledgement(session, messageID)
	harness.completeMaintenanceCycle(harness.source)
	assertAssignedDemandSnapshot(t, harness, assignedDemand(harness, "linux", message), 4, 3, 1)
}

func awaitRunnerPoolPollResult(t *testing.T, harness *e2eHarness) githubPollResult {
	t.Helper()
	attempt := runnerPoolPollAttempt(t, productionGitHubSource(t, harness), "linux")
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case result := <-attempt.result:
		attempt.result <- result
		return result
	case <-timer.C:
		t.Fatal("timed out waiting for linux GitHub poll result")
		return githubPollResult{}
	}
}

func productionGitHubSource(t *testing.T, harness *e2eHarness) *githubSourceRuntime {
	t.Helper()
	if harness.sourceRuntime == nil {
		t.Fatal("production GitHub source was not captured during generation construction")
	}
	return harness.sourceRuntime
}

func runnerPoolPollAttempt(
	t *testing.T,
	source *githubSourceRuntime,
	runnerPool core.RunnerPool,
) *githubPollAttempt {
	t.Helper()
	for adapter, attempt := range source.polls {
		if adapter.RunnerPool == runnerPool {
			return attempt
		}
	}
	t.Fatalf("active %s GitHub poll not found", runnerPool)
	return nil
}

func awaitInteger(t *testing.T, description string, values <-chan int) int {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		return 0
	}
}

func drainIntegers(values <-chan int) {
	for {
		select {
		case <-values:
		default:
			return
		}
	}
}

func assertNoInteger(t *testing.T, description string, values <-chan int) {
	t.Helper()
	select {
	case value := <-values:
		t.Fatalf("unexpected %s: %d", description, value)
	default:
	}
}
