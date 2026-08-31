//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestProductContractAcceptedJobLifecyclePostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseAcceptedJobLifecycle(t, newPostgresE2EHarness(t))
}

func TestProductContractProvisioningFailureRetriesSameJobFromQueueTailPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseProvisioningFailureRetry(t, newPostgresE2EHarness(t))
}

func TestProductContractCleanupFailureRetainsTerminalProcessingPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseCleanupFailureRecovery(t, newPostgresE2EHarness(t))
}

func TestProductContractPartialProvisioningCleanupGatesRetryPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exercisePartialProvisioningCleanupGatesRetry(t, newPostgresE2EHarness(t))
}

func TestProductContractQueuedTerminalDemandIsNeverProvisionedPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseQueuedTerminalDemand(t, newPostgresE2EHarness(t))
}

func TestProductContractGracefulShutdownJoinsSourceAndMaintenancePostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseGracefulShutdown(t, newPostgresE2EHarness(t))
}

func TestProductContractPostgresLeaderFailoverPreservesLifecycle(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	first, second := newLivePostgresCluster(t, "e2e-member-a", "e2e-member-b", false)
	assertPostgresFollowerState(t, first, second)
	exerciseClusterJob(t, first, []*e2eHarness{first, second}, 501)
	completePostgresLeaderFailover(t, first, second)
}

func TestProductContractPostgresFollowerProcessesWithoutSourceLeadership(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	leader, follower := newLivePostgresCluster(t, "e2e-local-leader", "e2e-local-follower", false)
	occupyPostgresMemberPool(t, leader, "linux")
	leader.completeMaintenanceCycle(leader.execution)
	leader.completeMaintenanceCycle(leader.membership)
	leader.markLinuxProcessorIneligible()

	leader.deliver(e2eAvailableMessage(leader, 503))
	follower.completeMaintenanceCycle(follower.execution)
	started := awaitProviderStart(t, "nonleader member-local provider start", follower.provider.starts)
	if attempts := leader.provider.attemptCount(); attempts != 0 {
		t.Fatalf("source leader provider attempts while locally ineligible = %d, want 0", attempts)
	}
	assertSingleClusterClaim(t, leader, []*e2eHarness{leader, follower}, started.JobID)
}

func TestProductContractPostgresLeaderFailoverPreservesOutstandingDemand(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	leader, follower := newLivePostgresCluster(t, "e2e-outstanding-leader", "e2e-outstanding-follower", false)
	accepted, queuedJobID := prepareOutstandingFailoverDemand(t, leader, follower)
	leader.close()
	follower.establishSourceSession()
	completeOutstandingFailover(t, follower, accepted, queuedJobID)
}

func completeOutstandingFailover(
	t *testing.T,
	follower *e2eHarness,
	accepted execution.StartRequest,
	queuedJobID core.JobID,
) {
	t.Helper()
	assertOutstandingAcceptedAfterFailover(t, follower, accepted.JobID)
	redeliverOutstandingFailoverDemand(t, follower, accepted, queuedJobID)
	terminalizeOutstandingFailoverDemand(t, follower, accepted)
	restoreOutstandingFollowerProcessing(t, follower, queuedJobID)
}

func redeliverOutstandingFailoverDemand(
	t *testing.T,
	follower *e2eHarness,
	accepted execution.StartRequest,
	queuedJobID core.JobID,
) {
	t.Helper()
	if got := assertExactDemandState(t, follower, 505, core.QueueAlreadyQueued); got != queuedJobID {
		t.Fatalf("queued job after failover = %s, want durable identity %s", got, queuedJobID)
	}
	follower.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(2),
		JobAvailableMessages: []*upstream.JobAvailable{
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 504, QueueTime: follower.currentTime.AddDate(-10, 0, 0)}},
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 505, QueueTime: follower.currentTime.AddDate(-10, 0, 0)}},
		},
	})
	assertOutstandingAcceptedAfterFailover(t, follower, accepted.JobID)
}

func terminalizeOutstandingFailoverDemand(t *testing.T, follower *e2eHarness, accepted execution.StartRequest) {
	t.Helper()
	follower.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(accepted.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 504}}},
	})
	assertOutstandingTerminalAfterFailover(t, follower, accepted.JobID)
	if attempts := follower.provider.attemptCount(); attempts != 0 {
		t.Fatalf("follower attempts before queued capacity becomes available = %d, want 0", attempts)
	}
}

func restoreOutstandingFollowerProcessing(
	t *testing.T,
	follower *e2eHarness,
	queuedJobID core.JobID,
) {
	t.Helper()
	follower.completeMaintenanceCycle(follower.execution)
	started := awaitProviderStart(t, "queued provider start after leader failover", follower.provider.starts)
	if started.JobID != queuedJobID {
		t.Fatalf("queued start after failover = %s, want durable identity %s", started.JobID, queuedJobID)
	}
}

func prepareOutstandingFailoverDemand(
	t *testing.T,
	leader *e2eHarness,
	follower *e2eHarness,
) (execution.StartRequest, core.JobID) {
	t.Helper()
	follower.completeMaintenanceCycle(follower.execution)
	follower.completeMaintenanceCycle(follower.membership)
	follower.markLinuxProcessorIneligible()
	leader.await("PostgreSQL queue listener before outstanding demand", leader.notificationReady)
	leader.deliver(e2eAvailableMessage(leader, 504))
	leader.completeMaintenanceCycle(leader.execution)
	accepted := awaitProviderStart(t, "accepted provider start before leader failover", leader.provider.starts)
	leader.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(accepted.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 504}}},
	})
	leader.completeMaintenanceCycle(leader.execution)
	leader.completeMaintenanceCycle(leader.membership)
	leader.markLinuxProcessorIneligible()
	follower.markLinuxProcessorIneligible()
	leader.deliver(e2eAvailableMessage(leader, 505))
	queuedJobID := assertExactDemandState(t, leader, 505, core.QueueAlreadyQueued)
	return accepted, queuedJobID
}

func occupyPostgresMemberPool(
	t *testing.T,
	harness *e2eHarness,
	runnerPool core.RunnerPool,
) execution.RunnerInstance {
	t.Helper()
	runnerName := core.RunnerName("occupied-" + harness.memberName + "-" + string(runnerPool))
	request := execution.StartRequest{
		RunnerName: runnerName, RunnerPool: runnerPool, EndpointID: harness.provider.EndpointID(),
	}
	harness.provider.recordRunningInstance(request)
	return execution.RunnerInstance{
		ID: string(runnerName), RunnerName: runnerName, RunnerPool: runnerPool,
		EndpointID: request.EndpointID, State: execution.RunnerStateRunning,
	}
}

func assertOutstandingAcceptedAfterFailover(
	t *testing.T,
	harness *e2eHarness,
	wantJobID core.JobID,
) {
	t.Helper()
	processing := outstandingProcessingAfterFailover(t, harness, wantJobID)
	if !processing.RunnerAccepted || processing.SourceTerminal {
		t.Fatalf("accepted processing after failover = %+v, want started and nonterminal", processing)
	}
}

func assertOutstandingTerminalAfterFailover(t *testing.T, harness *e2eHarness, wantJobID core.JobID) {
	t.Helper()
	processing := outstandingProcessingAfterFailover(t, harness, wantJobID)
	if !processing.RunnerAccepted || !processing.SourceTerminal {
		t.Fatalf("terminal processing after failover = %+v, want started and terminal", processing)
	}
}

func outstandingProcessingAfterFailover(
	t *testing.T,
	harness *e2eHarness,
	wantJobID core.JobID,
) core.ProcessingJob {
	t.Helper()
	processing, err := harness.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	if err != nil {
		t.Fatalf("read outstanding processing after failover: %v", err)
	}
	for _, candidate := range processing {
		if candidate.Job.ID == wantJobID {
			return candidate
		}
	}
	t.Fatalf("accepted processing %s missing after failover: %+v", wantJobID, processing)
	return core.ProcessingJob{}
}

func newLivePostgresCluster(
	t *testing.T,
	firstMember string,
	secondMember string,
	disableSecondNotifications bool,
) (first *e2eHarness, second *e2eHarness) {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}
	first = newE2EHarness(t, state, firstMember, false)
	first.establishSourceSession()
	assertPublishedCapacity(t, first.host.sessions["linux"], 1, "initial cluster Linux capacity")
	assertPublishedCapacity(t, first.host.sessions["windows"], 1, "initial cluster Windows capacity")
	second = newE2EHarness(t, state, secondMember, disableSecondNotifications)
	second.await("second member inventory", second.provider.refreshed)
	second.completeMaintenanceCycle(second.membership)
	second.completeMaintenanceCycle(second.source)
	rotateAndAssertCapacity(t, first, "linux", 0, 2, "aggregated live-member Linux capacity")
	rotateAndAssertCapacity(t, first, "windows", 0, 2, "aggregated live-member Windows capacity")
	return first, second
}

func assertPostgresFollowerState(t *testing.T, first *e2eHarness, second *e2eHarness) {
	t.Helper()
	capacity, err := first.runtime.state.store.AvailableRunnerPoolCapacity(
		context.Background(), "linux", first.currentTime.Add(-runtimeMemberTTL),
	)
	if err != nil || capacity != 2 {
		t.Fatalf("two-member available capacity = %d, error = %v", capacity, err)
	}
	for _, runnerPool := range []string{"linux", "windows"} {
		if opens := second.host.sessionOpenCount(runnerPool); opens != 0 {
			t.Fatalf("follower %s GitHub sessions before failover = %d, want 0", runnerPool, opens)
		}
	}
}

func completePostgresLeaderFailover(t *testing.T, first *e2eHarness, second *e2eHarness) {
	t.Helper()
	first.close()
	capacity, err := second.runtime.state.store.AvailableRunnerPoolCapacity(
		context.Background(), "linux", second.currentTime.Add(-runtimeMemberTTL),
	)
	if err != nil || capacity != 1 {
		t.Fatalf("capacity after leader withdrawal = %d, error = %v", capacity, err)
	}
	second.establishSourceSession()
	assertPublishedCapacity(t, second.host.sessions["linux"], 1, "capacity after leader withdrawal")
	assertPublishedCapacity(t, second.host.sessions["windows"], 1, "Windows capacity after leader withdrawal")
	exerciseAcceptedJobLifecycleAfterInitialCapacity(t, second, 502)
}

func TestProductContractPostgresLivenessAndForeignRecovery(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	first, candidates := createForeignRecoveryCandidates(t)
	second := joinSharedEndpointRecoveryMember(t, first)
	expireForeignOwnerCapacity(t, first, second)
	recoverForeignProcessing(t, first, second, candidates)
}

type foreignRecoveryCandidates struct {
	unstarted execution.StartRequest
	accepted  execution.StartRequest
}

func createForeignRecoveryCandidates(t *testing.T) (*e2eHarness, foreignRecoveryCandidates) {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	first := newE2EHarness(t, config.StateConfig{Postgres: &settings}, "e2e-recovery-member-a", false)
	first.establishSourceSession()
	assertPublishedCapacity(t, first.host.sessions["linux"], 1, "initial recovery Linux capacity")
	assertPublishedCapacity(t, first.host.sessions["windows"], 1, "initial recovery Windows capacity")
	first.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 601, QueueTime: first.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	unstarted := awaitProviderStart(t, "original unstarted provider start", first.provider.starts)
	assertPublishedCapacity(t, first.host.sessions["linux"], 0, "foreign processing capacity")
	first.deliverTo("windows", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 602, QueueTime: first.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	first.completeMaintenanceCycle(first.execution)
	accepted := awaitProviderStart(t, "original accepted provider start", first.windowsProvider.starts)
	first.deliverTo("windows", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(accepted.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 602}}},
	})
	return first, foreignRecoveryCandidates{unstarted: unstarted, accepted: accepted}
}

func createForeignRecoveryCandidate(t *testing.T) (*e2eHarness, execution.StartRequest) {
	t.Helper()
	harness, candidates := createForeignRecoveryCandidates(t)
	return harness, candidates.unstarted
}

func joinSharedEndpointRecoveryMember(t *testing.T, first *e2eHarness) *e2eHarness {
	t.Helper()
	second := newForeignRecoveryMember(t, first)
	second.provider.setEndpointID(first.provider.EndpointID())
	return prepareForeignRecoveryMember(t, first, second)
}

func newForeignRecoveryMember(t *testing.T, first *e2eHarness) *e2eHarness {
	t.Helper()
	return newE2EHarness(t, first.configured.State, "e2e-recovery-member-b", true)
}

func prepareForeignRecoveryMember(t *testing.T, first *e2eHarness, second *e2eHarness) *e2eHarness {
	t.Helper()
	second.await("recovery member inventory", second.provider.refreshed)
	second.completeMaintenanceCycle(second.membership)
	second.completeMaintenanceCycle(second.source)
	assertAvailableClusterCapacity(t, second, 1, "two live members with one processing claim")
	rotateAndAssertCapacity(t, first, "linux", 1, 1, "joined-member available capacity")
	return second
}

func expireForeignOwnerCapacity(t *testing.T, first *e2eHarness, second *e2eHarness) {
	t.Helper()
	first.advanceClock(runtimeMemberTTL + time.Second)
	first.completeMaintenanceCycle(first.membership)
	assertAvailableClusterCapacity(t, first, 0, "expired follower heartbeat")
	rotateAndAssertCapacity(t, first, "linux", 1, 0, "heartbeat-expired available capacity")
	second.advanceClock(runtimeMemberTTL + time.Second)
}

func recoverForeignProcessing(
	t *testing.T,
	first *e2eHarness,
	second *e2eHarness,
	candidates foreignRecoveryCandidates,
) {
	t.Helper()
	first.close()
	second.establishSourceSession()
	assertForeignRecoveryRetention(t, second, first.memberName, candidates, "31 seconds")
	second.completeMaintenanceCycle(second.execution)
	assertNoForeignRecoveryStart(t, second, "31 seconds")
	second.advanceClock(5*time.Minute - time.Second - runtimeMemberTTL - time.Second)
	second.completeMaintenanceCycle(second.execution)
	assertForeignRecoveryRetention(t, second, first.memberName, candidates, "4 minutes 59 seconds")
	assertNoForeignRecoveryStart(t, second, "4 minutes 59 seconds")
	second.advanceClock(2 * time.Second)
	second.completeMaintenanceCycle(second.execution)
	recovered := awaitProviderStart(t, "foreign processing recovery", second.provider.starts)
	if recovered.JobID != candidates.unstarted.JobID {
		t.Fatalf("foreign recovery JobID = %s, want durable JobID %s", recovered.JobID, candidates.unstarted.JobID)
	}
	processing, err := second.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].Job.ID != candidates.unstarted.JobID ||
		processing[0].MemberID != core.MemberID(second.memberName) {
		t.Fatalf("recovered processing = %+v, error = %v", processing, err)
	}
	assertAcceptedForeignProcessingRetained(t, second, first.memberName, candidates.accepted.JobID)
}

func assertForeignRecoveryRetention(
	t *testing.T,
	harness *e2eHarness,
	owner string,
	candidates foreignRecoveryCandidates,
	phase string,
) {
	t.Helper()
	linux, linuxErr := harness.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	if linuxErr != nil || len(linux) != 1 || linux[0].Job.ID != candidates.unstarted.JobID ||
		linux[0].MemberID != core.MemberID(owner) || linux[0].RunnerAccepted {
		t.Fatalf("unstarted foreign processing at %s = %+v, error = %v", phase, linux, linuxErr)
	}
	assertAcceptedForeignProcessingRetained(t, harness, owner, candidates.accepted.JobID)
}

func assertAcceptedForeignProcessingRetained(
	t *testing.T,
	harness *e2eHarness,
	owner string,
	wantJobID core.JobID,
) {
	t.Helper()
	windows, err := harness.runtime.state.store.ProcessingByPool(context.Background(), "windows")
	if err != nil || len(windows) != 1 || windows[0].Job.ID != wantJobID ||
		windows[0].MemberID != core.MemberID(owner) || !windows[0].RunnerAccepted {
		t.Fatalf("accepted foreign processing = %+v, error = %v", windows, err)
	}
}

func assertNoForeignRecoveryStart(t *testing.T, harness *e2eHarness, phase string) {
	t.Helper()
	select {
	case started := <-harness.provider.starts:
		t.Fatalf("foreign processing started at %s before the five-minute lease: %+v", phase, started)
	default:
	}
}

func TestProductContractPostgresLeadershipFencesLateGitHostSession(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture, first, gate := startPostgresFencingTerm(t)
	second := transferPostgresLeadership(t, fixture, first.configured.State)
	assertOldPostgresTermFenced(t, first, second, gate)
}

func startPostgresFencingTerm(
	t *testing.T,
) (*e2ePostgresFixture, *e2eHarness, *e2eSessionOpenGate) {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}
	first := newE2EHarness(t, state, "e2e-fencing-member-a", false)
	gate := first.host.blockNextSessionOpen("linux")
	first.completeMaintenanceCycle(first.membership)
	first.completeMaintenanceCycle(first.source)
	for _, runnerPool := range []string{"linux", "windows"} {
		first.await("fencing registration for "+runnerPool, first.host.registrationOK[runnerPool])
	}
	first.completeMaintenanceCycle(first.source)
	first.await("blocked old-term session open", gate.started)
	first.await("unblocked old-term Windows session", first.host.sessionOK["windows"])
	return fixture, first, gate
}

func transferPostgresLeadership(
	t *testing.T,
	fixture *e2ePostgresFixture,
	state config.StateConfig,
) *e2eHarness {
	t.Helper()
	second := newE2EHarness(t, state, "e2e-fencing-member-b", false)
	second.completeMaintenanceCycle(second.membership)
	second.completeMaintenanceCycle(second.source)
	if err := fixture.terminateLeaderConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	second.completeMaintenanceCycle(second.source)
	for _, runnerPool := range []string{"linux", "windows"} {
		second.await("new leader session for "+runnerPool, second.host.sessionOK[runnerPool])
	}
	return second
}

func assertOldPostgresTermFenced(
	t *testing.T,
	first *e2eHarness,
	second *e2eHarness,
	gate *e2eSessionOpenGate,
) {
	t.Helper()
	completed := first.laneCompleted[first.source].count()
	first.source.Tick(first.now())
	first.await("old-term cancellation", gate.canceled)
	close(gate.release)
	first.laneCompleted[first.source].waitAfter(t, "old leader fencing cycle", completed)
	first.await("late old-term session retirement", first.host.sessions["linux"].closed)
	if opens := second.host.sessionOpenCount("linux"); opens != 1 {
		t.Fatalf("new leader Linux session opens = %d, want 1", opens)
	}
}

func assertAvailableClusterCapacity(t *testing.T, harness *e2eHarness, want int, phase string) {
	t.Helper()
	cutoff := harness.now().Add(-runtimeMemberTTL)
	capacity, err := harness.runtime.state.store.AvailableRunnerPoolCapacity(context.Background(), "linux", cutoff)
	if err != nil || capacity != want {
		t.Fatalf("%s available capacity = %d, error = %v, want %d", phase, capacity, err, want)
	}
}

func exerciseClusterJob(t *testing.T, leader *e2eHarness, members []*e2eHarness, requestID int64) {
	t.Helper()
	queueTime := leader.currentTime.AddDate(-10, 0, 0)
	leader.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: queueTime,
		}}},
	})
	owner, started := awaitClusterStart(t, members)
	assertSingleClusterClaim(t, leader, members, started.JobID)
	leader.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	leader.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(started.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	drainSignals(owner.provider.inspected)
	owner.completeMaintenanceCycle(owner.execution)
	awaitRunnerInstance(t, "clustered terminal cleanup", owner.provider.destroys)
	owner.await("clustered cleanup inventory inspection", owner.provider.inspected)
	owner.completeMaintenanceCycle(owner.execution)
	owner.await("clustered post-cleanup cycle", owner.provider.inspected)
}

func assertSingleClusterClaim(
	t *testing.T,
	leader *e2eHarness,
	members []*e2eHarness,
	wantJobID core.JobID,
) {
	t.Helper()
	records, err := leader.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(records) != 1 || records[0].Job.ID != wantJobID {
		t.Fatalf("cluster processing = %+v, error = %v, want one claim for %s", records, err, wantJobID)
	}
	attempts := 0
	for _, member := range members {
		attempts += member.provider.attemptCount()
	}
	if attempts != 1 {
		t.Fatalf("cluster provider attempts = %d, want exactly 1 after durable claim", attempts)
	}
}

func awaitClusterStart(t *testing.T, members []*e2eHarness) (*e2eHarness, execution.StartRequest) {
	t.Helper()
	if len(members) != 2 {
		t.Fatal("cluster tracer requires exactly two members")
	}
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case started := <-members[0].provider.starts:
		return members[0], started
	case started := <-members[1].provider.starts:
		return members[1], started
	case <-timer.C:
		t.Fatal("timed out waiting for clustered provider start")
		return nil, execution.StartRequest{}
	}
}
