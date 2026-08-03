package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestScaleSetHandlerReconcilesAssignedJobs(t *testing.T) {
	provider := &fakeScalerProvider{}
	handler := scaleSetHandler{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name:       "linux",
		MaxRunners: 3,
		Provider:   provider,
		JITConfig:  fakeJITConfigGenerator{},
	}}}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
	})
	if err != nil {
		t.Fatalf("expected handler success, got %v", err)
	}
	if len(provider.started) != 2 {
		t.Fatalf("expected two starts, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerQueuesAvailableAndAssignedJobsAndRemovesCompleted(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(2)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	queueTime := time.Now()
	message := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: queueTime}}},
		JobAssignedMessages:  []*upstream.JobAssigned{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2, QueueTime: queueTime.Add(time.Second)}}},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2}}},
	}
	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatalf("expected message scheduling, got %v", err)
	}
	if len(provider.started) != 1 {
		t.Fatalf("expected only non-completed demand to start, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerUsesStatisticsForZeroIdentityAssignedJobs(t *testing.T) {
	fixture := newAssignedStatisticsFixture()
	if err := fixture.handler.HandleMessage(context.Background(), fixture.message); err != nil {
		t.Fatalf("expected assigned capacity reconciliation, got %v", err)
	}
	if len(fixture.provider.started) != 2 {
		t.Fatalf("expected two assigned runners, got %#v", fixture.provider.started)
	}
	for _, startedRunner := range fixture.provider.started {
		if strings.HasSuffix(startedRunner.RunnerName, "-0") {
			t.Fatalf("expected assigned runner without zero request identity, got %q", startedRunner.RunnerName)
		}
	}
	if !strings.Contains(fixture.logs.String(), "assigned capacity reconciled") ||
		strings.Contains(fixture.logs.String(), "requestID=0") {
		t.Fatalf("expected aggregate assigned-capacity log without fabricated identity, got %q", fixture.logs.String())
	}
}

func TestCoordinatedScaleSetHandlerLogsStatisticsBeforeReconciliation(t *testing.T) {
	logs := &bytes.Buffer{}
	worker := &statisticsLogObservingWorker{logs: logs}
	coordinator := scaler.NewCoordinator(3)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator,
		Repository:  "repo",
		ScaleSet:    "linux",
		Logger:      slog.New(slog.NewTextHandler(logs, nil)),
	}
	message := &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{
		TotalAvailableJobs: 1, TotalAcquiredJobs: 2, TotalAssignedJobs: 3, TotalRunningJobs: 4,
		TotalRegisteredRunners: 5, TotalBusyRunners: 6, TotalIdleRunners: 7,
	}}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if !worker.loggedBeforeReconcile {
		t.Fatalf("session statistics were not logged before reconciliation: %q", logs.String())
	}
	if !strings.Contains(logs.String(), "trigger=message") {
		t.Fatalf("message statistics source was not logged: %q", logs.String())
	}
}

type statisticsLogObservingWorker struct {
	recordingRecoveryWorker
	logs                  *bytes.Buffer
	runningCalls          int
	loggedBeforeReconcile bool
}

func (worker *statisticsLogObservingWorker) Running(ctx context.Context) (int, error) {
	if worker.runningCalls == 0 {
		worker.loggedBeforeReconcile = containsStandaloneSessionStatistics(worker.logs.String())
	}
	worker.runningCalls++
	return worker.recordingRecoveryWorker.Running(ctx)
}

type assignedStatisticsFixture struct {
	logs     *bytes.Buffer
	provider *fakeScalerProvider
	handler  coordinatedScaleSetHandler
	message  *upstream.RunnerScaleSetMessage
}

func newAssignedStatisticsFixture() assignedStatisticsFixture {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(2)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
	}}})
	return assignedStatisticsFixture{
		logs: logs, provider: provider,
		handler: coordinatedScaleSetHandler{Logger: logger, ScaleSet: "self-hosted", Coordinator: coordinator, Repository: "repo"},
		message: &upstream.RunnerScaleSetMessage{
			Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2}, JobAssignedMessages: []*upstream.JobAssigned{{}, {}},
		},
	}
}

func TestCoordinatedScaleSetHandlerAppliesLowerAssignedCountBeforeScheduling(t *testing.T) {
	capacityHolder := &recordingRecoveryWorker{running: 1}
	waitingWorker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("capacity-holder", capacityHolder)
	coordinator.AddRepository("waiting", waitingWorker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "waiting"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}

	capacityHolder.running = 0
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
	}); err != nil {
		t.Fatal(err)
	}
	if waitingWorker.assigned != 0 {
		t.Fatalf("expected current zero assigned count to prevent stale provisioning, got %d", waitingWorker.assigned)
	}
}

func TestCoordinatedWorkerReportsUsabilityFailure(t *testing.T) {
	store := &faultLifecycleDemandStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), inventoryErr: errors.New("inventory")}
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", Repository: "repo", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry(), Lifecycle: store,
	}}}
	if _, err := worker.ReconcileRunnerUsability(context.Background()); err == nil {
		t.Fatal("expected usability failure")
	}
}

func TestCoordinatedHandlerReportsLifecyclePersistenceFailures(t *testing.T) {
	store := &faultLifecycleDemandStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), busyErr: errors.New("busy")}
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	message := &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: "runner"}}}
	if err := handler.HandleMessage(context.Background(), message); err == nil {
		t.Fatal("expected busy persistence failure")
	}
	store.busyErr = nil
	store.completeErr = errors.New("complete")
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "runner"}},
	}); err == nil {
		t.Fatal("expected cleanup persistence failure")
	}
}

func TestMissingGitHubRegistrationIsReapedWhenItBlocksDemand(t *testing.T) {
	ctx := context.Background()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	provider := &incidentLifecycleProvider{}
	jit := newIncidentJITRegistry()
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 1, Provider: provider, JITConfig: jit,
	}}
	if err := coordinator.ConfigureRunnerLifecycle(&reconciler, "repo", scaler.RunnerProviderDocker); err != nil {
		t.Fatal(err)
	}
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: reconciler, Repository: "repo", ScaleSet: "linux"})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	delete(jit.runners, provider.started[0].RunnerName)
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 1 || pending[0].ReapReason != "github_registration_absent" {
		t.Fatalf("missing registration pending reaps = %#v, %v", pending, err)
	}
}
