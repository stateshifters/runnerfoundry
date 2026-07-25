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
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
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
		!strings.Contains(fixture.logs.String(), "startedRunners=2") ||
		strings.Contains(fixture.logs.String(), "requestID=0") {
		t.Fatalf("expected aggregate assigned-capacity log without fabricated identity, got %q", fixture.logs.String())
	}
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

func TestCoordinatedScaleSetHandlerRejectsMalformedAvailableDemand(t *testing.T) {
	worker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}

	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{}},
	})
	if err == nil || !strings.Contains(err.Error(), "positive runner request ID and queue time") {
		t.Fatalf("expected malformed available demand rejection, got %v", err)
	}
	if len(worker.acquired) != 0 || len(worker.started) != 0 {
		t.Fatalf("expected no malformed demand side effects, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRequiresStatisticsAndStartsAssignedWorker(t *testing.T) {
	worker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{}); err == nil {
		t.Fatal("expected missing statistics failure")
	}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if worker.assigned != 1 {
		t.Fatalf("expected assigned worker start, got %d", worker.assigned)
	}
}

func TestCoordinatedScaleSetHandlerIgnoresZeroIdentityCompletion(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 48, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobStartedMessages:   []*upstream.JobStarted{{}},
		JobCompletedMessages: []*upstream.JobCompleted{{}},
	}); err != nil {
		t.Fatal(err)
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 1 || worker.started[0] != 48 {
		t.Fatalf("expected valid demand to survive zero-ID completion, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRemovesRepeatedPositiveIdentityCompletionIdempotently(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 49, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	completion := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 49}}},
	}
	for range 2 {
		if err := handler.HandleMessage(context.Background(), completion); err != nil {
			t.Fatal(err)
		}
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 0 {
		t.Fatalf("expected positive completion identity to remove demand, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerPersistsLibvirtReapBeforeCompletionSucceeds(t *testing.T) {
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingRecoveryWorker{})
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator,
		Repository:  "repo",
		ScaleSet:    "linux",
		ReapLibvirt: true,
	}
	message := &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			Result:         "cancelled",
			RunnerName:     "repo-linux-42",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42},
		}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].RunnerName != "repo-linux-42" {
		t.Fatalf("pending reaps = %#v", pending)
	}
}

func TestCoordinatedScaleSetHandlerPersistsIdentityFreeLibvirtCompletionByRunnerName(t *testing.T) {
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingRecoveryWorker{})
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator, Repository: "repo", ScaleSet: "linux", ReapLibvirt: true,
	}
	message := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "repo-linux-1"}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	if err != nil || len(pending) != 1 || pending[0].RunnerName != "repo-linux-1" {
		t.Fatalf("pending reaps = %#v, %v", pending, err)
	}
}

func TestLibvirtCompletionIsNotAcknowledgedUntilCleanupIntentPersists(t *testing.T) {
	store := &failingCompletionLifecycleStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), fail: true}
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingRecoveryWorker{})
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator, Repository: "repo", ScaleSet: "linux", ReapLibvirt: true,
	}
	message := &upstream.RunnerScaleSetMessage{
		MessageID: 7, Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: "linux-42", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42},
		}},
	}
	client := &ackRecordingMessageClient{message: message}
	poller := scalesetpoller.Poller{Client: client, Handler: handler}

	if err := poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected durable completion failure")
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("failed persistence acknowledged messages %#v", client.acknowledged)
	}
	store.fail = false
	client.message = message
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	if err != nil || len(pending) != 1 || len(client.acknowledged) != 1 || client.acknowledged[0] != 7 {
		t.Fatalf("pending=%#v acknowledged=%#v error=%v", pending, client.acknowledged, err)
	}
}

type failingCompletionLifecycleStore struct {
	*scaler.MemoryDemandStore
	fail bool
}

func (store *failingCompletionLifecycleStore) CompleteJob(ctx context.Context, completion scaler.JobCompletion) error {
	if store.fail {
		return errors.New("persistence failed")
	}
	return store.MemoryDemandStore.CompleteJob(ctx, completion)
}

type ackRecordingMessageClient struct {
	message      *upstream.RunnerScaleSetMessage
	acknowledged []int
}

func (client *ackRecordingMessageClient) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	message := client.message
	client.message = nil
	if message == nil {
		return nil, scalesetpoller.ErrNoMessage
	}
	return message, nil
}

func (client *ackRecordingMessageClient) DeleteMessage(_ context.Context, messageID int) error {
	client.acknowledged = append(client.acknowledged, messageID)
	return nil
}
