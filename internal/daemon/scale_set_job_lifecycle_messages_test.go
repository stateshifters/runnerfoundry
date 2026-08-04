package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

func TestJobStartedMarksExactRunnerBusyBeforeMessageSucceeds(t *testing.T) {
	ctx := context.Background()
	fixture := incidentLifecycleFixture(t, 1)
	handler, provider, store := fixture.handler, fixture.provider, fixture.store
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobStartedMessages: []*upstream.JobStarted{{
			RunnerID: 99, RunnerName: provider.started[0].RunnerName,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	lifecycles, err := store.RunnerLifecycles(ctx, "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Phase != scaler.RunnerBusy || lifecycles[0].GitHubRunnerID != 99 {
		t.Fatalf("started lifecycle = %#v, %v", lifecycles, err)
	}
}

func TestCanceledCompletionsReapIdentityBoundAndIdentityFreeRunners(t *testing.T) {
	ctx := context.Background()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 2, store)
	now := time.Now().UTC()
	seedCanceledCompletionRunners(t, store, now)
	if err := coordinator.Apply(ctx, []scaler.JobDemand{{Repository: "repo", RequestID: 42, QueueTime: now}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{
			{Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42}},
			{Result: "canceled", RunnerName: "linux-1"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 2 {
		t.Fatalf("canceled pending reaps = %#v, %v", pending, err)
	}
}

func seedCanceledCompletionRunners(t *testing.T, store *scaler.MemoryDemandStore, now time.Time) {
	t.Helper()
	for _, runner := range []scaler.RunnerLifecycle{
		{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker, RunnerName: "linux-42", RequestID: 42, Phase: scaler.RunnerWaitingForJob, CreatedAt: now},
		{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker, RunnerName: "linux-1", Phase: scaler.RunnerWaitingForJob, CreatedAt: now},
	} {
		if err := store.RegisterRunner(context.Background(), runner); err != nil {
			t.Fatal(err)
		}
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

type incidentLifecycleProvider struct {
	instances []scaler.RunnerInstance
	started   []scaler.RunnerInstance
	destroyed []string
}

func (provider *incidentLifecycleProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	return append([]scaler.RunnerInstance(nil), provider.instances...), nil
}

func (provider *incidentLifecycleProvider) Start(_ context.Context, request scaler.RunnerStartRequest) error {
	instance := scaler.RunnerInstance{
		ID: fmt.Sprintf("instance-%d", len(provider.started)+1), RunnerName: request.RunnerName, State: scaler.RunnerStateRunning,
	}
	provider.started = append(provider.started, instance)
	provider.instances = append(provider.instances, instance)
	return nil
}

func (provider *incidentLifecycleProvider) Destroy(_ context.Context, instance scaler.RunnerInstance) error {
	provider.destroyed = append(provider.destroyed, instance.ID)
	for index := range provider.instances {
		if provider.instances[index].ID == instance.ID {
			provider.instances = append(provider.instances[:index], provider.instances[index+1:]...)
			break
		}
	}
	return nil
}

type incidentJITRegistry struct {
	nextID  int64
	runners map[string]scaler.JITRunner
}

func newIncidentJITRegistry() *incidentJITRegistry {
	return &incidentJITRegistry{runners: map[string]scaler.JITRunner{}}
}

func (registry *incidentJITRegistry) Generate(_ context.Context, runnerName string) (scaler.JITRunnerConfig, error) {
	registry.nextID++
	runner := scaler.JITRunner{ID: registry.nextID, Name: runnerName}
	registry.runners[runnerName] = runner
	return scaler.JITRunnerConfig{Runner: runner, JITConfig: "jit"}, nil
}

func (registry *incidentJITRegistry) Remove(_ context.Context, runner scaler.JITRunner) error {
	delete(registry.runners, runner.Name)
	return nil
}

func (registry *incidentJITRegistry) Find(_ context.Context, runnerName string) (scaler.JITRunner, bool, error) {
	runner, found := registry.runners[runnerName]
	return runner, found, nil
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
	registerLibvirtLifecycle(t, store, 42, "repo-linux-42")
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
			Result:         "canceled",
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
	registerLibvirtLifecycle(t, store, 0, "repo-linux-1")
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
	harness := newLibvirtCompletionAcknowledgementHarness(t)
	harness.expectPersistenceFailureWithoutAcknowledgement(t)
	harness.retryWithDurablePersistence(t)
	harness.expectPendingReapAcknowledged(t)
}

type libvirtCompletionAcknowledgementHarness struct {
	store   *failingCompletionLifecycleStore
	client  *ackRecordingMessageClient
	poller  scalesetpoller.Poller
	message *upstream.RunnerScaleSetMessage
}

func newLibvirtCompletionAcknowledgementHarness(t *testing.T) libvirtCompletionAcknowledgementHarness {
	t.Helper()
	store := &failingCompletionLifecycleStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), fail: true}
	registerLibvirtLifecycle(t, store.MemoryDemandStore, 42, "linux-42")
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
	return libvirtCompletionAcknowledgementHarness{store: store, client: client, poller: poller, message: message}
}

func registerLibvirtLifecycle(t *testing.T, store *scaler.MemoryDemandStore, requestID int64, runnerName string) {
	t.Helper()
	if err := store.RegisterRunner(context.Background(), scaler.RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderLibvirt,
		RequestID: requestID, RunnerName: runnerName,
	}); err != nil {
		t.Fatalf("register libvirt lifecycle: %v", err)
	}
}

func (harness libvirtCompletionAcknowledgementHarness) expectPersistenceFailureWithoutAcknowledgement(t *testing.T) {
	t.Helper()
	if err := harness.poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected durable completion failure")
	}
	if len(harness.client.acknowledged) != 0 {
		t.Fatalf("failed persistence acknowledged messages %#v", harness.client.acknowledged)
	}
}

func (harness libvirtCompletionAcknowledgementHarness) retryWithDurablePersistence(t *testing.T) {
	t.Helper()
	harness.store.fail = false
	harness.client.message = harness.message
	if err := harness.poller.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (harness libvirtCompletionAcknowledgementHarness) expectPendingReapAcknowledged(t *testing.T) {
	t.Helper()
	pending, err := harness.store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	acknowledged := harness.client.acknowledged
	if err != nil || len(pending) != 1 || len(acknowledged) != 1 || acknowledged[0] != 7 {
		t.Fatalf("pending=%#v acknowledged=%#v error=%v", pending, acknowledged, err)
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
	if err := store.MemoryDemandStore.CompleteJob(ctx, completion); err != nil {
		return fmt.Errorf("complete in-memory job: %w", err)
	}
	return nil
}

func (store *failingCompletionLifecycleStore) ApplyMessageUpdate(
	ctx context.Context,
	update scaler.MessageUpdate,
) (scaler.MessageUpdateResult, error) {
	if store.fail && len(update.Completed) > 0 {
		return scaler.MessageUpdateResult{}, errors.New("persistence failed")
	}
	result, err := store.MemoryDemandStore.ApplyMessageUpdate(ctx, update)
	if err != nil {
		return scaler.MessageUpdateResult{}, fmt.Errorf("apply in-memory message update: %w", err)
	}
	return result, nil
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
