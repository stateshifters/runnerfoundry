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

func TestCoordinatedScaleSetHandlerRetainsDemandUntilJobStarted(t *testing.T) {
	fixture := newRetainedDemandFixture(t)
	if err := fixture.handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:          &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAssignedMessages: []*upstream.JobAssigned{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 50}}},
	}); err != nil {
		t.Fatal(err)
	}
	fixture.worker.running = 0
	if err := fixture.coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fixture.worker.started) != 1 || fixture.worker.started[0] != 50 {
		t.Fatalf("expected JobAssigned demand to remain provisionable, got %#v", fixture.worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRemovesDemandWhenJobStarts(t *testing.T) {
	fixture := newRetainedDemandFixture(t)
	started := &upstream.RunnerScaleSetMessage{
		Statistics:         &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 50}}},
	}
	for range 2 {
		if err := fixture.handler.HandleMessage(context.Background(), started); err != nil {
			t.Fatal(err)
		}
	}
	fixture.worker.running = 0
	if err := fixture.coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fixture.worker.started) != 0 {
		t.Fatalf("expected JobStarted to complete durable demand, got %#v", fixture.worker.started)
	}
}

type retainedDemandFixture struct {
	worker      *recordingRecoveryWorker
	coordinator *scaler.Coordinator
	handler     coordinatedScaleSetHandler
}

func newRetainedDemandFixture(t *testing.T) retainedDemandFixture {
	t.Helper()
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 50, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	return retainedDemandFixture{
		worker: worker, coordinator: coordinator,
		handler: coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"},
	}
}

func TestCoordinatedScaleSetHandlerLeavesAssignedReconciliationRetryable(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 1, Provider: failingStartProvider{}, JITConfig: fakeJITConfigGenerator{},
	}}})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:          &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAssignedMessages: []*upstream.JobAssigned{{}},
	})
	if err == nil || !strings.Contains(err.Error(), "schedule shared runner capacity") {
		t.Fatalf("expected retryable assigned reconciliation failure, got %v", err)
	}
}

func TestCoordinatedScaleSetLogsJobServingLifecycle(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	provider := &fakeScalerProvider{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Logger: logger, Repository: "repo", ScaleSet: "linux",
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Logger: logger, ScaleSet: "linux", Coordinator: coordinator, Repository: "repo"}
	queueTime := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: queueTime}}},
	}); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	for _, expected := range []string{
		"job demand observed", "status=available", "requestID=42", "repository=repo", "scaleSet=linux",
		"job acquired", "runner started",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected log %q in %q", expected, output)
		}
	}
}

func TestCoordinatedScaleSetHandlerLeavesMessageRetryableWhenSchedulingFails(t *testing.T) {
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: &fakeScalerProvider{listErr: errors.New("list failed")},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: time.Now()}}},
	})
	if err == nil || !strings.Contains(err.Error(), "schedule shared runner capacity") {
		t.Fatalf("expected scheduling failure, got %v", err)
	}
}

func TestCoordinatedScaleSetHandlerDoesNotDuplicateRunnerWhenAvailableDemandIsRedelivered(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	message := &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 50, QueueTime: time.Now(),
		}}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(provider.started) != 1 {
		t.Fatalf("expected redelivery to reuse the provider-visible runner, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerLeavesMessageRetryableWhenPersistenceFails(t *testing.T) {
	coordinator := scaler.NewDurableCoordinator("linux", 1, failingDaemonDemandStore{})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: time.Now()}}},
	})
	if err == nil || !strings.Contains(err.Error(), "persist scale-set message update") {
		t.Fatalf("expected persistence failure, got %v", err)
	}
}

func TestCoordinatedScaleSetHandlerReportsCompletedDemandRemovalFailure(t *testing.T) {
	coordinator := scaler.NewDurableCoordinator("linux", 1, failingDaemonDemandStore{})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "persist atomic scale-set message") {
		t.Fatalf("completion error = %v", err)
	}
}

type failingDaemonDemandStore struct{}

func (failingDaemonDemandStore) ApplyMessageUpdate(context.Context, scaler.MessageUpdate) (scaler.MessageUpdateResult, error) {
	return scaler.MessageUpdateResult{}, errors.New("persist failed")
}

func (failingDaemonDemandStore) Apply(context.Context, string, []scaler.JobDemand, []scaler.DemandRemoval) error {
	return errors.New("persist failed")
}
func (failingDaemonDemandStore) RemoveRepository(context.Context, string, string) error { return nil }
func (failingDaemonDemandStore) RemoveScaleSet(context.Context, string) error           { return nil }
func (failingDaemonDemandStore) Ordered(context.Context, string) ([]scaler.QueuedDemand, error) {
	return nil, nil
}
func (failingDaemonDemandStore) Delete(context.Context, string, string, int64) error { return nil }
