package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCoordinatorStartsOldestObservedJobWithoutExceedingSharedCapacity(t *testing.T) {
	startOrder := []string{}
	workers := map[string]*recordingWorker{
		"newer": {repository: "newer", startOrder: &startOrder},
		"older": {repository: "older", startOrder: &startOrder},
	}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("newer", workers["newer"])
	coordinator.AddRepository("older", workers["older"])
	queueTime := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	coordinator.Observe(JobDemand{RequestID: 2, Repository: "newer", QueueTime: queueTime.Add(time.Minute)})
	coordinator.Observe(JobDemand{RequestID: 1, Repository: "older", QueueTime: queueTime})

	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("expected scheduling to succeed, got %v", err)
	}
	if len(startOrder) != 1 || startOrder[0] != "older" {
		t.Fatalf("expected oldest repository to receive capacity, got %#v", startOrder)
	}
	if len(workers["older"].acquired) != 1 || workers["older"].acquired[0] != 1 {
		t.Fatalf("expected oldest job to be acquired before start, got %#v", workers["older"].acquired)
	}
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("expected capacity check to succeed, got %v", err)
	}
	if len(startOrder) != 1 {
		t.Fatalf("expected shared capacity to prevent another runner, got %#v", startOrder)
	}
}

func TestCoordinatorDeduplicatesAndRemovesDemand(t *testing.T) {
	startOrder := []string{}
	worker := &recordingWorker{repository: "repo", startOrder: &startOrder}
	coordinator := NewCoordinator(2)
	coordinator.AddRepository("repo", worker)
	demand := JobDemand{RequestID: 7, Repository: "repo", QueueTime: time.Now()}
	coordinator.Observe(demand)
	coordinator.Observe(demand)
	coordinator.Remove("repo", 7)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 0 {
		t.Fatalf("expected removed demand not to start runners, got %#v", startOrder)
	}
}

func TestCoordinatorUpdatesDuplicateDemandWhenGitHubReportsItAcquired(t *testing.T) {
	startOrder := []string{}
	worker := &recordingWorker{repository: "repo", startOrder: &startOrder}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	demand := JobDemand{RequestID: 7, Repository: "repo", QueueTime: time.Now()}
	coordinator.Observe(demand)
	demand.Acquired = true
	coordinator.Observe(demand)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.acquired) != 0 || len(startOrder) != 1 {
		t.Fatalf("expected acquired update to avoid reacquisition, acquired=%#v started=%#v", worker.acquired, startOrder)
	}
}

func TestCoordinatorRemovesRepositoryDemand(t *testing.T) {
	startOrder := []string{}
	worker := &recordingWorker{repository: "removed", startOrder: &startOrder}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("removed", worker)
	coordinator.Observe(JobDemand{RequestID: 1, Repository: "removed", QueueTime: time.Now()})
	if err := coordinator.RemoveRepository(context.Background(), "removed"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 0 {
		t.Fatalf("expected repository demand to be cleared, got %#v", startOrder)
	}
}

func TestCoordinatorSurfacesWorkerFailuresAndSkipsRemovedWorkers(t *testing.T) {
	coordinator := NewCoordinator(2)
	coordinator.AddRepository("count-fails", failingWorker{runningErr: errors.New("count failed")})
	if err := coordinator.Schedule(context.Background()); err == nil {
		t.Fatal("expected running count failure")
	}
	if err := coordinator.RemoveRepository(context.Background(), "count-fails"); err != nil {
		t.Fatal(err)
	}
	coordinator.Observe(JobDemand{RequestID: 1, Repository: "removed", QueueTime: time.Now()})
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("expected removed worker demand to be skipped, got %v", err)
	}
	coordinator.AddRepository("start-fails", failingWorker{startErr: errors.New("start failed")})
	coordinator.Observe(JobDemand{RequestID: 2, Repository: "start-fails", QueueTime: time.Now()})
	if err := coordinator.Schedule(context.Background()); err == nil {
		t.Fatal("expected start failure")
	}
}

func TestCoordinatorLeavesDemandQueuedWhenAcquisitionFails(t *testing.T) {
	coordinator := NewCoordinator(1)
	worker := &failingWorker{acquireErr: errors.New("acquire failed")}
	coordinator.AddRepository("repo", worker)
	coordinator.Observe(JobDemand{RequestID: 1, Repository: "repo", QueueTime: time.Now()})
	if err := coordinator.Schedule(context.Background()); err == nil {
		t.Fatal("expected acquisition failure")
	}
	worker.acquireErr = nil
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("expected queued demand retry, got %v", err)
	}
}

func TestCoordinatorUsesObservationOrderForEqualQueueTimes(t *testing.T) {
	startOrder := []string{}
	coordinator := NewCoordinator(2)
	coordinator.AddRepository("first", &recordingWorker{repository: "first", startOrder: &startOrder})
	coordinator.AddRepository("second", &recordingWorker{repository: "second", startOrder: &startOrder})
	queueTime := time.Now()
	coordinator.Observe(JobDemand{RequestID: 1, Repository: "first", QueueTime: queueTime})
	coordinator.Observe(JobDemand{RequestID: 2, Repository: "second", QueueTime: queueTime})
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 2 || startOrder[0] != "first" {
		t.Fatalf("expected observation-order tie break, got %#v", startOrder)
	}
}

func TestCoordinatorRecoversProviderVisibleDemand(t *testing.T) {
	store := newMemoryDemandStore()
	worker := &recordingWorker{existingRequestID: 42}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []JobDemand{{Repository: "repo", RequestID: 42, QueueTime: time.Now()}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.acquired) != 0 {
		t.Fatalf("expected existing runner to complete recovery, got %#v", worker.acquired)
	}
}

func TestCoordinatorReportsRecoveryFailure(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", failingWorker{existsErr: errors.New("inspect failed")})
	coordinator.Observe(JobDemand{Repository: "repo", RequestID: 1, QueueTime: time.Now()})
	if err := coordinator.Schedule(context.Background()); err == nil {
		t.Fatal("expected recovery failure")
	}
	store := NewMemoryDemandStore()
	if err := store.RemoveScaleSet(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorReportsDurableStoreFailures(t *testing.T) {
	ctx := context.Background()
	tests := map[string]*failingDemandStore{
		"load":                {orderedErr: errors.New("load failed")},
		"recover delete":      {demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 1}}}, deleteErr: errors.New("delete failed")},
		"persist acquisition": {demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 1}}}, applyErr: errors.New("apply failed")},
		"complete admission":  {demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 1, Acquired: true}}}, deleteErr: errors.New("delete failed")},
	}
	for name, store := range tests {
		t.Run(name, func(t *testing.T) {
			coordinator := NewDurableCoordinator("linux", 1, store)
			startOrder := []string{}
			worker := &recordingWorker{startOrder: &startOrder}
			if name == "recover delete" {
				worker.existingRequestID = 1
			}
			coordinator.AddRepository("repo", worker)
			if err := coordinator.Schedule(ctx); err == nil {
				t.Fatal("expected durable store failure")
			}
		})
	}
}

type failingDemandStore struct {
	demand     []QueuedDemand
	orderedErr error
	applyErr   error
	deleteErr  error
}

func (store *failingDemandStore) Apply(context.Context, string, []JobDemand, []DemandRemoval) error {
	return store.applyErr
}
func (store *failingDemandStore) RemoveRepository(context.Context, string, string) error { return nil }
func (store *failingDemandStore) RemoveScaleSet(context.Context, string) error           { return nil }
func (store *failingDemandStore) Ordered(context.Context, string) ([]QueuedDemand, error) {
	return store.demand, store.orderedErr
}
func (store *failingDemandStore) Delete(context.Context, string, string, int64) error {
	return store.deleteErr
}

type recordingWorker struct {
	repository        string
	running           int
	startOrder        *[]string
	acquired          []int64
	existingRequestID int64
}

func (worker *recordingWorker) Acquire(_ context.Context, requestID int64) error {
	worker.acquired = append(worker.acquired, requestID)
	return nil
}

type failingWorker struct {
	runningErr error
	acquireErr error
	startErr   error
	existsErr  error
}

func (worker failingWorker) Running(context.Context) (int, error) { return 0, worker.runningErr }
func (worker failingWorker) Exists(context.Context, int64) (bool, error) {
	return false, worker.existsErr
}
func (worker failingWorker) Acquire(context.Context, int64) error  { return worker.acquireErr }
func (worker failingWorker) StartOne(context.Context, int64) error { return worker.startErr }

func (worker *recordingWorker) Running(context.Context) (int, error) {
	return worker.running, nil
}
func (worker *recordingWorker) Exists(_ context.Context, requestID int64) (bool, error) {
	return requestID == worker.existingRequestID, nil
}

func (worker *recordingWorker) StartOne(context.Context, int64) error {
	*worker.startOrder = append(*worker.startOrder, worker.repository)
	worker.running++
	return nil
}
