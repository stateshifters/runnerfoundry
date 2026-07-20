package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCoordinatorPreservesDemandAcrossCancellationBoundaries(t *testing.T) {
	startOrder := []string{}
	tests := map[string]struct {
		store  *failingDemandStore
		worker RepositoryWorker
	}{
		"running inspection": {
			store:  &failingDemandStore{demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 11, Acquired: true}}}},
			worker: failingWorker{runningErr: context.Canceled},
		},
		"named runner inspection": {
			store:  &failingDemandStore{demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 11, Acquired: true}}}},
			worker: failingWorker{existsErr: context.Canceled},
		},
		"acquisition": {
			store:  &failingDemandStore{demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 11}}}},
			worker: failingWorker{acquireErr: context.Canceled},
		},
		"acquisition persistence": {
			store:  &failingDemandStore{demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 11}}}, applyErr: context.Canceled},
			worker: &recordingWorker{repository: "repo", startOrder: &startOrder},
		},
		"runner start": {
			store:  &failingDemandStore{demand: []QueuedDemand{{JobDemand: JobDemand{Repository: "repo", RequestID: 11, Acquired: true}}}},
			worker: failingWorker{startErr: context.Canceled},
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			coordinator := NewDurableCoordinator("linux", 1, testCase.store)
			coordinator.AddRepository("repo", testCase.worker)
			err := coordinator.Schedule(context.Background())
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected cancellation, got %v", err)
			}
			if len(testCase.store.demand) != 1 || testCase.store.demand[0].RequestID != 11 {
				t.Fatalf("expected demand preservation, got %#v", testCase.store.demand)
			}
		})
	}
}

func TestCoordinatorReportsDemandMutationFailures(t *testing.T) {
	store := &failingDemandStore{applyErr: errors.New("apply failed"), removeRepositoryErr: errors.New("remove failed")}
	coordinator := NewDurableCoordinator("linux", 1, store)
	if err := coordinator.Apply(context.Background(), nil, nil); err == nil {
		t.Fatal("expected apply failure")
	}
	if err := coordinator.RemoveRepository(context.Background(), "repo"); err == nil {
		t.Fatal("expected repository removal failure")
	}
}

func TestCoordinatorReconcilesAssignedJobsWithinSharedCapacity(t *testing.T) {
	firstWorker := &recordingWorker{running: 1}
	secondWorker := &recordingWorker{}
	coordinator := NewCoordinator(2)
	coordinator.AddRepository("first", firstWorker)
	coordinator.AddRepository("second", secondWorker)

	result, err := coordinator.ReconcileAssigned(context.Background(), "second", 2)
	if err != nil {
		t.Fatal(err)
	}
	if secondWorker.assignedStarts != 1 || result.StartedRunners != 1 || result.RunningRunners != 0 {
		t.Fatalf("expected one remaining shared slot to start, worker=%d result=%#v", secondWorker.assignedStarts, result)
	}
}

func TestCoordinatorRetainsAssignedDemandUntilCapacityReturns(t *testing.T) {
	capacityHolder := &recordingWorker{running: 1}
	waitingWorker := &recordingWorker{}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("capacity-holder", capacityHolder)
	coordinator.AddRepository("waiting", waitingWorker)

	result, err := coordinator.ReconcileAssigned(context.Background(), "waiting", 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.StartedRunners != 0 || waitingWorker.assignedStarts != 0 {
		t.Fatalf("expected full capacity to defer assigned demand, result=%#v starts=%d", result, waitingWorker.assignedStarts)
	}

	capacityHolder.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if waitingWorker.assignedStarts != 1 {
		t.Fatalf("expected retained assigned demand to start after capacity returned, got %d", waitingWorker.assignedStarts)
	}
}

func TestCoordinatorRemovesAssignedDemandWithRepository(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.ObserveAssigned("repo", 1)
	if err := coordinator.RemoveRepository(context.Background(), "repo"); err != nil {
		t.Fatal(err)
	}
	worker := &recordingWorker{}
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if worker.assignedStarts != 0 {
		t.Fatalf("expected repository removal to discard assigned demand, got %d starts", worker.assignedStarts)
	}
}

func TestCoordinatorSchedulesDurableFIFOBeforeAssignedCapacity(t *testing.T) {
	startOrder := []string{}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("fifo", &recordingWorker{repository: "fifo", startOrder: &startOrder})
	coordinator.AddRepository("assigned", &recordingWorker{repository: "assigned", startOrder: &startOrder})
	coordinator.Observe(JobDemand{Repository: "fifo", RequestID: 1, QueueTime: time.Now(), Acquired: true})

	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.ReconcileAssigned(context.Background(), "assigned", 1); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 1 || startOrder[0] != "fifo" {
		t.Fatalf("expected durable FIFO to consume shared capacity first, got %#v", startOrder)
	}
}

func TestCoordinatorReportsAssignedRunnerFailure(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", failingWorker{startErr: errors.New("start failed")})
	if _, err := coordinator.ReconcileAssigned(context.Background(), "repo", 1); err == nil {
		t.Fatal("expected assigned runner failure")
	}
}

func TestCoordinatorHandlesMissingAndFailingAssignedWorkers(t *testing.T) {
	coordinator := NewCoordinator(1)
	result, err := coordinator.ReconcileAssigned(context.Background(), "missing", 1)
	if err != nil || result.AssignedJobs != 1 {
		t.Fatalf("expected missing worker to preserve assigned statistic without failure, result=%#v err=%v", result, err)
	}
	coordinator.AddRepository("failing", failingWorker{runningErr: errors.New("count failed")})
	if _, err := coordinator.ReconcileAssigned(context.Background(), "failing", 1); err == nil {
		t.Fatal("expected assigned runner count failure")
	}
}

type failingDemandStore struct {
	demand              []QueuedDemand
	orderedErr          error
	applyErr            error
	deleteErr           error
	removeRepositoryErr error
}

func (store *failingDemandStore) Apply(context.Context, string, []JobDemand, []DemandRemoval) error {
	return store.applyErr
}
func (store *failingDemandStore) RemoveRepository(context.Context, string, string) error {
	return store.removeRepositoryErr
}
func (store *failingDemandStore) RemoveScaleSet(context.Context, string) error { return nil }
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
	assignedStarts    int
}

func (worker *recordingWorker) Acquire(_ context.Context, requestID int64) error {
	worker.acquired = append(worker.acquired, requestID)
	return nil
}

func (worker *recordingWorker) StartAssigned(context.Context) error {
	worker.assignedStarts++
	if worker.startOrder != nil {
		*worker.startOrder = append(*worker.startOrder, worker.repository)
	}
	worker.running++
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
func (worker failingWorker) StartAssigned(context.Context) error   { return worker.startErr }

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
