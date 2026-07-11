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
	coordinator.RemoveRepository("removed")
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
	coordinator.RemoveRepository("count-fails")
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

type recordingWorker struct {
	repository string
	running    int
	startOrder *[]string
	acquired   []int64
}

func (worker *recordingWorker) Acquire(_ context.Context, requestID int64) error {
	worker.acquired = append(worker.acquired, requestID)
	return nil
}

type failingWorker struct {
	runningErr error
	acquireErr error
	startErr   error
}

func (worker failingWorker) Running(context.Context) (int, error) { return 0, worker.runningErr }
func (worker failingWorker) Acquire(context.Context, int64) error { return worker.acquireErr }
func (worker failingWorker) StartOne(context.Context) error       { return worker.startErr }

func (worker *recordingWorker) Running(context.Context) (int, error) {
	return worker.running, nil
}

func (worker *recordingWorker) StartOne(context.Context) error {
	*worker.startOrder = append(*worker.startOrder, worker.repository)
	worker.running++
	return nil
}
