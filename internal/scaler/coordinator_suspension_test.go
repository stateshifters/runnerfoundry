package scaler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCoordinatorRetainsDemandWhileProvisioningIsSuspended(t *testing.T) {
	startOrder := []string{}
	worker := &recordingWorker{repository: "repo", startOrder: &startOrder}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.SuspendProvisioning(context.Background()); err != nil {
		t.Fatal(err)
	}
	applyCoordinatorDemand(t, coordinator, []JobDemand{{
		RequestID: 42, Repository: "repo", QueueTime: time.Now(),
	}}, nil)

	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("schedule suspended demand: %v", err)
	}
	if len(startOrder) != 0 {
		t.Fatalf("suspended provisioning started runners: %#v", startOrder)
	}
	coordinator.ResumeProvisioning()
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("schedule resumed provisioning: %v", err)
	}
	if len(startOrder) != 1 || startOrder[0] != "repo" {
		t.Fatalf("resumed provisioning did not start retained demand: %#v", startOrder)
	}
}

func TestCoordinatorRetainsAssignedObservationWhileProvisioningIsSuspended(t *testing.T) {
	startOrder := []string{}
	worker := &recordingWorker{repository: "repo", startOrder: &startOrder}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.SuspendProvisioning(context.Background()); err != nil {
		t.Fatal(err)
	}

	result, err := coordinator.ReconcileAssigned(context.Background(), "repo", 1)
	if err != nil {
		t.Fatalf("observe assigned jobs while suspended: %v", err)
	}
	if result.AssignedJobs != 1 || worker.assignedStarts != 0 {
		t.Fatalf("unexpected suspended reconciliation: result=%#v starts=%d", result, worker.assignedStarts)
	}
	coordinator.ResumeProvisioning()
	if err = coordinator.Schedule(context.Background()); err != nil {
		t.Fatalf("schedule resumed provisioning: %v", err)
	}
	if worker.assignedStarts != 1 {
		t.Fatalf("resumed provisioning started %d assigned runners, want 1", worker.assignedStarts)
	}
}

func TestCoordinatorSuspensionHonorsContextWhileSchedulingIsInFlight(t *testing.T) {
	worker := &blockingRunningWorker{entered: make(chan struct{}), release: make(chan struct{})}
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	scheduleDone := make(chan error, 1)
	go func() { scheduleDone <- coordinator.Schedule(context.Background()) }()
	<-worker.entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := coordinator.SuspendProvisioning(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("suspension error = %v, want context cancellation", err)
	}
	close(worker.release)
	if err := <-scheduleDone; err != nil {
		t.Fatalf("in-flight schedule: %v", err)
	}
}

func TestCoordinatorScheduleHonorsContextWhileProvisioningBoundaryIsOccupied(t *testing.T) {
	coordinator := NewCoordinator(1)
	if err := coordinator.acquireProvisioning(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer coordinator.releaseProvisioning()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := coordinator.Schedule(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("schedule error = %v, want context cancellation", err)
	}
}

func TestCoordinatorProvisioningBoundaryContinuesAfterInFlightPass(t *testing.T) {
	coordinator := NewCoordinator(1)
	if err := coordinator.acquireProvisioning(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := &observedContext{observed: make(chan struct{})}
	acquired := make(chan error, 1)
	go func() { acquired <- coordinator.acquireProvisioning(ctx) }()
	<-ctx.observed
	coordinator.releaseProvisioning()

	if err := <-acquired; err != nil {
		t.Fatalf("acquire released provisioning boundary: %v", err)
	}
	coordinator.releaseProvisioning()
}

type observedContext struct {
	observed chan struct{}
	once     sync.Once
}

func (*observedContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (ctx *observedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return nil
}

func (*observedContext) Err() error    { return nil }
func (*observedContext) Value(any) any { return nil }

type blockingRunningWorker struct {
	entered chan struct{}
	release chan struct{}
}

func (worker *blockingRunningWorker) Running(context.Context) (int, error) {
	close(worker.entered)
	<-worker.release
	return 0, nil
}

func (*blockingRunningWorker) Exists(context.Context, int64) (bool, error) { return false, nil }
func (*blockingRunningWorker) Acquire(context.Context, int64) error        { return nil }
func (*blockingRunningWorker) StartOne(context.Context, int64) error       { return nil }
func (*blockingRunningWorker) StartAssigned(context.Context) error         { return nil }
