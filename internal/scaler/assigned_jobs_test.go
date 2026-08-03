package scaler

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestAssignedJobControllerStartsDeficitWithinAvailableCapacity(t *testing.T) {
	backend := &recordingAssignedJobBackend{preparation: AssignedJobCapacity{
		ActiveRunners: 1, AvailableCapacity: 1,
	}}
	controller := NewAssignedJobController(backend)

	result, err := controller.Reconcile(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if result != (AssignedReconciliation{AssignedJobs: 3, RunningRunners: 1, StartedRunners: 1}) {
		t.Fatalf("reconciliation = %#v", result)
	}
	if backend.startCalls != 1 {
		t.Fatalf("start calls = %d, want 1", backend.startCalls)
	}
}

func TestAssignedJobControllerStopsWhenBackendCapacityChanges(t *testing.T) {
	backend := &recordingAssignedJobBackend{
		preparation:  AssignedJobCapacity{AvailableCapacity: 3},
		startResults: []bool{true, false, true},
	}
	controller := NewAssignedJobController(backend)

	result, err := controller.Reconcile(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if result.StartedRunners != 1 || backend.startCalls != 2 {
		t.Fatalf("reconciliation = %#v, start calls = %d", result, backend.startCalls)
	}
}

func TestAssignedJobControllerDoesNotStartSatisfiedAssignment(t *testing.T) {
	backend := &recordingAssignedJobBackend{preparation: AssignedJobCapacity{
		ActiveRunners: 2, AvailableCapacity: 3,
	}}
	controller := NewAssignedJobController(backend)

	result, err := controller.Reconcile(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.StartedRunners != 0 || backend.startCalls != 0 {
		t.Fatalf("reconciliation = %#v, start calls = %d", result, backend.startCalls)
	}
}

func TestAssignedJobControllerReportsPreparationFailure(t *testing.T) {
	expectedErr := errors.New("prepare failed")
	controller := NewAssignedJobController(&recordingAssignedJobBackend{prepareErr: expectedErr})

	if _, err := controller.Reconcile(context.Background(), 1); !errors.Is(err, expectedErr) {
		t.Fatalf("reconcile error = %v, want %v", err, expectedErr)
	}
}

func TestAssignedJobControllerReportsStartFailure(t *testing.T) {
	expectedErr := errors.New("start failed")
	controller := NewAssignedJobController(&recordingAssignedJobBackend{
		preparation: AssignedJobCapacity{AvailableCapacity: 1}, startErr: expectedErr,
	})

	if _, err := controller.Reconcile(context.Background(), 1); !errors.Is(err, expectedErr) {
		t.Fatalf("reconcile error = %v, want %v", err, expectedErr)
	}
}

func TestAssignedJobControllerSerializesReconciliations(t *testing.T) {
	backend := newBlockingAssignedJobBackend()
	controller := NewAssignedJobController(backend)
	reconciliationErrors := make(chan error, 2)
	go reconcileAssignedInBackground(controller, reconciliationErrors)
	if call := <-backend.entered; call != 1 {
		t.Fatalf("first backend call = %d", call)
	}
	secondInvoked := make(chan struct{})
	go func() {
		close(secondInvoked)
		reconcileAssignedInBackground(controller, reconciliationErrors)
	}()
	<-secondInvoked
	close(backend.releaseFirst)
	assertAssignedReconciliationSucceeded(t, reconciliationErrors)
	if call := <-backend.entered; call != 2 {
		t.Fatalf("second backend call = %d", call)
	}
	close(backend.releaseSecond)
	assertAssignedReconciliationSucceeded(t, reconciliationErrors)
	if backend.maximumConcurrency() != 1 {
		t.Fatalf("maximum backend concurrency = %d, want 1", backend.maximumConcurrency())
	}
}

func reconcileAssignedInBackground(
	controller *AssignedJobController,
	errors chan<- error,
) {
	_, err := controller.Reconcile(context.Background(), 0)
	errors <- err
}

func assertAssignedReconciliationSucceeded(t *testing.T, reconciliationErrors <-chan error) {
	t.Helper()
	if err := <-reconciliationErrors; err != nil {
		t.Fatalf("background reconciliation: %v", err)
	}
}

type blockingAssignedJobBackend struct {
	entered       chan int
	releaseFirst  chan struct{}
	releaseSecond chan struct{}
	mutex         sync.Mutex
	calls         int
	active        int
	maximumActive int
}

func newBlockingAssignedJobBackend() *blockingAssignedJobBackend {
	return &blockingAssignedJobBackend{
		entered: make(chan int), releaseFirst: make(chan struct{}), releaseSecond: make(chan struct{}),
	}
}

func (backend *blockingAssignedJobBackend) Prepare(context.Context, int) (AssignedJobCapacity, error) {
	backend.mutex.Lock()
	backend.calls++
	call := backend.calls
	backend.active++
	if backend.active > backend.maximumActive {
		backend.maximumActive = backend.active
	}
	backend.mutex.Unlock()
	backend.entered <- call
	if call == 1 {
		<-backend.releaseFirst
	} else {
		<-backend.releaseSecond
	}
	backend.mutex.Lock()
	backend.active--
	backend.mutex.Unlock()
	return AssignedJobCapacity{}, nil
}

func (backend *blockingAssignedJobBackend) maximumConcurrency() int {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	return backend.maximumActive
}

func (*blockingAssignedJobBackend) StartAssigned(context.Context) (bool, error) {
	return false, nil
}

type recordingAssignedJobBackend struct {
	preparation  AssignedJobCapacity
	prepareErr   error
	startCalls   int
	startResults []bool
	startErr     error
}

func (backend *recordingAssignedJobBackend) Prepare(context.Context, int) (AssignedJobCapacity, error) {
	return backend.preparation, backend.prepareErr
}

func (backend *recordingAssignedJobBackend) StartAssigned(context.Context) (bool, error) {
	result := true
	if backend.startCalls < len(backend.startResults) {
		result = backend.startResults[backend.startCalls]
	}
	backend.startCalls++
	return result, backend.startErr
}
