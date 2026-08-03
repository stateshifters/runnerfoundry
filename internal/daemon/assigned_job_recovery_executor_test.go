package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type assignedRecoveryConcurrencyTracker struct {
	mutex     sync.Mutex
	active    int
	maximum   int
	entered   chan struct{}
	completed chan struct{}
	release   <-chan struct{}
}

func (tracker *assignedRecoveryConcurrencyTracker) prepare(ctx context.Context) error {
	tracker.mutex.Lock()
	tracker.active++
	tracker.maximum = max(tracker.maximum, tracker.active)
	tracker.mutex.Unlock()
	tracker.entered <- struct{}{}
	select {
	case <-ctx.Done():
	case <-tracker.release:
	}
	tracker.mutex.Lock()
	tracker.active--
	tracker.mutex.Unlock()
	tracker.completed <- struct{}{}
	return fmt.Errorf("track assigned recovery: %w", ctx.Err())
}

func (tracker *assignedRecoveryConcurrencyTracker) maximumActive() int {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	return tracker.maximum
}

type trackedAssignedJobBackend struct {
	tracker *assignedRecoveryConcurrencyTracker
}

func (backend trackedAssignedJobBackend) Prepare(ctx context.Context, _ int) (scaler.AssignedJobCapacity, error) {
	return scaler.AssignedJobCapacity{}, backend.tracker.prepare(ctx)
}

func (trackedAssignedJobBackend) StartAssigned(context.Context) (bool, error) { return false, nil }

func TestAssignedRecoveryExecutorBoundsIndependentScaleSets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	tracker := &assignedRecoveryConcurrencyTracker{
		entered: make(chan struct{}, 5), completed: make(chan struct{}, 5), release: release,
	}
	executor := newAssignedJobRecoveryExecutor(ctx, time.Second)
	for index := 0; index < 5; index++ {
		recovery := recoveryForExecutor(executor, trackedAssignedJobBackend{tracker: tracker}, index)
		recovery.remember(&upstream.RunnerScaleSetStatistic{})
		recovery.start(ctx)
	}
	for index := 0; index < assignedJobRecoveryWorkerCount; index++ {
		awaitSignal(t, tracker.entered, "assigned recovery did not enter worker")
	}
	select {
	case <-tracker.entered:
		t.Fatal("assigned recovery exceeded the four-worker limit")
	default:
	}
	executor.retryFailed(ctx)
	close(release)
	for index := 0; index < 5; index++ {
		awaitSignal(t, tracker.completed, "assigned recovery did not complete")
	}
	if maximum := tracker.maximumActive(); maximum != assignedJobRecoveryWorkerCount {
		t.Fatalf("maximum assigned recovery concurrency = %d", maximum)
	}
	executor.stop()
}

type coalescingAssignedJobBackend struct {
	mutex    sync.Mutex
	assigned []int
	entered  chan struct{}
	release  <-chan struct{}
}

func (backend *coalescingAssignedJobBackend) Prepare(ctx context.Context, assigned int) (scaler.AssignedJobCapacity, error) {
	backend.mutex.Lock()
	backend.assigned = append(backend.assigned, assigned)
	call := len(backend.assigned)
	backend.mutex.Unlock()
	backend.entered <- struct{}{}
	if call == 1 {
		select {
		case <-ctx.Done():
			return scaler.AssignedJobCapacity{}, fmt.Errorf("coalesce assigned recovery: %w", ctx.Err())
		case <-backend.release:
		}
	}
	return scaler.AssignedJobCapacity{}, nil
}

func (*coalescingAssignedJobBackend) StartAssigned(context.Context) (bool, error) { return false, nil }

func (backend *coalescingAssignedJobBackend) observations() []int {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	return append([]int(nil), backend.assigned...)
}

func TestAssignedRecoveryExecutorCoalescesLatestStatisticsPerScaleSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	backend := &coalescingAssignedJobBackend{entered: make(chan struct{}, 2), release: release}
	executor := newAssignedJobRecoveryExecutor(ctx, time.Second)
	recovery := recoveryForExecutor(executor, backend, 1)
	recovery.remember(&upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1})
	recovery.start(ctx)
	awaitSignal(t, backend.entered, "initial assigned recovery did not start")
	observeAssignedJobs(ctx, t, recovery, 2)
	observeAssignedJobs(ctx, t, recovery, 3)
	close(release)
	awaitSignal(t, backend.entered, "coalesced assigned recovery did not start")
	awaitCoalescedAssignedObservations(t, backend)
	executor.stop()
}

func observeAssignedJobs(
	ctx context.Context,
	t *testing.T,
	recovery *clusterAssignedJobRecovery,
	assignedJobs int,
) {
	t.Helper()
	if err := recovery.observe(ctx, &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: assignedJobs}, "message"); err != nil {
		t.Fatal(err)
	}
}

func awaitCoalescedAssignedObservations(t *testing.T, backend *coalescingAssignedJobBackend) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		observations := backend.observations()
		if len(observations) == 2 {
			if observations[0] != 1 || observations[1] != 3 {
				t.Fatalf("assigned observations = %#v", observations)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("assigned observations = %#v", observations)
		case <-time.After(time.Millisecond):
		}
	}
}

type retryAssignedJobBackend struct {
	mutex sync.Mutex
	calls int
}

func (backend *retryAssignedJobBackend) Prepare(context.Context, int) (scaler.AssignedJobCapacity, error) {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	backend.calls++
	if backend.calls == 1 {
		return scaler.AssignedJobCapacity{}, errors.New("unavailable")
	}
	return scaler.AssignedJobCapacity{}, nil
}

func (*retryAssignedJobBackend) StartAssigned(context.Context) (bool, error) { return false, nil }

func (backend *retryAssignedJobBackend) callCount() int {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	return backend.calls
}

func TestAssignedRecoverySafetyWakeRetriesOnlyFailedScaleSets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	safetyTicks := make(chan time.Time)
	executor := newAssignedJobRecoveryExecutorWithSafetyTicks(ctx, time.Second, safetyTicks, func() {})
	failedBackend := &retryAssignedJobBackend{}
	failed := recoveryForExecutor(executor, failedBackend, 1)
	failed.remember(&upstream.RunnerScaleSetStatistic{})
	failed.start(ctx)
	awaitBackendCalls(t, failedBackend, 1)
	healthyBackend := &retryAssignedJobBackend{calls: 1}
	healthy := recoveryForExecutor(executor, healthyBackend, 2)
	healthy.remember(&upstream.RunnerScaleSetStatistic{})
	healthy.start(ctx)
	awaitBackendCalls(t, healthyBackend, 2)
	deadline := time.After(time.Second)
	for failedBackend.callCount() < 2 {
		safetyTicks <- time.Now()
		select {
		case <-deadline:
			t.Fatalf("failed recovery calls = %d, want 2", failedBackend.callCount())
		case <-time.After(time.Millisecond):
		}
	}
	if calls := healthyBackend.callCount(); calls != 2 {
		t.Fatalf("healthy recovery calls = %d, want 2", calls)
	}
	executor.stop()
}

func TestAssignedRecoveryExecutorLogsTriggerSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := &synchronizedLogBuffer{}
	executor := newAssignedJobRecoveryExecutor(ctx, time.Second)
	recovery := recoveryForExecutor(executor, inertAssignedJobBackend{}, 1)
	recovery.logger = slog.New(slog.NewJSONHandler(logs, nil))
	recovery.controllerEpoch = 7
	recovery.remember(&upstream.RunnerScaleSetStatistic{})
	recovery.start(ctx)
	deadline := time.After(time.Second)
	for !strings.Contains(logs.String(), `"action":"assigned_recovery_trigger"`) {
		select {
		case <-deadline:
			t.Fatalf("assigned recovery trigger log = %s", logs.String())
		case <-time.After(time.Millisecond):
		}
	}
	output := logs.String()
	if !strings.Contains(output, `"trigger":"session"`) || !strings.Contains(output, `"epoch":7`) {
		t.Fatalf("assigned recovery trigger context = %s", output)
	}
	executor.stop()
}

func TestAssignedRecoveryStopsWhenItsSessionIsCanceled(t *testing.T) {
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	defer stopExecutor()
	sessionCtx, stopSession := context.WithCancel(context.Background())
	tracker := &assignedRecoveryConcurrencyTracker{
		entered: make(chan struct{}, 1), completed: make(chan struct{}, 1), release: make(chan struct{}),
	}
	executor := newAssignedJobRecoveryExecutor(executorCtx, time.Second)
	recovery := recoveryForExecutor(executor, trackedAssignedJobBackend{tracker: tracker}, 1)
	recovery.remember(&upstream.RunnerScaleSetStatistic{})
	recovery.start(sessionCtx)
	awaitSignal(t, tracker.entered, "session recovery did not start")
	stopSession()
	recovery.stop()
	select {
	case <-tracker.completed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("session cancellation did not cancel its assigned recovery attempt")
	}
	executor.stop()
}

func recoveryForExecutor(
	executor *assignedJobRecoveryExecutor,
	backend scaler.AssignedJobBackend,
	index int,
) *clusterAssignedJobRecovery {
	return &clusterAssignedJobRecovery{
		controller: scaler.NewAssignedJobController(backend), repository: fmt.Sprintf("repo-%d", index),
		scaleSet: "linux", executor: executor,
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

func awaitBackendCalls(t *testing.T, backend *retryAssignedJobBackend, expected int) {
	t.Helper()
	deadline := time.After(time.Second)
	for backend.callCount() < expected {
		select {
		case <-deadline:
			t.Fatalf("backend calls = %d, want %d", backend.callCount(), expected)
		case <-time.After(time.Millisecond):
		}
	}
}
