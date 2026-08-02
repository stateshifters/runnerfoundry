package daemon

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRepositoryWorkerPoolReservesCapacityForUrgentRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newRepositoryWorkerPool(ctx, time.Second)
	t.Cleanup(func() {
		cancel()
		pool.wait()
	})

	routineStarted := make(chan string, 4)
	releaseRoutine := make(chan struct{})
	for index := 0; index < 4; index++ {
		repository := string(rune('a' + index))
		pool.enqueue(repository, repositoryWorkRoutine, func(context.Context) error {
			routineStarted <- repository
			<-releaseRoutine
			return nil
		}, nil)
	}
	for startedCount := 0; startedCount < 3; startedCount++ {
		awaitStringSignal(t, routineStarted)
	}
	assertNoRepositoryWorkSignal(t, routineStarted)

	urgentStarted := make(chan string, 1)
	pool.enqueue("urgent", repositoryWorkUrgent, func(context.Context) error {
		urgentStarted <- "urgent"
		return nil
	}, nil)
	awaitStringSignal(t, urgentStarted)
	close(releaseRoutine)
}

func TestRepositoryWorkerPoolSerializesWorkForOneRepository(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newRepositoryWorkerPool(ctx, time.Second)
	t.Cleanup(func() {
		cancel()
		pool.wait()
	})

	probe := newRepositorySerializationProbe()
	pool.enqueue("https://github.com/acme/repository", repositoryWorkUrgent, probe.runFirst, nil)
	awaitStringSignal(t, probe.firstStarted)
	pool.enqueue("https://github.com/acme/repository", repositoryWorkUrgent, probe.runSecond, nil)
	assertNoRepositoryWorkSignal(t, probe.secondStarted)
	close(probe.releaseFirst)
	awaitStringSignal(t, probe.secondStarted)
	if probe.maximum.Load() != 1 {
		t.Fatalf("maximum concurrent work = %d, want 1", probe.maximum.Load())
	}
}

type repositorySerializationProbe struct {
	firstStarted  chan string
	releaseFirst  chan struct{}
	secondStarted chan string
	active        atomic.Int32
	maximum       atomic.Int32
}

func newRepositorySerializationProbe() *repositorySerializationProbe {
	return &repositorySerializationProbe{
		firstStarted: make(chan string, 1), releaseFirst: make(chan struct{}), secondStarted: make(chan string, 1),
	}
}

func (probe *repositorySerializationProbe) runFirst(context.Context) error {
	probe.enter()
	probe.firstStarted <- "first"
	<-probe.releaseFirst
	probe.active.Add(-1)
	return nil
}

func (probe *repositorySerializationProbe) runSecond(context.Context) error {
	probe.enter()
	probe.secondStarted <- "second"
	probe.active.Add(-1)
	return nil
}

func (probe *repositorySerializationProbe) enter() {
	current := probe.active.Add(1)
	probe.maximum.Store(max(probe.maximum.Load(), current))
}

func TestRepositoryWorkerPoolGivesEachAttemptAnIndependentTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newRepositoryWorkerPool(ctx, 10*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		pool.wait()
	})

	firstResult := make(chan error, 1)
	secondContextState := make(chan error, 1)
	pool.enqueue("first", repositoryWorkUrgent, func(attemptCtx context.Context) error {
		<-attemptCtx.Done()
		return attemptCtx.Err()
	}, func(err error) { firstResult <- err })
	pool.enqueue("second", repositoryWorkUrgent, func(attemptCtx context.Context) error {
		secondContextState <- attemptCtx.Err()
		return nil
	}, nil)
	if err := awaitErrorSignal(t, firstResult); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first result = %v, want deadline exceeded", err)
	}
	if err := awaitErrorSignal(t, secondContextState); err != nil {
		t.Fatalf("second context inherited error %v", err)
	}
}

func TestRepositoryWorkerPoolCompletesEveryCoalescedRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newRepositoryWorkerPool(ctx, time.Second)
	t.Cleanup(func() {
		cancel()
		pool.wait()
	})

	firstStarted := make(chan string, 1)
	releaseFirst := make(chan struct{})
	pool.enqueue("repository", repositoryWorkRoutine, func(context.Context) error {
		firstStarted <- "first"
		<-releaseFirst
		return nil
	}, nil)
	awaitStringSignal(t, firstStarted)
	completed := make(chan string, 2)
	pool.enqueue("repository", repositoryWorkRoutine, func(context.Context) error { return nil }, func(error) {
		completed <- "routine"
	})
	pool.enqueue("repository", repositoryWorkUrgent, func(context.Context) error { return nil }, func(error) {
		completed <- "urgent"
	})
	close(releaseFirst)
	firstCompletion := awaitStringSignal(t, completed)
	secondCompletion := awaitStringSignal(t, completed)
	if firstCompletion == secondCompletion {
		t.Fatalf("coalesced completions = %q and %q", firstCompletion, secondCompletion)
	}
}

func TestRepositoryWorkerPoolKeepsUrgentPriorityWhenRoutineWorkIsCoalesced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newRepositoryWorkerPool(ctx, time.Second)
	t.Cleanup(func() {
		cancel()
		pool.wait()
	})

	started := make(chan struct{})
	release := make(chan struct{})
	pool.enqueue("repository", repositoryWorkRoutine, func(context.Context) error {
		close(started)
		<-release
		return nil
	}, nil)
	awaitCompletionSignal(t, started)
	pool.enqueue("repository", repositoryWorkUrgent, func(context.Context) error { return nil }, nil)
	pool.enqueue("repository", repositoryWorkRoutine, func(context.Context) error { return nil }, nil)

	pool.mutex.Lock()
	priority := pool.pending["repository"].priority
	pool.mutex.Unlock()
	if priority != repositoryWorkUrgent {
		t.Fatalf("coalesced priority = %d, want urgent", priority)
	}
	close(release)
}

func awaitStringSignal(t *testing.T, signals <-chan string) string {
	t.Helper()
	select {
	case value := <-signals:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for repository work")
		return ""
	}
}

func awaitErrorSignal(t *testing.T, signals <-chan error) error {
	t.Helper()
	select {
	case value := <-signals:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for repository work")
		return nil
	}
}

func awaitBoolSignal(t *testing.T, signals <-chan bool) bool {
	t.Helper()
	select {
	case value := <-signals:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for repository work")
		return false
	}
}

func awaitCompletionSignal(t *testing.T, signals <-chan struct{}) {
	t.Helper()
	select {
	case <-signals:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for repository work")
	}
}

func assertNoRepositoryWorkSignal[Value any](t *testing.T, signals <-chan Value) {
	t.Helper()
	select {
	case value := <-signals:
		t.Fatalf("unexpected repository work signal %#v", value)
	case <-time.After(20 * time.Millisecond):
	}
}
