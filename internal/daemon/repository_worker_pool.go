package daemon

import (
	"context"
	"sync"
	"time"
)

const (
	repositoryWorkerCount        = 4
	routineRepositoryWorkerCount = repositoryWorkerCount - 1
)

type repositoryWorkPriority int

const (
	repositoryWorkUrgent repositoryWorkPriority = iota
	repositoryWorkRoutine
)

type repositoryWorkerCapability int

const (
	repositoryWorkerUrgentOnly repositoryWorkerCapability = iota
	repositoryWorkerRoutineCapable
)

type repositoryWorkItem struct {
	repository string
	priority   repositoryWorkPriority
	execute    func(context.Context) error
	completed  []func(error)
}

type repositoryWorkerPool struct {
	attemptTimeout time.Duration
	urgentNotify   chan struct{}
	generalNotify  chan struct{}
	mutex          sync.Mutex
	pending        map[string]repositoryWorkItem
	running        map[string]struct{}
	workers        sync.WaitGroup
}

func newRepositoryWorkerPool(ctx context.Context, attemptTimeout time.Duration) *repositoryWorkerPool {
	pool := &repositoryWorkerPool{
		attemptTimeout: attemptTimeout,
		urgentNotify:   make(chan struct{}, 1), generalNotify: make(chan struct{}, routineRepositoryWorkerCount),
		pending: map[string]repositoryWorkItem{}, running: map[string]struct{}{},
	}
	pool.workers.Add(repositoryWorkerCount)
	for workerIndex := 0; workerIndex < routineRepositoryWorkerCount; workerIndex++ {
		go pool.runWorker(ctx, repositoryWorkerRoutineCapable)
	}
	go pool.runWorker(ctx, repositoryWorkerUrgentOnly)
	return pool
}

func (pool *repositoryWorkerPool) enqueue(
	repository string,
	priority repositoryWorkPriority,
	execute func(context.Context) error,
	completed func(error),
) {
	pool.mutex.Lock()
	existing, pending := pool.pending[repository]
	if pending && existing.priority < priority {
		priority = existing.priority
	}
	callbacks := existing.completed
	if completed != nil {
		callbacks = append(callbacks, completed)
	}
	pool.pending[repository] = repositoryWorkItem{
		repository: repository, priority: priority, execute: execute, completed: callbacks,
	}
	pool.mutex.Unlock()
	pool.signal(priority)
}

func (pool *repositoryWorkerPool) wait() {
	pool.workers.Wait()
}

func (pool *repositoryWorkerPool) runWorker(ctx context.Context, capability repositoryWorkerCapability) {
	defer pool.workers.Done()
	for {
		work, available := pool.next(ctx, capability)
		if !available {
			return
		}
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, pool.attemptTimeout)
		err := work.execute(attemptCtx)
		cancelAttempt()
		for _, completed := range work.completed {
			completed(err)
		}
		pool.complete(work.repository)
	}
}

func (pool *repositoryWorkerPool) next(
	ctx context.Context,
	capability repositoryWorkerCapability,
) (repositoryWorkItem, bool) {
	for {
		work, available, stopped := pool.tryNext(ctx, capability)
		if stopped {
			return repositoryWorkItem{}, false
		}
		if available {
			return work, true
		}
		if !pool.waitForWork(ctx, capability) {
			return repositoryWorkItem{}, false
		}
	}
}

func (pool *repositoryWorkerPool) tryNext(
	ctx context.Context,
	capability repositoryWorkerCapability,
) (work repositoryWorkItem, available bool, stopped bool) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if ctx.Err() != nil {
		return repositoryWorkItem{}, false, true
	}
	work, available = pool.dequeue(repositoryWorkUrgent)
	if !available && capability == repositoryWorkerRoutineCapable {
		work, available = pool.dequeue(repositoryWorkRoutine)
	}
	return work, available, false
}

func (pool *repositoryWorkerPool) waitForWork(
	ctx context.Context,
	capability repositoryWorkerCapability,
) bool {
	notify := pool.urgentNotify
	if capability == repositoryWorkerRoutineCapable {
		notify = pool.generalNotify
	}
	select {
	case <-ctx.Done():
		return false
	case <-notify:
		return true
	}
}

func (pool *repositoryWorkerPool) dequeue(priority repositoryWorkPriority) (repositoryWorkItem, bool) {
	for repository, work := range pool.pending {
		if work.priority != priority {
			continue
		}
		if _, running := pool.running[repository]; running {
			continue
		}
		delete(pool.pending, repository)
		pool.running[repository] = struct{}{}
		return work, true
	}
	return repositoryWorkItem{}, false
}

func (pool *repositoryWorkerPool) complete(repository string) {
	pool.mutex.Lock()
	delete(pool.running, repository)
	pendingPriorities := pool.pendingPriorities()
	pool.mutex.Unlock()
	for _, priority := range pendingPriorities {
		pool.signal(priority)
	}
}

func (pool *repositoryWorkerPool) pendingPriorities() []repositoryWorkPriority {
	priorities := make([]repositoryWorkPriority, 0, len(pool.pending))
	for _, work := range pool.pending {
		priorities = append(priorities, work.priority)
	}
	return priorities
}

func (pool *repositoryWorkerPool) signal(priority repositoryWorkPriority) {
	if priority == repositoryWorkUrgent {
		select {
		case pool.urgentNotify <- struct{}{}:
		default:
		}
	}
	select {
	case pool.generalNotify <- struct{}{}:
	default:
	}
}
