package daemon

import (
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

var runnerReapRetryDelays = [...]time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	60 * time.Second,
}

type runnerReapRetryKey struct {
	scaleSet   string
	repository string
	runnerName string
}

type runnerReapRetryEntry struct {
	attempt    int
	retryAfter time.Time
}

type runnerReapRetryTracker struct {
	mutex   sync.Mutex
	now     func() time.Time
	entries map[runnerReapRetryKey]runnerReapRetryEntry
}

func newRunnerReapRetryTracker(now func() time.Time) *runnerReapRetryTracker {
	return &runnerReapRetryTracker{now: now, entries: make(map[runnerReapRetryKey]runnerReapRetryEntry)}
}

func (tracker *runnerReapRetryTracker) ready(runner scaler.RunnerLifecycle) bool {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	entry, exists := tracker.entries[runnerReapRetryKeyFor(runner)]
	return !exists || !tracker.now().Before(entry.retryAfter)
}

func (tracker *runnerReapRetryTracker) deferRunner(runner scaler.RunnerLifecycle) time.Duration {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	key := runnerReapRetryKeyFor(runner)
	entry := tracker.entries[key]
	delayIndex := min(entry.attempt, len(runnerReapRetryDelays)-1)
	delay := runnerReapRetryDelays[delayIndex]
	entry.attempt++
	entry.retryAfter = tracker.now().Add(delay)
	tracker.entries[key] = entry
	return delay
}

func (tracker *runnerReapRetryTracker) clear(runner scaler.RunnerLifecycle) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	delete(tracker.entries, runnerReapRetryKeyFor(runner))
}

func (tracker *runnerReapRetryTracker) prune(pending []scaler.RunnerLifecycle) {
	retained := make(map[runnerReapRetryKey]struct{}, len(pending))
	for _, runner := range pending {
		retained[runnerReapRetryKeyFor(runner)] = struct{}{}
	}
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	for key := range tracker.entries {
		if _, exists := retained[key]; !exists {
			delete(tracker.entries, key)
		}
	}
}

func runnerReapRetryKeyFor(runner scaler.RunnerLifecycle) runnerReapRetryKey {
	return runnerReapRetryKey{
		scaleSet: runner.ScaleSet, repository: runner.Repository, runnerName: runner.RunnerName,
	}
}
