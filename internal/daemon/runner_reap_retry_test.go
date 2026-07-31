package daemon

import (
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestRunnerReapRetryBacksOffAndResetsPerRunner(t *testing.T) {
	now := time.Date(2026, time.July, 31, 12, 0, 0, 0, time.UTC)
	retries := newRunnerReapRetryTracker(func() time.Time { return now })
	runner := scaler.RunnerLifecycle{ScaleSet: "set", Repository: "repo", RunnerName: "runner-1"}

	for _, expectedDelay := range []time.Duration{
		5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 60 * time.Second, 60 * time.Second,
	} {
		if !retries.ready(runner) {
			t.Fatalf("expected retry to be ready before %s deferral", expectedDelay)
		}
		if delay := retries.deferRunner(runner); delay != expectedDelay {
			t.Fatalf("delay=%s expected=%s", delay, expectedDelay)
		}
		if retries.ready(runner) {
			t.Fatal("expected deferred runner to remain in backoff")
		}
		now = now.Add(expectedDelay)
	}

	retries.clear(runner)
	if delay := retries.deferRunner(runner); delay != 5*time.Second {
		t.Fatalf("reset delay=%s", delay)
	}
}

func TestRunnerReapRetryPrunesMissingLifecycle(t *testing.T) {
	retries := newRunnerReapRetryTracker(time.Now)
	present := scaler.RunnerLifecycle{ScaleSet: "set", Repository: "repo", RunnerName: "present"}
	missing := scaler.RunnerLifecycle{ScaleSet: "set", Repository: "repo", RunnerName: "missing"}
	retries.deferRunner(present)
	retries.deferRunner(missing)

	retries.prune([]scaler.RunnerLifecycle{present})
	if len(retries.entries) != 1 {
		t.Fatalf("entries=%d", len(retries.entries))
	}
	if _, exists := retries.entries[runnerReapRetryKeyFor(present)]; !exists {
		t.Fatal("expected present lifecycle retry to remain")
	}
}
