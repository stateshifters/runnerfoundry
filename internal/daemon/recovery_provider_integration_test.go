package daemon

import (
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/scaler"
)

func TestDaemonRestartRetainsProviderVisibleDemandWithoutDuplication(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	worker := &recordingRecoveryWorker{existing: map[int64]bool{3: true}}
	runRestartRecovery(t, []scaler.JobDemand{{
		RequestID: 3, Repository: repositoryURL, QueueTime: time.Now(), Acquired: true,
	}}, worker)
	if len(worker.acquired) != 0 || len(worker.started) != 0 {
		t.Fatalf("expected provider-visible request 3 to avoid duplication, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}
