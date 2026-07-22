//go:build integration

package daemon

import (
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/scaler"
)

func TestDaemonRestartAcquiresAndProvisionsPersistedAvailableDemand(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	worker := &recordingRecoveryWorker{}
	runRestartRecovery(t, []scaler.JobDemand{{
		RequestID: 1, Repository: repositoryURL, QueueTime: time.Now(),
	}}, worker)
	if len(worker.acquired) != 1 || worker.acquired[0] != 1 || len(worker.started) != 1 || worker.started[0] != 1 {
		t.Fatalf("expected request 1 acquisition and provisioning, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}
