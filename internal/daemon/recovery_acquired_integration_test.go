//go:build integration

package daemon

import (
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestDaemonRestartProvisionsPersistedAcquiredDemandWithoutReacquiring(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	worker := &recordingRecoveryWorker{}
	runRestartRecovery(t, []scaler.JobDemand{{
		RequestID: 2, Repository: repositoryURL, QueueTime: time.Now(), Acquired: true,
	}}, worker)
	if len(worker.acquired) != 0 || len(worker.started) != 1 || worker.started[0] != 2 {
		t.Fatalf("expected request 2 provisioning without acquisition, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}
