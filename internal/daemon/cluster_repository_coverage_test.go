package daemon

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestRepositoryCompletenessAuditReportsReadyAndMissingCoverage(t *testing.T) {
	logs := &bytes.Buffer{}
	leader, key := readyRepositoryCoverageLeader(logs)

	leader.auditDesiredRepositoryCompleteness(context.Background())
	assertRepositoryCoverageLog(t, logs.String(), "complete", `"ready":1`, `"currentEpochObserved":1`,
		`"pendingRepair":0`, `"missingScaleSets":[]`)

	logs.Reset()
	registration := leader.confirmedRegistrations[key]
	registration.GitHubScaleSetID = 99
	leader.confirmedRegistrations[key] = registration
	leader.auditDesiredRepositoryCompleteness(context.Background())
	assertRepositoryCoverageLog(t, logs.String(), "incomplete", `"ready":0`, `"currentEpochObserved":1`,
		`"pendingRepair":1`, `"missingScaleSets":["linux"]`)

	logs.Reset()
	leader.sessions = map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}
	leader.auditDesiredRepositoryCompleteness(context.Background())
	assertRepositoryCoverageLog(t, logs.String(), "incomplete", `"ready":0`, `"currentEpochObserved":0`,
		`"pendingRepair":1`, `"missingScaleSets":["linux"]`)
}

func readyRepositoryCoverageLeader(logs *bytes.Buffer) (*clusterLeader, repositoryScaleSetSessionKey) {
	repository := githubapp.Repository{URL: "repo", InstallationID: 7}
	status := cluster.ScaleSetStatus{Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 2}
	desired := desiredRepositoryGeneration{repository: repository, statuses: []cluster.ScaleSetStatus{status}, generation: 9}
	key := repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name}
	pollerDone := make(chan struct{})
	recovery := &clusterAssignedJobRecovery{controllerEpoch: 14}
	recovery.remember(&upstream.RunnerScaleSetStatistic{})
	leader := &clusterLeader{
		daemon: &clusteredDaemon{logger: newClusterLogSnapshotLogger(logs)},
		sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{
			key: {
				key: key, installationID: repository.InstallationID,
				capacity: status.EffectiveCapacity, runnerScaleSet: &upstream.RunnerScaleSet{ID: 42},
				assignedJobs: recovery, pollerDone: pollerDone, stopPolling: func() {},
			},
		},
		desiredRepositories: map[string]desiredRepositoryGeneration{repository.URL: desired},
		desiredStateKnown:   true, repositoryControllerEpoch: 14,
		repositoryControlStarted: true,
		confirmedRegistrations: map[repositoryScaleSetSessionKey]repositorystate.Registration{
			key: {
				RepositoryURL: repository.URL, ScaleSetName: status.Name,
				RunnerGroup: status.RunnerGroup, GitHubScaleSetID: 42,
			},
		},
		pendingRepositoryRepairs: map[string]repositoryRepair{},
		repositoryRepairDelays:   repositoryRepairRetryDelays[:], now: time.Now,
	}
	return leader, key
}

func assertRepositoryCoverageLog(t *testing.T, logs string, phase string, expected ...string) {
	t.Helper()
	fragments := append([]string{
		`"action":"repository_session_coverage"`, `"phase":"` + phase + `"`, `"repository":"repo"`,
		`"desired":1`, `"generation":9`, `"epoch":14`,
	}, expected...)
	for _, fragment := range fragments {
		if !strings.Contains(logs, fragment) {
			t.Fatalf("repository coverage log %q does not contain %q", logs, fragment)
		}
	}
}

func TestNilPollerIsNotRunning(t *testing.T) {
	if sessionPollerRunning(nil) {
		t.Fatal("nil poller reported running")
	}
}
