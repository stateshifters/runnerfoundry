package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestUnexpectedRepositoryPollerTerminationQueuesCurrentGenerationRepair(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	repository := githubapp.Repository{InstallationID: 1, URL: "https://github.com/acme/poller"}
	status := cluster.ScaleSetStatus{Name: "linux", EffectiveCapacity: 1}
	key := repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name}
	leader.repositoryControllerEpoch = 7
	leader.publishDesiredRepositories(
		map[string]githubapp.Repository{repository.URL: repository},
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{key: status},
	)
	client := &fakeMessageClient{getMessageErr: fmt.Errorf("fenced: %w", scaler.ErrNotLeader)}
	session := stoppedSession(key, fakeScaleSetDeletionClient{}, client)
	session.capacity = status.EffectiveCapacity
	session.runnerScaleSet = &upstream.RunnerScaleSet{ID: 11}
	session.assignedJobs = &clusterAssignedJobRecovery{controllerEpoch: leader.repositoryControllerEpoch}
	leader.sessions[key] = session
	leader.scheduler.add(session)
	session.pollerDone = leader.startSessionPoller(context.Background(), session)

	awaitRepositoryRepairCount(t, leader)
	if leader.sessions[key] != nil || leader.scheduler.session(key) != nil {
		t.Fatal("unexpectedly stopped poller retained an active session")
	}
	if !client.closed {
		t.Fatal("unexpectedly stopped poller did not close its message session")
	}
	leader.stop(context.Background())
}

func TestRepositoryRepairStopsWhenCallerIsCanceled(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, "https://github.com/acme/caller-canceled")
	entered := make(chan struct{})
	daemon.newGitHubClient = blockingRepositoryClientFactory(entered)
	repairCtx, cancelRepair := context.WithCancel(context.Background())
	cancelRepair()
	leader.repairPendingRepositories(repairCtx)
	leader.stop(context.Background())
}

func TestRepositoryRepairStopsWhenLeadershipContextIsCanceled(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, "https://github.com/acme/leadership-canceled")
	termCtx, cancelTerm := context.WithCancel(context.Background())
	cancelTerm()
	leader.stopRepositoryWorkers()
	leader.repairPendingRepositories(termCtx)
	leader.stop(context.Background())
}

func TestObsoleteRepositoryRepairRemainsPendingWithoutCompleteSessionGeneration(t *testing.T) {
	repositoryURL := "https://github.com/acme/obsolete"
	leader, daemon := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, repositoryURL)
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errRepositoryWorkObsolete
	}
	leader.repairPendingRepositories(context.Background())
	leader.repairMutex.Lock()
	_, pending := leader.pendingRepositoryRepairs[repositoryURL]
	leader.repairMutex.Unlock()
	if !pending {
		t.Fatal("obsolete repository repair cleared without a complete current session generation")
	}
	leader.stop(context.Background())
}

func TestOlderRepositoryResultCannotCompleteCurrentGenerationRepair(t *testing.T) {
	repositoryURL := "https://github.com/acme/generation"
	leader, _ := repositoryRepairTestLeader(t, nil)
	leader.pendingRepositoryRepairs[repositoryURL] = repositoryRepair{
		repository: githubapp.Repository{InstallationID: 2, URL: repositoryURL},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 2}},
		generation: 2,
	}

	leader.applyRepositoryRepairResult(context.Background(), repositoryRepairResult{repair: repositoryRepair{
		repository: githubapp.Repository{InstallationID: 1, URL: repositoryURL},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 1}},
		generation: 1,
	}})

	if repair := leader.pendingRepositoryRepairs[repositoryURL]; repair.generation != 2 {
		t.Fatalf("current repair was replaced by stale completion: %#v", repair)
	}
	leader.stop(context.Background())
}

func TestObsoleteGenerationSupersedesRepairWithoutConsumingBackoff(t *testing.T) {
	logs := &bytes.Buffer{}
	now := time.Unix(100, 0).UTC()
	leader, old, current := obsoleteGenerationFixture(t, logs, now)

	leader.finishRepositoryReconciliation(context.Background(), old, nil)

	repair := leader.pendingRepositoryRepairs[current.repository.URL]
	if repair.generation != current.generation || repair.repository != current.repository ||
		repair.attempts != 3 || !repair.retryAfter.Equal(now) {
		t.Fatalf("superseded repair = %#v, want current generation with immediate unchanged attempt", repair)
	}
	if output := logs.String(); !strings.Contains(output, `"phase":"superseded"`) ||
		strings.Contains(output, `"phase":"completed"`) {
		t.Fatalf("superseded repair log = %q", output)
	}
	leader.stop(context.Background())
}

func obsoleteGenerationFixture(
	t *testing.T,
	logs *bytes.Buffer,
	now time.Time,
) (leader *clusterLeader, old desiredRepositoryGeneration, current desiredRepositoryGeneration) {
	t.Helper()
	repositoryURL := "https://github.com/acme/generation"
	leader, _ = repositoryRepairTestLeader(t, nil)
	leader.daemon.logger = slog.New(slog.NewJSONHandler(logs, nil))
	leader.now = func() time.Time { return now }
	old = desiredRepositoryGeneration{
		repository: githubapp.Repository{InstallationID: 1, URL: repositoryURL},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 1}}, generation: 1,
	}
	current = desiredRepositoryGeneration{
		repository: githubapp.Repository{InstallationID: 2, URL: repositoryURL},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 2}}, generation: 2,
	}
	leader.desiredStateKnown = true
	leader.desiredRepositories[repositoryURL] = current
	leader.pendingRepositoryRepairs[repositoryURL] = repositoryRepair{
		repository: old.repository, statuses: old.statuses, generation: old.generation,
		attempts: 3, retryAfter: now.Add(time.Minute),
	}
	return leader, old, current
}

func TestAuthoritativeRepositoryRemovalCancelsRepairWithoutCompletion(t *testing.T) {
	repositoryURL := "https://github.com/acme/removed"
	leader, _ := repositoryRepairTestLeader(t, nil)
	logs := &bytes.Buffer{}
	leader.daemon.logger = slog.New(slog.NewJSONHandler(logs, nil))
	expected := desiredRepositoryGeneration{
		repository: githubapp.Repository{InstallationID: 1, URL: repositoryURL},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 1}}, generation: 1,
	}
	leader.desiredStateKnown = true
	leader.pendingRepositoryRepairs[repositoryURL] = repositoryRepair{
		repository: expected.repository, statuses: expected.statuses, generation: expected.generation,
	}

	leader.finishRepositoryReconciliation(context.Background(), expected, nil)
	leader.cancelRepositoryRepair(context.Background(), repositoryURL, expected.generation)

	if _, pending := leader.pendingRepositoryRepairs[repositoryURL]; pending {
		t.Fatal("authoritatively removed repository retained repair")
	}
	if output := logs.String(); !strings.Contains(output, `"phase":"canceled"`) ||
		strings.Contains(output, `"phase":"completed"`) {
		t.Fatalf("removed repository repair log = %q", output)
	}
	leader.stop(context.Background())
}

func TestUnknownDesiredRepositoryIncompleteResultQueuesRepair(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	expected := desiredRepositoryGeneration{
		repository: githubapp.Repository{InstallationID: 1, URL: "https://github.com/acme/unknown"},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 1}}, generation: 1,
	}

	leader.finishRepositoryReconciliation(context.Background(), expected, nil)

	if repair, pending := leader.pendingRepositoryRepairs[expected.repository.URL]; !pending || repair.generation != expected.generation {
		t.Fatalf("incomplete unknown desired repair = %#v, pending=%t", repair, pending)
	}
	leader.stop(context.Background())
}

func TestUnexpectedPollerStopIgnoresStoppedAndReplacedSessionsAndReportsCloseFailure(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	stopped := stoppedSession(key, fakeScaleSetDeletionClient{}, &fakeMessageClient{})
	leader.stopped = true
	leader.handleUnexpectedSessionPollerStop(context.Background(), stopped)
	leader.stopped = false
	leader.sessions[key] = stoppedSession(key, fakeScaleSetDeletionClient{}, &fakeMessageClient{})
	leader.handleUnexpectedSessionPollerStop(context.Background(), stopped)

	logs := &bytes.Buffer{}
	leader.daemon.logger = slog.New(slog.NewJSONHandler(logs, nil))
	failed := stoppedSession(key, fakeScaleSetDeletionClient{}, &fakeMessageClient{closeErr: errors.New("close failed")})
	leader.sessions[key] = failed
	leader.handleUnexpectedSessionPollerStop(context.Background(), failed)
	if !strings.Contains(logs.String(), "close unexpectedly stopped leader message session failed") {
		t.Fatalf("unexpected poller close log = %q", logs.String())
	}
	leader.stop(context.Background())
}

func TestDueRepositoryRepairsAreReturnedInRepositoryOrder(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, "https://github.com/acme/z-last")
	addDueRepositoryRepair(leader, "https://github.com/acme/a-first")
	repairs := leader.dueRepositoryRepairs()
	if len(repairs) != 2 || repairs[0].repository.URL != "https://github.com/acme/a-first" {
		t.Fatalf("due repairs = %#v, want repository order", repairs)
	}
	leader.stop(context.Background())
}

func addDueRepositoryRepair(leader *clusterLeader, repositoryURL string) {
	leader.repairMutex.Lock()
	leader.pendingRepositoryRepairs[repositoryURL] = repositoryRepair{
		repository: githubapp.Repository{InstallationID: 1, URL: repositoryURL},
		statuses: []cluster.ScaleSetStatus{{
			Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1,
		}},
	}
	leader.repairMutex.Unlock()
}
