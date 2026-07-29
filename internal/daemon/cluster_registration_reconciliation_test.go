package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestClusterRepositoryActivationRetainsRegistrationsAcrossPartialSessionFailure(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	leader, registry, desired, repositories := fixture.leader, fixture.registry, fixture.desired, fixture.repositories
	registry.failSessionName = "windows"

	if err := leader.startMissingSessions(context.Background(), desired, repositories); err == nil {
		t.Fatal("expected partial session failure")
	}
	if len(leader.sessions) != 0 || len(registry.registrations) != 2 || registry.closedSessions != 1 {
		t.Fatalf("sessions=%d registrations=%d closed=%d", len(leader.sessions), len(registry.registrations), registry.closedSessions)
	}

	registry.failSessionName = ""
	if err := leader.startMissingSessions(context.Background(), desired, repositories); err != nil {
		t.Fatal(err)
	}
	if len(leader.sessions) != 2 || registry.createCalls["linux"] != 1 || registry.createCalls["windows"] != 1 {
		t.Fatalf("sessions=%d createCalls=%#v", len(leader.sessions), registry.createCalls)
	}
	if registry.listCalls != 0 {
		t.Fatalf("registration lists=%d, want 0", registry.listCalls)
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryAuditRepairsDriftAndRetainsSessionsOnTransientFailure(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	leader, registry, desired, repositories := fixture.leader, fixture.registry, fixture.desired, fixture.repositories
	if err := leader.startMissingSessions(context.Background(), desired, repositories); err != nil {
		t.Fatal(err)
	}
	oldLinux := leader.sessions[repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}]
	registry.getErr = errors.New("audit unavailable")
	if err := leader.startMissingSessions(context.Background(), desired, repositories); err == nil {
		t.Fatal("expected degraded audit")
	}
	if leader.sessions[oldLinux.key] != oldLinux || registry.closedSessions != 0 {
		t.Fatal("transient audit failure replaced healthy sessions")
	}

	registry.getErr = nil
	delete(registry.registrations, "linux")
	if err := leader.startMissingSessions(context.Background(), desired, repositories); err != nil {
		t.Fatal(err)
	}
	newLinux := leader.sessions[oldLinux.key]
	if newLinux == oldLinux || newLinux.runnerScaleSet.ID == oldLinux.runnerScaleSet.ID {
		t.Fatal("confirmed registration drift did not replace the complete session generation")
	}
	if registry.closedSessions != 2 || len(leader.sessions) != 2 {
		t.Fatalf("closed=%d sessions=%d", registry.closedSessions, len(leader.sessions))
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryAuditKeepsMatchingSessionGeneration(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	if err := fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories); err != nil {
		t.Fatal(err)
	}
	original := fixture.leader.sessions[repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}]
	if err := fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories); err != nil {
		t.Fatal(err)
	}
	if fixture.leader.sessions[original.key] != original {
		t.Fatal("matching generation was replaced")
	}
	fixture.leader.stop(context.Background())
}

func TestClusterRepositoryRegistrationAuditReportsClientFailure(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	fixture.leader.daemon.newGitHubClient = func(
		string, githubapp.InstallationID, config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		return nonPruningScaleSetClient{}, nil
	}
	if err := fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories); err == nil {
		t.Fatal("expected registration client failure")
	}
}

func TestClusterRepositorySessionGenerationRequiresEveryRegistration(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	_, err := fixture.leader.startRepositorySessionGeneration(
		context.Background(), fixture.repositories["repo"],
		[]cluster.ScaleSetStatus{{Name: "linux"}}, fixture.registry, map[string]*upstream.RunnerScaleSet{},
	)
	if err == nil {
		t.Fatal("expected missing confirmed registration")
	}
}

func TestClusterRepositorySessionSuspensionIsIsolatedAndReportsCloseFailure(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	otherKey := repositoryScaleSetSessionKey{repository: "other", scaleSet: "linux"}
	fixture.leader.sessions[otherKey] = stoppedSession(otherKey, fakeScaleSetDeletionClient{}, &fakeMessageClient{})
	targetKey := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	fixture.leader.sessions[targetKey] = stoppedSession(targetKey, fakeScaleSetDeletionClient{}, &fakeMessageClient{
		closeErr: errors.New("close failed"),
	})
	if err := fixture.leader.suspendRepositorySessions(context.Background(), "repo"); err == nil {
		t.Fatal("expected close failure")
	}
	if fixture.leader.sessions[otherKey] == nil {
		t.Fatal("other repository session was removed")
	}
}

func TestClusterRepositoryReconciliationLeavesFailedGenerationAbsentWhenSuspensionFails(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	active := stoppedSession(key, fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(context.Context, int) error { return nil }},
		&fakeMessageClient{closeErr: errors.New("close failed")})
	active.runnerScaleSet = &upstream.RunnerScaleSet{ID: 999, Name: "linux"}
	fixture.leader.sessions[key] = active
	err := fixture.leader.reconcileRepositorySessions(
		context.Background(), fixture.repositories["repo"], desiredRepositoryStatuses("repo", fixture.desired),
	)
	if err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("reconciliation error = %v", err)
	}
	if fixture.leader.sessions[key] != nil || fixture.registry.closedSessions != 0 {
		t.Fatalf("session=%#v replacement close calls=%d", fixture.leader.sessions[key], fixture.registry.closedSessions)
	}
}

func TestClusterSessionMatchingAndDeclarationHelpersHandleEmptyAndPresentSets(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	if !fixture.leader.repositorySessionsMatch("repo", nil, nil) {
		t.Fatal("empty desired generation should match")
	}
	if !hasClusterStatus([]cluster.ScaleSetStatus{{Name: "linux"}}, "linux") {
		t.Fatal("declared scale set was not found")
	}
}

type clusterRegistrationClient struct {
	*registrationReconciliationClient
	failSessionName string
	closedSessions  int
}

type clusterRegistrationTestFixture struct {
	leader       *clusterLeader
	registry     *clusterRegistrationClient
	desired      map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus
	repositories map[string]githubapp.Repository
}

func clusterRegistrationFixture(t *testing.T) clusterRegistrationTestFixture {
	t.Helper()
	registry := &clusterRegistrationClient{registrationReconciliationClient: successfulRegistrationReconciliationClient()}
	registry.messageSessionClient = registry.messageSession
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	daemon.config = clusteredConfig(t)
	daemon.logger = slog.Default()
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return registry, nil
	}
	desired := map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		{repository: "repo", scaleSet: "linux"}: {
			Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
		},
		{repository: "repo", scaleSet: "windows"}: {
			Name: "windows", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
		},
	}
	return clusterRegistrationTestFixture{
		leader: newClusterLeader(daemon, &fakeClusterLease{}), registry: registry, desired: desired,
		repositories: map[string]githubapp.Repository{"repo": {URL: "repo", InstallationID: 1}},
	}
}

func (client *clusterRegistrationClient) messageSession(
	_ context.Context,
	runnerScaleSetID int,
	_ string,
) (scaleSetMessageClient, error) {
	name := ""
	for registrationName, registration := range client.registrations {
		if registration.ID == runnerScaleSetID {
			name = registrationName
			break
		}
	}
	if name == client.failSessionName {
		return scaleSetMessageClient{}, errors.New("session unavailable")
	}
	messageClient := &registrationMessageClient{onClose: func() { client.closedSessions++ }}
	return scaleSetMessageClient{Client: messageClient}, nil
}

type registrationMessageClient struct {
	fakeMessageClient
	onClose func()
}

func (client *registrationMessageClient) GetMessage(ctx context.Context, _, _ int) (*upstream.RunnerScaleSetMessage, error) {
	<-ctx.Done()
	return nil, fmt.Errorf("wait for registration message: %w", ctx.Err())
}

func (client *registrationMessageClient) Close(context.Context) error {
	client.onClose()
	return nil
}
