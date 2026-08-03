package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterRepositoryActivationRetainsRegistrationsAcrossPartialSessionFailure(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	leader, registry, desired, repositories := fixture.leader, fixture.registry, fixture.desired, fixture.repositories
	now := time.Unix(100, 0)
	leader.now = func() time.Time { return now }
	registry.failSessionName = "windows"

	if err := leader.startMissingSessions(context.Background(), desired, repositories); err == nil {
		t.Fatal("expected partial session failure")
	}
	if len(leader.sessions) != 0 || len(registry.registrations) != 2 || registry.closedSessions.Load() != 1 {
		t.Fatalf("sessions=%d registrations=%d closed=%d", len(leader.sessions), len(registry.registrations), registry.closedSessions.Load())
	}

	registry.failSessionName = ""
	now = now.Add(5 * time.Second)
	leader.repairPendingRepositories(context.Background())
	if len(leader.sessions) != 2 || registry.createCalls["linux"] != 1 || registry.createCalls["windows"] != 1 {
		t.Fatalf("sessions=%d createCalls=%#v", len(leader.sessions), registry.createCalls)
	}
	if registry.listCalls != 0 {
		t.Fatalf("registration lists=%d, want 0", registry.listCalls)
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryActivationPersistsInitialAssignedJobsBeforePolling(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	bootstrapEntered, releaseBootstrap := blockInitialAssignedRecovery(&fixture)
	completed := make(chan error, 1)
	go func() {
		completed <- fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories)
	}()
	<-bootstrapEntered
	select {
	case <-fixture.registry.pollObserved:
		t.Fatal("message polling started before assigned-job obligations were durable")
	default:
	}
	close(releaseBootstrap)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	<-fixture.registry.pollObserved
	fixture.leader.stop(context.Background())
	assertInitialAssignedRecovery(t, fixture.state)
}

func blockInitialAssignedRecovery(fixture *clusterRegistrationTestFixture) (<-chan struct{}, chan struct{}) {
	linuxKey := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	fixture.desired = map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{linuxKey: fixture.desired[linuxKey]}
	fixture.registry.initialStatistics = map[string]*upstream.RunnerScaleSetStatistic{
		"linux": {TotalAssignedJobs: 1},
	}
	fixture.state.assignedJobCapacity = scaler.AssignedJobCapacity{AvailableCapacity: 1}
	bootstrapEntered := make(chan struct{})
	releaseBootstrap := make(chan struct{})
	var bootstrapOnce sync.Once
	fixture.state.onObserveAssignedJobs = func() {
		bootstrapOnce.Do(func() {
			close(bootstrapEntered)
			<-releaseBootstrap
		})
	}
	fixture.registry.bootstrapCompleted = releaseBootstrap
	return bootstrapEntered, releaseBootstrap
}

func assertInitialAssignedRecovery(t *testing.T, state *fakeClusterState) {
	t.Helper()
	observations, assignments, _ := state.assignedJobsSnapshot()
	if len(observations) < 1 || observations[0].AssignedJobs != 1 {
		t.Fatalf("assigned-job observations = %#v", observations)
	}
	if len(assignments) != 1 {
		t.Fatalf("assigned-job assignments = %#v", assignments)
	}
}

func TestClusterRepositoryActivationDoesNotPublishSessionBeforeAssignedRecoveryIsDurable(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	linuxKey := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	fixture.desired = map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{linuxKey: fixture.desired[linuxKey]}
	bootstrapEntered, releaseBootstrap := blockAssignedObservation(fixture.state)
	completed := make(chan error, 1)
	go func() {
		completed <- fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories)
	}()
	<-bootstrapEntered
	if session := fixture.leader.scheduler.session(linuxKey); session != nil {
		t.Fatal("scheduler published session before assigned recovery was durable")
	}
	select {
	case err := <-completed:
		t.Fatalf("repository activation completed before assigned recovery: %v", err)
	default:
	}
	close(releaseBootstrap)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if session := fixture.leader.scheduler.session(linuxKey); session == nil {
		t.Fatal("scheduler did not publish session after assigned recovery became durable")
	}
	fixture.leader.stop(context.Background())
}

func blockAssignedObservation(state *fakeClusterState) (<-chan struct{}, chan struct{}) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	state.onObserveAssignedJobs = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	return entered, release
}

func TestClusterRepositoryActivationQueuesRepairWhenInitialStatisticsAreMissing(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	fixture.registry.missingStatistics = map[string]bool{"linux": true}
	if err := fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories); err == nil {
		t.Fatal("expected missing initial statistics failure")
	}
	if len(fixture.leader.sessions) != 0 {
		t.Fatalf("sessions activated without initial statistics = %#v", fixture.leader.sessions)
	}
	if _, pending := fixture.leader.pendingRepositoryRepairs["repo"]; !pending {
		t.Fatal("repository repair was not queued")
	}
}

func TestClusterRepositoryAuditRepairsDriftAndRetainsSessionsOnTransientFailure(t *testing.T) {
	fixture := clusterRegistrationFixture(t)
	var now atomic.Int64
	now.Store(100)
	fixture.leader.now = func() time.Time { return time.Unix(now.Load(), 0) }
	oldLinux := startRegistrationSessions(t, fixture)
	assertTransientRegistrationAuditRetainsSessions(t, fixture, oldLinux)
	repairDriftedRegistration(t, fixture, oldLinux, &now)
	fixture.leader.stop(context.Background())
}

func startRegistrationSessions(
	t *testing.T,
	fixture clusterRegistrationTestFixture,
) *repositoryScaleSetSession {
	t.Helper()
	if err := fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories); err != nil {
		t.Fatal(err)
	}
	return fixture.leader.sessions[repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}]
}

func assertTransientRegistrationAuditRetainsSessions(
	t *testing.T,
	fixture clusterRegistrationTestFixture,
	oldLinux *repositoryScaleSetSession,
) {
	t.Helper()
	fixture.registry.getErr = errors.New("audit unavailable")
	if err := fixture.leader.startMissingSessions(context.Background(), fixture.desired, fixture.repositories); err == nil {
		t.Fatal("expected degraded audit")
	}
	if fixture.leader.sessions[oldLinux.key] != oldLinux || fixture.registry.closedSessions.Load() != 0 {
		t.Fatal("transient audit failure replaced healthy sessions")
	}
}

func repairDriftedRegistration(
	t *testing.T,
	fixture clusterRegistrationTestFixture,
	oldLinux *repositoryScaleSetSession,
	now *atomic.Int64,
) {
	t.Helper()
	fixture.registry.getErr = nil
	delete(fixture.registry.registrations, "linux")
	now.Add(5)
	fixture.leader.repairPendingRepositories(context.Background())
	newLinux := fixture.leader.sessions[oldLinux.key]
	if newLinux == oldLinux || newLinux.runnerScaleSet.ID == oldLinux.runnerScaleSet.ID {
		t.Fatal("confirmed registration drift did not replace the complete session generation")
	}
	if fixture.registry.closedSessions.Load() != 2 || len(fixture.leader.sessions) != 2 {
		t.Fatalf("closed=%d sessions=%d", fixture.registry.closedSessions.Load(), len(fixture.leader.sessions))
	}
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
	if fixture.leader.sessions[key] != nil || fixture.registry.closedSessions.Load() != 0 {
		t.Fatalf("session=%#v replacement close calls=%d", fixture.leader.sessions[key], fixture.registry.closedSessions.Load())
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
	failSessionName       string
	closedSessions        atomic.Int32
	initialStatistics     map[string]*upstream.RunnerScaleSetStatistic
	missingStatistics     map[string]bool
	bootstrapCompleted    <-chan struct{}
	polledBeforeBootstrap bool
	pollObserved          chan struct{}
	pollOnce              sync.Once
}

type clusterRegistrationTestFixture struct {
	leader       *clusterLeader
	registry     *clusterRegistrationClient
	state        *fakeClusterState
	desired      map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus
	repositories map[string]githubapp.Repository
}

func clusterRegistrationFixture(t *testing.T) clusterRegistrationTestFixture {
	t.Helper()
	registry := &clusterRegistrationClient{
		registrationReconciliationClient: successfulRegistrationReconciliationClient(),
		pollObserved:                     make(chan struct{}),
	}
	registry.messageSessionClient = registry.messageSession
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	daemon.config = clusteredConfig(t)
	daemon.logger = slog.Default()
	daemon.assignments = state
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
		leader: newClusterLeader(context.Background(), daemon, &fakeClusterLease{}), registry: registry, state: state, desired: desired,
		repositories: map[string]githubapp.Repository{"repo": {URL: "repo", InstallationID: 1}},
	}
}

func (client *clusterRegistrationClient) messageSession(
	_ context.Context,
	runnerScaleSetID int,
	_ string,
) (scaleSetMessageClient, error) {
	name := client.registrationName(runnerScaleSetID)
	if name == client.failSessionName {
		return scaleSetMessageClient{}, errors.New("session unavailable")
	}
	statistics := client.sessionStatistics(name)
	messageClient := &registrationMessageClient{
		fakeMessageClient: fakeMessageClient{
			session: upstream.RunnerScaleSetSession{Statistics: statistics}, missingSessionStatistics: statistics == nil,
		},
		onClose:    func() { client.closedSessions.Add(1) },
		beforePoll: client.recordEarlyPoll,
	}
	return scaleSetMessageClient{Client: messageClient}, nil
}

func (client *clusterRegistrationClient) registrationName(runnerScaleSetID int) string {
	for registrationName, registration := range client.registrations {
		if registration.ID == runnerScaleSetID {
			return registrationName
		}
	}
	return ""
}

func (client *clusterRegistrationClient) sessionStatistics(name string) *upstream.RunnerScaleSetStatistic {
	if !client.missingStatistics[name] {
		if configured := client.initialStatistics[name]; configured != nil {
			return configured
		}
		return &upstream.RunnerScaleSetStatistic{}
	}
	return nil
}

func (client *clusterRegistrationClient) recordEarlyPoll() {
	defer client.pollOnce.Do(func() { close(client.pollObserved) })
	if client.bootstrapCompleted == nil {
		return
	}
	select {
	case <-client.bootstrapCompleted:
	default:
		client.polledBeforeBootstrap = true
	}
}

type registrationMessageClient struct {
	fakeMessageClient
	onClose    func()
	beforePoll func()
}

func (client *registrationMessageClient) GetMessage(ctx context.Context, _, _ int) (*upstream.RunnerScaleSetMessage, error) {
	if client.beforePoll != nil {
		client.beforePoll()
	}
	<-ctx.Done()
	return nil, fmt.Errorf("wait for registration message: %w", ctx.Err())
}

func (client *registrationMessageClient) Close(context.Context) error {
	client.onClose()
	return nil
}
