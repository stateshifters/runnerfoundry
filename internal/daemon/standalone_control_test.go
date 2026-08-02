package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestStandaloneControlReconnectsDurableRepositoryBeforeDiscoveryCompletes(t *testing.T) {
	fixture := newBlockedDiscoveryControl(t)
	defer fixture.stop()
	fixture.control.RequestDiscovery(context.Background())
	waitForStandaloneSignal(t, fixture.discoveryStarted, "discovery did not start")
	fixture.control.RequestDiscovery(context.Background())
	registrationID := waitForStandaloneInt(t, fixture.started, "durable repository did not reconnect while discovery was blocked")
	if registrationID != 1 {
		t.Fatalf("registration ID = %d, want freshly audited ID 1", registrationID)
	}
}

type blockedDiscoveryControlFixture struct {
	control          *standaloneControl
	discoveryStarted <-chan struct{}
	discoveryRelease chan struct{}
	started          <-chan int
}

func (fixture blockedDiscoveryControlFixture) stop() {
	close(fixture.discoveryRelease)
	fixture.control.Stop()
}

func newBlockedDiscoveryControl(t *testing.T) blockedDiscoveryControlFixture {
	t.Helper()
	discoveryStarted := make(chan struct{})
	discoveryRelease := make(chan struct{})
	started := make(chan int, 1)
	active := blockedDiscoveryActiveConfiguration(discoveryStarted, discoveryRelease)
	control, err := startStandaloneControl(
		context.Background(), blockedDiscoveryDaemon(started), &active, standaloneControlOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	return blockedDiscoveryControlFixture{
		control: control, discoveryStarted: discoveryStarted, discoveryRelease: discoveryRelease, started: started,
	}
}

func blockedDiscoveryDaemon(started chan<- int) standaloneDaemon {
	return standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return successfulRegistrationReconciliationClient(), nil
		},
		startConfirmedScaleSet: func(
			_ context.Context, _ *slog.Logger, _ scaleSetClient, _ string, _ config.ScaleSetConfig,
			_ *scaler.Coordinator, registration *upstream.RunnerScaleSet,
		) (scaleSetLifecycle, error) {
			started <- registration.ID
			return inertLifecycle(), nil
		},
	}
}

func blockedDiscoveryActiveConfiguration(started chan struct{}, release chan struct{}) activeConfiguration {
	const repositoryURL = "https://github.com/acme/repository"
	state := newMemoryStandaloneState()
	state.snapshots[1] = repositorystate.Snapshot{
		Repositories: []repositorystate.Repository{{AppID: 1, URL: repositoryURL, InstallationID: 7}},
		Registrations: []repositorystate.Registration{{
			AppID: 1, RepositoryURL: repositoryURL, ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 42,
		}},
	}
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = state
	active.RepositoryMutex = nil
	active.Discoverer = blockingStandaloneDiscoverer{started: started, release: release}
	return active
}

func TestStandaloneControlReconcilesRepositoriesIndependently(t *testing.T) {
	control, started := newIndependentRepositoryControl(t)
	defer control.Stop()
	control.Reconcile(context.Background(), githubapp.Repository{InstallationID: 1, URL: "blocked"}, repositoryWorkRoutine)
	control.Reconcile(context.Background(), githubapp.Repository{InstallationID: 2, URL: "healthy"}, repositoryWorkRoutine)
	repositoryURL := waitForStandaloneString(t, started, "blocked repository delayed independent reconciliation")
	if repositoryURL != "healthy" {
		t.Fatalf("started repository = %q, want healthy", repositoryURL)
	}
}

func newIndependentRepositoryControl(t *testing.T) (*standaloneControl, <-chan string) {
	t.Helper()
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	active.RepositoryMutex = nil
	started := make(chan string, 2)
	daemon := standaloneDaemon{
		newGitHubClient: func(repositoryURL string, _ githubapp.InstallationID, _ config.GitHubAppCredentials) (scaleSetClient, error) {
			client := successfulRegistrationReconciliationClient()
			if repositoryURL == "blocked" {
				client.getRunnerScaleSet = func(ctx context.Context, _ int, _ string) (*upstream.RunnerScaleSet, error) {
					<-ctx.Done()
					return nil, fmt.Errorf("blocked scale set lookup canceled: %w", ctx.Err())
				}
			}
			return client, nil
		},
		startScaleSet: func(
			_ context.Context, _ *slog.Logger, _ scaleSetClient, repositoryURL string,
			_ config.ScaleSetConfig, _ *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			started <- repositoryURL
			return inertLifecycle(), nil
		},
	}
	control, err := startStandaloneControl(context.Background(), daemon, &active, standaloneControlOptions{
		attemptTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return control, started
}

func TestStandaloneControlRetriesFailedRepositoryWithBoundedBackoff(t *testing.T) {
	scheduled := make(chan scheduledStandaloneControlRetry, 6)
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	daemon := standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return nil, errors.New("unavailable")
		},
	}
	control, err := startStandaloneControl(context.Background(), daemon, &active, standaloneControlOptions{
		retryAfter: func(delay time.Duration, retry func()) standaloneRetryTimer {
			scheduled <- scheduledStandaloneControlRetry{delay: delay, retry: retry}
			return inertStandaloneRetryTimer{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer control.Stop()
	control.Reconcile(context.Background(), githubapp.Repository{InstallationID: 1, URL: "repo"}, repositoryWorkUrgent)
	for index, expectedDelay := range []time.Duration{
		5 * time.Second, 10 * time.Second, 20 * time.Second,
		40 * time.Second, 60 * time.Second, 60 * time.Second,
	} {
		retry := waitForStandaloneRetry(t, scheduled)
		assertStandaloneRetryDelay(t, index+1, retry.delay, expectedDelay)
		retry.retry()
	}
}

func assertStandaloneRetryDelay(t *testing.T, attempt int, actual time.Duration, expected time.Duration) {
	t.Helper()
	if actual != expected {
		t.Fatalf("retry %d delay = %s, want %s", attempt, actual, expected)
	}
}

func TestStandaloneControlCancelsObsoleteRetryOnShutdown(t *testing.T) {
	scheduled := make(chan scheduledStandaloneControlRetry, 1)
	timer := &recordingStandaloneRetryTimer{stopped: make(chan struct{})}
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	var attempts int
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			attempts++
			return nil, errors.New("unavailable")
		},
	}, &active, standaloneControlOptions{
		retryAfter: func(delay time.Duration, retry func()) standaloneRetryTimer {
			scheduled <- scheduledStandaloneControlRetry{delay: delay, retry: retry}
			return timer
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	control.Reconcile(context.Background(), githubapp.Repository{URL: "repo"}, repositoryWorkUrgent)
	retry := <-scheduled
	control.Stop()
	select {
	case <-timer.stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel repository retry")
	}
	retry.retry()
	if attempts != 1 {
		t.Fatalf("obsolete retry attempts = %d, want 1", attempts)
	}
}

func TestStartStandaloneControlReportsRepositoryStateFailures(t *testing.T) {
	expectedErr := errors.New("state failed")
	confirmedDiscovery := githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}
	confirmedRegistration := repositorystate.Registration{AppID: 1, RepositoryURL: "repo", ScaleSetName: "linux"}
	tests := []struct {
		name          string
		state         *faultStandaloneState
		discovery     *githubapp.DiscoveryResult
		registrations []repositorystate.Registration
	}{
		{name: "start controller", state: &faultStandaloneState{standaloneState: newMemoryStandaloneState(), startErr: expectedErr}},
		{name: "confirmed discovery", state: &faultStandaloneState{standaloneState: newMemoryStandaloneState(), applyErr: expectedErr}, discovery: &confirmedDiscovery},
		{name: "confirmed registration", state: &faultStandaloneState{standaloneState: newMemoryStandaloneState(), upsertErr: expectedErr}, registrations: []repositorystate.Registration{confirmedRegistration}},
		{name: "reload confirmed snapshot", state: &faultStandaloneState{standaloneState: newMemoryStandaloneState(), loadErr: expectedErr}, registrations: []repositorystate.Registration{confirmedRegistration}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			active := testActiveConfiguration()
			active.Credentials.AppID = 1
			active.RepositoryState = test.state
			active.ConfirmedDiscovery = test.discovery
			active.ConfirmedRegistrations = test.registrations
			if _, err := startStandaloneControl(context.Background(), standaloneDaemon{}, &active, standaloneControlOptions{}); !errors.Is(err, expectedErr) {
				t.Fatalf("start error = %v, want %v", err, expectedErr)
			}
		})
	}
}

func TestStartStandaloneControlAppliesReloadConfirmedState(t *testing.T) {
	active := confirmedStandaloneActiveConfiguration()
	control := mustStartOfflineStandaloneControl(t, &active)
	callback := make(chan struct{})
	control.retryAfter(0, func() { close(callback) })
	waitForStandaloneSignal(t, callback, "default retry scheduler did not invoke callback")
	control.Stop()
	assertConfirmedStandaloneStateConsumed(t, active)
}

func confirmedStandaloneActiveConfiguration() activeConfiguration {
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	active.ConfirmedDiscovery = &githubapp.DiscoveryResult{
		Installations:        []githubapp.InstallationID{1, 2},
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: "repo"}},
		InstallationFailures: map[githubapp.InstallationID]error{2: errors.New("unavailable")},
	}
	active.ConfirmedRegistrations = []repositorystate.Registration{{
		AppID: 1, RepositoryURL: "repo", ScaleSetName: "linux", GitHubScaleSetID: 9,
	}}
	return active
}

func mustStartOfflineStandaloneControl(t *testing.T, active *activeConfiguration) *standaloneControl {
	t.Helper()
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return nil, errors.New("offline")
		},
	}, active, standaloneControlOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return control
}

func assertConfirmedStandaloneStateConsumed(t *testing.T, active activeConfiguration) {
	t.Helper()
	if len(active.RepositorySnapshot.Repositories) != 1 || len(active.RepositorySnapshot.Registrations) != 1 ||
		active.ConfirmedDiscovery != nil || active.ConfirmedRegistrations != nil {
		t.Fatalf("active confirmed state = %#v", active)
	}
}

func TestStartStandaloneControlAuditsAlreadyHealthyDurableRepository(t *testing.T) {
	state := newMemoryStandaloneState()
	state.snapshots[1] = repositorystate.Snapshot{Repositories: []repositorystate.Repository{{
		AppID: 1, URL: "repo", InstallationID: 1,
	}}}
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = state
	client := successfulRegistrationReconciliationClient()
	originalGet := client.getRunnerScaleSet
	audited := make(chan struct{}, 1)
	client.getRunnerScaleSet = func(ctx context.Context, runnerGroupID int, name string) (*upstream.RunnerScaleSet, error) {
		audited <- struct{}{}
		return originalGet(ctx, runnerGroupID, name)
	}
	active.Repositories["repo"] = managedRepository{
		InstallationID: 1, RegistrationStatus: repositoryRegistrationOperational,
		Client:     client,
		Lifecycles: map[string]scaleSetLifecycle{"linux": {RunnerScaleSetID: 1}},
	}
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{}, &active, standaloneControlOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-audited:
	case <-time.After(time.Second):
		t.Fatal("healthy durable repository was not audited")
	}
	control.Stop()
}

func TestStandaloneControlDiscoveryFailuresDoNotReplaceDurableSnapshot(t *testing.T) {
	expectedErr := errors.New("discovery failed")
	for _, test := range standaloneDiscoveryFailureCases(expectedErr) {
		t.Run(test.name, func(t *testing.T) {
			assertStandaloneDiscoveryFailureRetainsSnapshot(t, test)
		})
	}
}

type standaloneDiscoveryFailureCase struct {
	name       string
	discoverer repositoryDiscoverer
	state      repositorystate.Store
}

func standaloneDiscoveryFailureCases(expectedErr error) []standaloneDiscoveryFailureCase {
	return []standaloneDiscoveryFailureCase{
		{name: "discover", discoverer: staticDiscoverer{err: expectedErr}, state: newMemoryStandaloneState()},
		{name: "initial discover", discoverer: staticDiscoverer{err: expectedErr}, state: &faultStandaloneState{
			standaloneState: newMemoryStandaloneState(),
			startSnapshot:   &repositorystate.Snapshot{Control: repositorystate.Control{ControllerEpoch: 1}},
		}},
		{name: "persist", discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{
			Repositories: []githubapp.Repository{{InstallationID: 1, URL: "repo"}},
		}}, state: &faultStandaloneState{standaloneState: newMemoryStandaloneState(), applyErr: expectedErr}},
		{name: "fenced", discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{
			Repositories: []githubapp.Repository{{InstallationID: 1, URL: "repo"}},
		}}, state: &faultStandaloneState{standaloneState: newMemoryStandaloneState(), applyErr: repositorystate.ErrControllerFenced}},
		{name: "name collision", discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{
			Repositories: []githubapp.Repository{
				{InstallationID: 1, URL: "https://github.com/acme/repo.a"},
				{InstallationID: 1, URL: "https://github.com/acme/repo-a"},
			},
		}}, state: newMemoryStandaloneState()},
	}
}

func assertStandaloneDiscoveryFailureRetainsSnapshot(t *testing.T, test standaloneDiscoveryFailureCase) {
	t.Helper()
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = test.state
	active.Discoverer = test.discoverer
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{}, &active, standaloneControlOptions{})
	if err != nil {
		t.Fatal(err)
	}
	control.RequestDiscovery(context.Background())
	control.background.Wait()
	control.Stop()
	if active.RepositorySnapshot.Control.DiscoveryGeneration != 0 {
		t.Fatalf("snapshot replaced after failed discovery: %#v", active.RepositorySnapshot)
	}
}

func TestStandaloneControlIgnoresAbsentRepositoryRemoval(t *testing.T) {
	control, active := newRemovalTestControl(t, nil)
	completed := make(chan error, 1)
	control.enqueueRemoval(context.Background(), "absent", func(err error) { completed <- err })
	if err := <-completed; err != nil {
		t.Fatalf("absent removal = %v", err)
	}
	control.Stop()
	if len(active.Repositories) != 0 {
		t.Fatalf("repositories = %#v", active.Repositories)
	}
}

func TestStandaloneControlRemovesRepository(t *testing.T) {
	shutdown := make(chan struct{})
	lifecycle := inertLifecycle()
	lifecycle.Shutdown = func(context.Context) error { close(shutdown); return nil }
	control, active := newRemovalTestControl(t, &lifecycle)
	control.enqueueRemoval(context.Background(), "repo", nil)
	waitForStandaloneSignal(t, shutdown, "repository removal did not run")
	control.Stop()
	if _, present := active.Repositories["repo"]; present {
		t.Fatal("repository was not removed")
	}
}

func TestStandaloneControlRetriesFailedRepositoryRemoval(t *testing.T) {
	scheduled := make(chan scheduledStandaloneControlRetry, 1)
	lifecycle := inertLifecycle()
	lifecycle.DestroyActive = func(context.Context) error { return errors.New("destroy failed") }
	control, _ := newRemovalTestControl(t, &lifecycle)
	control.retryAfter = func(delay time.Duration, retry func()) standaloneRetryTimer {
		scheduled <- scheduledStandaloneControlRetry{delay: delay, retry: retry}
		return inertStandaloneRetryTimer{}
	}
	control.enqueueRemoval(context.Background(), "repo", nil)
	waitForStandaloneRetry(t, scheduled)
	control.Stop()
}

func TestStandaloneControlPersistsOnlyActiveRegistrationsAndReportsFailure(t *testing.T) {
	expectedErr := errors.New("upsert failed")
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = &faultStandaloneState{standaloneState: newMemoryStandaloneState(), upsertErr: expectedErr}
	active.RepositoryMutex = &sync.Mutex{}
	control := standaloneControl{active: &active, epoch: 1}
	if err := control.persistRegistrations(context.Background(), "repo", managedRepository{}); err != nil {
		t.Fatalf("empty registration persistence = %v", err)
	}
	if err := control.persistRegistrations(context.Background(), "repo", managedRepository{
		Lifecycles: map[string]scaleSetLifecycle{"linux": {RunnerScaleSetID: 7}},
	}); !errors.Is(err, expectedErr) {
		t.Fatalf("persistence error = %v, want %v", err, expectedErr)
	}
}

func TestStandaloneControlRemovesOnlyUnconfiguredDurableRegistrations(t *testing.T) {
	control, active := newObsoleteRegistrationControl(t)
	err := control.persistRegistrations(context.Background(), "repo", managedRepository{
		Lifecycles: map[string]scaleSetLifecycle{"linux": {RunnerScaleSetID: 4}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertStandaloneRegistrations(t, active.RepositorySnapshot.Registrations)
}

func newObsoleteRegistrationControl(t *testing.T) (*standaloneControl, *activeConfiguration) {
	t.Helper()
	ctx := context.Background()
	state := newMemoryStandaloneState()
	snapshot := mustStartMemoryController(t, state, 1)
	for _, registration := range []repositorystate.Registration{
		{AppID: 1, RepositoryURL: "repo", ScaleSetName: "linux", GitHubScaleSetID: 1},
		{AppID: 1, RepositoryURL: "repo", ScaleSetName: "removed", GitHubScaleSetID: 2},
		{AppID: 1, RepositoryURL: "other", ScaleSetName: "removed", GitHubScaleSetID: 3},
	} {
		if err := state.UpsertRegistration(ctx, 1, snapshot.Control.ControllerEpoch, registration); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := state.LoadSnapshot(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = state
	active.RepositorySnapshot = snapshot
	active.RepositoryMutex = &sync.Mutex{}
	return &standaloneControl{active: &active, epoch: snapshot.Control.ControllerEpoch}, &active
}

func assertStandaloneRegistrations(t *testing.T, snapshot []repositorystate.Registration) {
	t.Helper()
	registrations := map[string]int{}
	for _, registration := range snapshot {
		registrations[registration.RepositoryURL+"/"+registration.ScaleSetName] = registration.GitHubScaleSetID
	}
	if len(registrations) != 2 || registrations["repo/linux"] != 4 || registrations["other/removed"] != 3 {
		t.Fatalf("registrations = %#v", registrations)
	}
}

func TestStandaloneControlReportsRegistrationDeleteFailure(t *testing.T) {
	expectedErr := errors.New("state failed")
	state := &faultStandaloneState{standaloneState: newMemoryStandaloneState(), deleteErr: expectedErr}
	control := standalonePersistenceFailureControl(t, state)
	control.active.RepositorySnapshot.Registrations = []repositorystate.Registration{{
		AppID: 1, RepositoryURL: "repo", ScaleSetName: "removed",
	}}
	assertStandalonePersistenceError(t, &control, expectedErr)
}

func TestStandaloneControlReportsRegistrationRefreshFailure(t *testing.T) {
	expectedErr := errors.New("state failed")
	state := &faultStandaloneState{standaloneState: newMemoryStandaloneState(), loadErr: expectedErr}
	control := standalonePersistenceFailureControl(t, state)
	assertStandalonePersistenceError(t, &control, expectedErr)
}

func standalonePersistenceFailureControl(t *testing.T, state *faultStandaloneState) standaloneControl {
	t.Helper()
	snapshot, err := state.StartController(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = state
	active.RepositorySnapshot = snapshot
	active.RepositoryMutex = &sync.Mutex{}
	return standaloneControl{active: &active, epoch: snapshot.Control.ControllerEpoch}
}

func assertStandalonePersistenceError(t *testing.T, control *standaloneControl, expectedErr error) {
	t.Helper()
	err := control.persistRegistrations(context.Background(), "repo", managedRepository{})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("persistence error = %v, want %v", err, expectedErr)
	}
}

func TestStandaloneControlReportsSuccessfulRepairAfterRetry(t *testing.T) {
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{}, &active, standaloneControlOptions{})
	if err != nil {
		t.Fatal(err)
	}
	control.retries["repo"] = 1
	control.timers["repo"] = inertStandaloneRetryTimer{}
	control.completed(context.Background(), githubapp.Repository{URL: "repo"}, nil)
	control.Stop()
	if control.retries["repo"] != 0 {
		t.Fatalf("retry count = %d", control.retries["repo"])
	}
}

func newRemovalTestControl(t *testing.T, lifecycle *scaleSetLifecycle) (*standaloneControl, *activeConfiguration) {
	t.Helper()
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	if lifecycle != nil {
		active.Repositories["repo"] = managedRepository{
			InstallationID: 1, Lifecycles: map[string]scaleSetLifecycle{"linux": *lifecycle},
		}
	}
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{}, &active, standaloneControlOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return control, &active
}

type faultStandaloneState struct {
	standaloneState
	startErr      error
	applyErr      error
	upsertErr     error
	loadErr       error
	deleteErr     error
	startSnapshot *repositorystate.Snapshot
}

func (state *faultStandaloneState) StartController(ctx context.Context, appID int64) (repositorystate.Snapshot, error) {
	if state.startErr != nil {
		return repositorystate.Snapshot{}, state.startErr
	}
	if state.startSnapshot != nil {
		return *state.startSnapshot, nil
	}
	snapshot, err := state.standaloneState.StartController(ctx, appID)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("delegate start controller: %w", err)
	}
	return snapshot, nil
}

func (state *faultStandaloneState) ApplyDiscovery(
	ctx context.Context,
	appID int64,
	epoch int64,
	installations []githubapp.InstallationID,
	failed []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) (repositorystate.Snapshot, error) {
	if state.applyErr != nil {
		return repositorystate.Snapshot{}, state.applyErr
	}
	snapshot, err := state.standaloneState.ApplyDiscovery(
		ctx, appID, epoch, installations, failed, repositories, completedAt,
	)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("delegate apply discovery: %w", err)
	}
	return snapshot, nil
}

func (state *faultStandaloneState) UpsertRegistration(
	ctx context.Context, appID int64, epoch int64, registration repositorystate.Registration,
) error {
	if state.upsertErr != nil {
		return state.upsertErr
	}
	if err := state.standaloneState.UpsertRegistration(ctx, appID, epoch, registration); err != nil {
		return fmt.Errorf("delegate upsert registration: %w", err)
	}
	return nil
}

func (state *faultStandaloneState) LoadSnapshot(ctx context.Context, appID int64) (repositorystate.Snapshot, error) {
	if state.loadErr != nil {
		return repositorystate.Snapshot{}, state.loadErr
	}
	snapshot, err := state.standaloneState.LoadSnapshot(ctx, appID)
	if err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("delegate load snapshot: %w", err)
	}
	return snapshot, nil
}

func (state *faultStandaloneState) DeleteRegistration(
	ctx context.Context, appID int64, epoch int64, repositoryURL string, scaleSetName string,
) error {
	if state.deleteErr != nil {
		return state.deleteErr
	}
	if err := state.standaloneState.DeleteRegistration(ctx, appID, epoch, repositoryURL, scaleSetName); err != nil {
		return fmt.Errorf("delegate delete registration: %w", err)
	}
	return nil
}

type scheduledStandaloneControlRetry struct {
	delay time.Duration
	retry func()
}

func waitForStandaloneSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

func waitForStandaloneInt(t *testing.T, values <-chan int, failure string) int {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal(failure)
		return 0
	}
}

func waitForStandaloneString(t *testing.T, values <-chan string, failure string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal(failure)
		return ""
	}
}

func waitForStandaloneRetry(
	t *testing.T,
	scheduled <-chan scheduledStandaloneControlRetry,
) scheduledStandaloneControlRetry {
	t.Helper()
	select {
	case retry := <-scheduled:
		return retry
	case <-time.After(time.Second):
		t.Fatal("repository retry was not scheduled")
		return scheduledStandaloneControlRetry{}
	}
}

type inertStandaloneRetryTimer struct{}

func (inertStandaloneRetryTimer) Stop() bool { return true }

type recordingStandaloneRetryTimer struct {
	stopped chan struct{}
}

func (timer *recordingStandaloneRetryTimer) Stop() bool {
	close(timer.stopped)
	return true
}

type blockingStandaloneDiscoverer struct {
	started chan struct{}
	release chan struct{}
}

func (discoverer blockingStandaloneDiscoverer) Discover(ctx context.Context) (githubapp.DiscoveryResult, error) {
	close(discoverer.started)
	select {
	case <-ctx.Done():
		return githubapp.DiscoveryResult{}, fmt.Errorf("blocking discovery canceled: %w", ctx.Err())
	case <-discoverer.release:
		return githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}, nil
	}
}
