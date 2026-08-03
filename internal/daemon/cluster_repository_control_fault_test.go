package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestClusterRepositoryControllerFailureDegradesLeader(t *testing.T) {
	store := &faultRepositoryStateStore{fakeRepositoryStateStore: &fakeRepositoryStateStore{}, startErr: errors.New("failed")}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if leader.reconcile(context.Background()) {
		t.Fatal("leader with a failed repository controller reported operational")
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryDiscoveryPublicationFailureDegradesUninitializedLeader(t *testing.T) {
	store := &faultRepositoryStateStore{fakeRepositoryStateStore: &fakeRepositoryStateStore{}, applyErr: errors.New("failed")}
	discovery := githubapp.DiscoveryResult{
		Installations:        []githubapp.InstallationID{1},
		InstallationFailures: map[githubapp.InstallationID]error{1: errors.New("unavailable")},
	}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{result: discovery})
	if leader.reconcile(context.Background()) {
		t.Fatal("leader with an unpublished discovery reported operational")
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryControllerPreventsOverlappingDiscovery(t *testing.T) {
	leader, _ := durableRepositoryTestLeader(t, &fakeRepositoryStateStore{}, staticDiscoverer{})
	leader.discoveryRunning = true
	if leader.reconcile(context.Background()) {
		t.Fatal("uninitialized leader without an available discovery reported operational")
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryControllerStartIsIdempotent(t *testing.T) {
	store := &fakeRepositoryStateStore{}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	if leader.repositorySnapshot.Control.ControllerEpoch != 1 {
		t.Fatalf("controller epoch = %d, want 1", leader.repositorySnapshot.Control.ControllerEpoch)
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryControllerFencesWritesAfterAppCredentialsChange(t *testing.T) {
	store := &fakeRepositoryStateStore{}
	leader, daemon := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	daemon.configurationMutex.Lock()
	daemon.credentials.AppID = 43
	daemon.configurationMutex.Unlock()
	assertRepositoryControllerFenced(t, leader)
	assertRepositorySnapshotEmpty(t, store)
	leader.stop(context.Background())
}

func assertRepositoryControllerFenced(t *testing.T, leader *clusterLeader) {
	t.Helper()
	if leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller remained active under different app credentials")
	}
	if _, applied := leader.applyRepositoryDiscovery(context.Background(), githubapp.DiscoveryResult{}); applied {
		t.Fatal("old repository controller published discovery under different app credentials")
	}
	if err := leader.persistConfirmedRegistrations(context.Background(), "repo", nil, nil); !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("registration persistence error = %v, want controller fencing", err)
	}
	if err := leader.deletePersistedRegistration(
		context.Background(), repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
	); !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("registration deletion error = %v, want controller fencing", err)
	}
}

func assertRepositorySnapshotEmpty(t *testing.T, store repositorystate.Store) {
	t.Helper()
	snapshot, err := store.LoadSnapshot(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Control.DiscoveryGeneration != 0 || len(snapshot.Registrations) != 0 {
		t.Fatalf("old app snapshot changed after fencing: %#v", snapshot)
	}
}

func TestClusterRepositoryDiscoveryCannotStartAfterLeadershipStops(t *testing.T) {
	leader, _ := durableRepositoryTestLeader(t, &fakeRepositoryStateStore{}, staticDiscoverer{})
	termCtx, cancelTerm := context.WithCancel(context.Background())
	cancelTerm()
	if results := leader.beginRepositoryDiscovery(termCtx); results != nil {
		t.Fatal("repository discovery started after leadership stopped")
	}
	leader.stop(context.Background())
}

func TestBackgroundRepositoryDiscoveryHandlesPublicationFailure(t *testing.T) {
	store := &faultRepositoryStateStore{fakeRepositoryStateStore: &fakeRepositoryStateStore{}, applyErr: errors.New("failed")}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	results := make(chan repositoryDiscoveryOutcome, 1)
	results <- repositoryDiscoveryOutcome{discovery: githubapp.DiscoveryResult{}}
	leader.continueRepositoryDiscovery(context.Background(), results, nil, nil)
	leader.background.Wait()
	leader.stop(context.Background())
}

func TestBackgroundRepositoryDiscoveryPublishesChangedDesiredState(t *testing.T) {
	store := &fakeRepositoryStateStore{}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	repository := githubapp.Repository{InstallationID: 1, URL: "https://github.com/acme/new"}
	results := make(chan repositoryDiscoveryOutcome, 1)
	results <- repositoryDiscoveryOutcome{discovery: githubapp.DiscoveryResult{
		Installations: []githubapp.InstallationID{1}, Repositories: []githubapp.Repository{repository},
	}}
	statuses := []cluster.ScaleSetStatus{{Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1}}
	leader.continueRepositoryDiscovery(context.Background(), results, statuses, nil)
	leader.background.Wait()
	leader.desiredMutex.Lock()
	_, desired := leader.desiredRepositories[repository.URL]
	leader.desiredMutex.Unlock()
	if !desired {
		t.Fatal("changed discovery did not publish repository desired state")
	}
	leader.stop(context.Background())
}

func TestRepositorySetComparisonIncludesLengthAndInstallation(t *testing.T) {
	repository := githubapp.Repository{InstallationID: 1, URL: "repo"}
	if sameRepositorySet(nil, []githubapp.Repository{repository}) {
		t.Fatal("different repository counts matched")
	}
	if !sameRepositorySet([]githubapp.Repository{repository}, []githubapp.Repository{repository}) {
		t.Fatal("identical repositories did not match")
	}
	changed := repository
	changed.InstallationID = 2
	if sameRepositorySet([]githubapp.Repository{repository}, []githubapp.Repository{changed}) {
		t.Fatal("different repository installations matched")
	}
}

func TestPersistedRegistrationFailuresAreReturnedWithoutRequiringEveryRegistration(t *testing.T) {
	store := &faultRepositoryStateStore{fakeRepositoryStateStore: &fakeRepositoryStateStore{}, upsertErr: errors.New("failed")}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	statuses := []cluster.ScaleSetStatus{{Name: "linux"}, {Name: "windows"}}
	err := leader.persistConfirmedRegistrations(context.Background(), "repo", statuses, map[string]*upstream.RunnerScaleSet{
		"linux": {ID: 1, Name: "linux"},
	})
	if err == nil {
		t.Fatal("registration persistence failure was ignored")
	}
	leader.stop(context.Background())
}

func TestPersistedRegistrationDeletionUsesActiveControllerEpoch(t *testing.T) {
	store := &faultRepositoryStateStore{fakeRepositoryStateStore: &fakeRepositoryStateStore{}, deleteErr: errors.New("failed")}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	err := leader.deletePersistedRegistration(context.Background(), repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"})
	if err == nil {
		t.Fatal("registration deletion failure was ignored")
	}
	leader.stop(context.Background())
}

func TestConfirmedRegistrationPersistenceRemovesOnlyConfiguredOmissions(t *testing.T) {
	store := &fakeRepositoryStateStore{snapshot: repositorystate.Snapshot{
		Registrations: []repositorystate.Registration{
			{RepositoryURL: "repo", ScaleSetName: "obsolete", GitHubScaleSetID: 1},
			{RepositoryURL: "other", ScaleSetName: "retained", GitHubScaleSetID: 2},
		},
	}}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	err := leader.persistConfirmedRegistrations(
		context.Background(), "repo",
		[]cluster.ScaleSetStatus{{Name: "current", RunnerGroup: "Default"}},
		map[string]*upstream.RunnerScaleSet{"current": {ID: 3, Name: "current"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSnapshot(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	assertConfirmedRegistrationSet(t, snapshot)
	leader.stop(context.Background())
}

func assertConfirmedRegistrationSet(t *testing.T, snapshot repositorystate.Snapshot) {
	t.Helper()
	registrations := map[repositoryScaleSetSessionKey]int{}
	for _, registration := range snapshot.Registrations {
		registrations[repositoryScaleSetSessionKey{
			repository: registration.RepositoryURL, scaleSet: registration.ScaleSetName,
		}] = registration.GitHubScaleSetID
	}
	if len(registrations) != 2 ||
		registrations[repositoryScaleSetSessionKey{repository: "repo", scaleSet: "current"}] != 3 ||
		registrations[repositoryScaleSetSessionKey{repository: "other", scaleSet: "retained"}] != 2 {
		t.Fatalf("registrations = %#v, want current repository declaration and untouched other repository", registrations)
	}
}

func TestConfirmedRegistrationPersistenceReportsStaleDeletionFailure(t *testing.T) {
	store := &faultRepositoryStateStore{
		fakeRepositoryStateStore: &fakeRepositoryStateStore{snapshot: repositorystate.Snapshot{
			Registrations: []repositorystate.Registration{{RepositoryURL: "repo", ScaleSetName: "obsolete"}},
		}},
		deleteErr: errors.New("failed"),
	}
	leader, _ := durableRepositoryTestLeader(t, store, staticDiscoverer{})
	if !leader.startRepositoryController(context.Background()) {
		t.Fatal("repository controller did not start")
	}
	if err := leader.persistConfirmedRegistrations(context.Background(), "repo", nil, nil); err == nil {
		t.Fatal("stale registration deletion failure was ignored")
	}
	leader.stop(context.Background())
}

func TestRepositoryActivationRejectsObsoleteDesiredState(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, nil)
	repository := githubapp.Repository{InstallationID: 1, URL: "repo"}
	status := cluster.ScaleSetStatus{Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1}
	client := successfulRepositoryRepairClient()
	messageClient := &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	client.messageSessionClient = func(context.Context, int, string) (scaleSetMessageClient, error) {
		leader.publishDesiredRepositories(map[string]githubapp.Repository{}, map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{})
		return scaleSetMessageClient{Client: messageClient}, nil
	}
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return client, nil
	}
	leader.publishDesiredRepositories(map[string]githubapp.Repository{repository.URL: repository}, map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		{repository: repository.URL, scaleSet: status.Name}: status,
	})
	if err := leader.reconcileRepositorySessions(context.Background(), repository, []cluster.ScaleSetStatus{status}); !errors.Is(err, errRepositoryWorkObsolete) {
		t.Fatalf("obsolete reconciliation error = %v", err)
	}
	leader.stop(context.Background())
}

func TestScaleSetStatusComparisonRejectsDifferentLengthAndValues(t *testing.T) {
	status := cluster.ScaleSetStatus{Name: "linux"}
	if sameScaleSetStatuses(nil, []cluster.ScaleSetStatus{status}) {
		t.Fatal("different status counts matched")
	}
	if !sameScaleSetStatuses([]cluster.ScaleSetStatus{status}, []cluster.ScaleSetStatus{status}) {
		t.Fatal("identical statuses did not match")
	}
	if sameScaleSetStatuses([]cluster.ScaleSetStatus{status}, []cluster.ScaleSetStatus{{Name: "windows"}}) {
		t.Fatal("different statuses matched")
	}
}

func TestDesiredRepositoryGenerationIsStableUntilIdentityOrStatusChanges(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	repository := githubapp.Repository{InstallationID: 1, URL: "repo"}
	status := cluster.ScaleSetStatus{Name: "linux", EffectiveCapacity: 1}
	key := repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name}
	publish := func(repository githubapp.Repository, status cluster.ScaleSetStatus) desiredRepositoryGeneration {
		leader.publishDesiredRepositories(
			map[string]githubapp.Repository{repository.URL: repository},
			map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{key: status},
		)
		desired, _ := leader.currentDesiredRepository(repository.URL)
		return desired
	}

	first := publish(repository, status)
	unchanged := publish(repository, status)
	status.EffectiveCapacity = 2
	changedStatus := publish(repository, status)
	repository.InstallationID = 2
	changedIdentity := publish(repository, status)

	if first.generation != unchanged.generation {
		t.Fatalf("unchanged generation advanced from %d to %d", first.generation, unchanged.generation)
	}
	if changedStatus.generation <= unchanged.generation || changedIdentity.generation <= changedStatus.generation {
		t.Fatalf("generations did not advance across changes: %d, %d, %d",
			unchanged.generation, changedStatus.generation, changedIdentity.generation)
	}
	leader.stop(context.Background())
}

func TestDesiredRepositoryCompletenessAuditQueuesMissingGenerationRepair(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	repository := githubapp.Repository{InstallationID: 1, URL: "repo"}
	status := cluster.ScaleSetStatus{Name: "linux", EffectiveCapacity: 1}
	key := repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name}
	leader.publishDesiredRepositories(
		map[string]githubapp.Repository{repository.URL: repository},
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{key: status},
	)

	leader.auditDesiredRepositoryCompleteness(context.Background())

	repair, pending := leader.pendingRepositoryRepairs[repository.URL]
	desired, _ := leader.currentDesiredRepository(repository.URL)
	if !pending || repair.generation != desired.generation {
		t.Fatalf("pending repair = %#v, want generation %d", repair, desired.generation)
	}
	leader.stop(context.Background())
}

func TestRepositorySchedulingIgnoresRepositoryWithoutDesiredScaleSets(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	leader.scheduleMissingSessions(
		context.Background(),
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{},
		map[string]githubapp.Repository{"repo": {URL: "repo"}},
	)
	leader.repositoryWorkers.mutex.Lock()
	pendingCount := len(leader.repositoryWorkers.pending)
	leader.repositoryWorkers.mutex.Unlock()
	if pendingCount != 0 {
		t.Fatalf("scheduled repository work = %d, want none", pendingCount)
	}
	leader.stop(context.Background())
}

func TestRetainedRepairAdoptsChangedDesiredGenerationImmediately(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	now := time.Unix(200, 0).UTC()
	leader.now = func() time.Time { return now }
	repository := githubapp.Repository{InstallationID: 2, URL: "repo"}
	status := cluster.ScaleSetStatus{Name: "linux", EffectiveCapacity: 2}
	key := repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name}
	leader.pendingRepositoryRepairs[repository.URL] = repositoryRepair{
		repository: githubapp.Repository{InstallationID: 1, URL: repository.URL},
		statuses:   []cluster.ScaleSetStatus{{Name: "linux", EffectiveCapacity: 1}},
		generation: 1, attempts: 3, retryAfter: now.Add(time.Minute),
	}
	leader.publishDesiredRepositories(
		map[string]githubapp.Repository{repository.URL: repository},
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{key: status},
	)
	desired, _ := leader.currentDesiredRepository(repository.URL)
	leader.retainDesiredRepositoryRepairs(
		map[string]githubapp.Repository{repository.URL: repository},
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{key: status},
	)

	repair := leader.pendingRepositoryRepairs[repository.URL]
	if repair.generation != desired.generation || repair.repository != repository || repair.attempts != 3 ||
		!repair.retryAfter.Equal(now) {
		t.Fatalf("retained changed repair = %#v, want immediate current generation", repair)
	}
	leader.stop(context.Background())
}

func TestLegacyRepositoryReconciliationDoesNotQueueRepairAfterCancellation(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	handledErr := leader.handleMissingSessionResult(
		canceledCtx,
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{},
		repositorySessionResult{repository: githubapp.Repository{URL: "repo"}, err: errors.New("failed")},
	)
	if handledErr == nil {
		t.Fatal("canceled result returned success")
	}
	leader.repairMutex.Lock()
	pendingCount := len(leader.pendingRepositoryRepairs)
	leader.repairMutex.Unlock()
	if pendingCount != 0 {
		t.Fatalf("pending repairs after cancellation = %d, want none", pendingCount)
	}
	leader.stop(context.Background())
}

func TestScheduledRepositoryFailureBecomesPendingRepair(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, nil)
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("failed")
	}
	repository := githubapp.Repository{URL: "repo"}
	status := cluster.ScaleSetStatus{Name: "linux", RunnerGroup: "Default", Compatible: true}
	leader.scheduleMissingSessions(
		context.Background(),
		map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{{repository: "repo", scaleSet: "linux"}: status},
		map[string]githubapp.Repository{"repo": repository},
	)
	awaitRepositoryRepairCount(t, leader)
	leader.stop(context.Background())
}

type faultRepositoryStateStore struct {
	*fakeRepositoryStateStore
	startErr  error
	applyErr  error
	upsertErr error
	deleteErr error
}

func (store *faultRepositoryStateStore) StartController(ctx context.Context, appID int64) (repositorystate.Snapshot, error) {
	if store.startErr != nil {
		return repositorystate.Snapshot{}, store.startErr
	}
	return store.fakeRepositoryStateStore.StartController(ctx, appID)
}

func (store *faultRepositoryStateStore) ApplyDiscovery(
	ctx context.Context,
	appID int64,
	epoch int64,
	installations []githubapp.InstallationID,
	failures []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) (repositorystate.Snapshot, error) {
	if store.applyErr != nil {
		return repositorystate.Snapshot{}, store.applyErr
	}
	return store.fakeRepositoryStateStore.ApplyDiscovery(
		ctx, appID, epoch, installations, failures, repositories, completedAt,
	)
}

func (store *faultRepositoryStateStore) UpsertRegistration(
	ctx context.Context,
	appID int64,
	epoch int64,
	registration repositorystate.Registration,
) error {
	if store.upsertErr != nil {
		return store.upsertErr
	}
	return store.fakeRepositoryStateStore.UpsertRegistration(ctx, appID, epoch, registration)
}

func (store *faultRepositoryStateStore) DeleteRegistration(
	ctx context.Context,
	appID int64,
	epoch int64,
	repository string,
	scaleSet string,
) error {
	if store.deleteErr != nil {
		return store.deleteErr
	}
	return store.fakeRepositoryStateStore.DeleteRegistration(ctx, appID, epoch, repository, scaleSet)
}
