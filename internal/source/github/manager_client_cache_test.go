package github_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestManagerKeepsRepositoryClientsSeparateAcrossInstallations(t *testing.T) {
	t.Parallel()

	first := managerRecord("https://github.com/octo/repo", 17)
	second := first
	second.InstallationID = 2
	second.RunnerPool = "windows"
	second.ScaleSetID = 18
	repository := &registrationRepositoryFake{records: []githubsource.Registration{first, second}}
	var targetsMutex sync.Mutex
	var targets []transport.Repository
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		_ context.Context,
		target transport.Repository,
	) (githubsource.ManagerClient, error) {
		targetsMutex.Lock()
		targets = append(targets, target)
		targetsMutex.Unlock()
		return githubsource.ManagerClient{}, nil
	}, repository)

	if err := manager.RefreshMembers(context.Background(), twoPoolManagerConfig()); err != nil {
		t.Fatalf("RefreshMembers() error = %v", err)
	}
	if len(targets) != 2 || targets[0].InstallationID == targets[1].InstallationID {
		t.Fatalf("repository client targets = %#v, want distinct installations", targets)
	}
}

func TestManagerRetriesFailedRepositoryClientConstruction(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("authentication unavailable")
	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	factoryCalls := 0
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return githubsource.ManagerClient{}, wantErr
		}
		return githubsource.ManagerClient{}, nil
	}, repository)

	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); !errors.Is(err, wantErr) {
		t.Fatalf("first RefreshMembers() error = %v, want %v", err, wantErr)
	}
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("second RefreshMembers() error = %v", err)
	}
	if factoryCalls != 2 {
		t.Fatalf("repository client constructions = %d, want failed attempt and retry", factoryCalls)
	}
}

func TestManagerRetriesCancelledRepositoryClientConstruction(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	factoryCalls := 0
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return githubsource.ManagerClient{}, context.Canceled
		}
		return githubsource.ManagerClient{}, nil
	}, repository)

	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); !errors.Is(err, context.Canceled) {
		t.Fatalf("first RefreshMembers() error = %v, want %v", err, context.Canceled)
	}
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("second RefreshMembers() error = %v", err)
	}
	if factoryCalls != 2 {
		t.Fatalf("repository client constructions = %d, want canceled attempt and retry", factoryCalls)
	}
}

func TestManagerReusesRepositoryClientAcrossBootstrapAndSessionPaths(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	factoryCalls := 0
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		factoryCalls++
		return admittedManagerClient(record, &managerSessionFake{}, nil), nil
	}, repository)

	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("RefreshMembers() error = %v", err)
	}
	session, err := manager.OpenRegistrationSession(context.Background(), record,
		githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool}, "member", &recordingEventHandler{})
	if err != nil {
		t.Fatalf("OpenRegistrationSession() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := session.Close(context.Background()); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})
	if factoryCalls != 1 {
		t.Fatalf("repository client constructions = %d, want one shared client", factoryCalls)
	}
}

func TestManagerCoalescesConcurrentRepositoryClientConstruction(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	constructionStarted := make(chan struct{}, 2)
	releaseConstruction := make(chan struct{})
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		constructionStarted <- struct{}{}
		<-releaseConstruction
		return githubsource.ManagerClient{}, nil
	}, repository)

	results := make(chan error, 2)
	go func() { results <- manager.RefreshMembers(context.Background(), managerConfigForTests()) }()
	<-constructionStarted
	go func() { results <- manager.RefreshMembers(context.Background(), managerConfigForTests()) }()
	select {
	case <-constructionStarted:
		close(releaseConstruction)
		t.Fatal("concurrent refresh started duplicate repository client construction")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseConstruction)
	for completed := 0; completed < 2; completed++ {
		if err := <-results; err != nil {
			t.Fatalf("RefreshMembers() error = %v", err)
		}
	}
}

func TestManagerLoadsDifferentRepositoryClientsIndependently(t *testing.T) {
	t.Parallel()

	blocked := managerRecord("https://github.com/octo/blocked", 17)
	independent := managerRecord("https://github.com/octo/independent", 18)
	independent.RunnerPool = "windows"
	repository := &registrationRepositoryFake{records: []githubsource.Registration{blocked, independent}}
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	factory := independentlyLoadingClientFactory(blocked, independent, blockedStarted, releaseBlocked)
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory, repository)

	blockedResult := openRegistrationAsync(manager, blocked)
	<-blockedStarted
	independentResult := openRegistrationAsync(manager, independent)
	select {
	case err := <-independentResult:
		if err != nil {
			t.Fatalf("independent OpenRegistrationSession() error = %v", err)
		}
	case <-time.After(time.Second):
		close(releaseBlocked)
		t.Fatal("blocked repository client construction stalled an unrelated repository")
	}
	close(releaseBlocked)
	if err := <-blockedResult; err != nil {
		t.Fatalf("blocked OpenRegistrationSession() error = %v", err)
	}
}

func independentlyLoadingClientFactory(
	blocked githubsource.Registration,
	independent githubsource.Registration,
	blockedStarted chan<- struct{},
	releaseBlocked <-chan struct{},
) githubsource.ManagerClientFactoryFunc {
	return func(_ context.Context, target transport.Repository) (githubsource.ManagerClient, error) {
		record := independent
		if target.URL == blocked.RepositoryURL {
			close(blockedStarted)
			<-releaseBlocked
			record = blocked
		}
		return admittedManagerClient(record, &managerSessionFake{}, nil), nil
	}
}

func openRegistrationAsync(
	manager *githubsource.Manager,
	record githubsource.Registration,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		session, err := manager.OpenRegistrationSession(context.Background(), record,
			githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool}, "member", &recordingEventHandler{})
		if err == nil {
			err = session.Close(context.Background())
		}
		result <- err
	}()
	return result
}

func twoPoolManagerConfig() githubsource.ManagerConfig {
	return githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
		{RunnerPool: "windows", RunnerGroup: "default"},
	}}
}
