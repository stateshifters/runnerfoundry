package github_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestManagerCopiesInitialSessionObservationToAdapter(t *testing.T) {
	t.Parallel()

	sessionID := uuid.MustParse("51d707c7-e6fa-4339-99cd-a21b29313811")
	statistics := &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 4}
	session := &sessionWithInitialStatistics{
		managerSessionFake: &managerSessionFake{}, sessionID: sessionID, statistics: statistics,
	}
	records := repositorySessionRecords()[:1]
	manager := newManagerFixture(t, &managerDiscovererFake{},
		repositorySessionFactory([]githubsource.ManagedSession{session}, 0, nil), &registrationRepositoryFake{})
	generation, err := manager.OpenRepositorySessions(
		context.Background(), records, "member-1", &recordingEventHandler{},
	)
	if err != nil {
		t.Fatalf("OpenRepositorySessions(): %v", err)
	}
	statistics.TotalAssignedJobs = 9
	adapter := generation.Adapters()[0]
	if adapter.InitialSessionID != sessionID || adapter.InitialStatistics == nil ||
		adapter.InitialStatistics.TotalAssignedJobs != 4 {
		t.Fatalf("adapter initial observation = (%s, %#v), want (%s, 4 assigned)",
			adapter.InitialSessionID, adapter.InitialStatistics, sessionID)
	}
}

func TestManagerRetainsMissingInitialStatisticsAsPendingProtocolState(t *testing.T) {
	t.Parallel()

	sessionID := uuid.MustParse("61d707c7-e6fa-4339-99cd-a21b29313812")
	session := &sessionWithInitialStatistics{managerSessionFake: &managerSessionFake{}, sessionID: sessionID}
	manager := newManagerFixture(t, &managerDiscovererFake{},
		repositorySessionFactory([]githubsource.ManagedSession{session}, 0, nil), &registrationRepositoryFake{})
	generation, err := manager.OpenRepositorySessions(context.Background(), repositorySessionRecords()[:1],
		"member-1", &recordingEventHandler{})
	if err != nil {
		t.Fatalf("OpenRepositorySessions(): %v", err)
	}
	adapter := generation.Adapters()[0]
	if adapter.InitialSessionID != sessionID || adapter.InitialStatistics != nil || !adapter.InitialStatisticsPending {
		t.Fatalf("adapter initial protocol state = (%s, %#v, %t)",
			adapter.InitialSessionID, adapter.InitialStatistics, adapter.InitialStatisticsPending)
	}
}

type sessionWithInitialStatistics struct {
	*managerSessionFake
	sessionID  uuid.UUID
	statistics *upstream.RunnerScaleSetStatistic
}

func (session *sessionWithInitialStatistics) SessionID() uuid.UUID { return session.sessionID }

func (session *sessionWithInitialStatistics) InitialStatistics() *upstream.RunnerScaleSetStatistic {
	return session.statistics
}

func TestManagerOpensRepositorySessionsAsAtomicGeneration(t *testing.T) {
	records := repositorySessionRecords()
	first := &managerSessionFake{}
	second := &managerSessionFake{}
	factory := repositorySessionFactory([]githubsource.ManagedSession{first, second}, 0, nil)
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory, &registrationRepositoryFake{})

	generation, err := manager.OpenRepositorySessions(
		context.Background(), records, "member-1", &recordingEventHandler{events: &[]string{}},
	)
	if err != nil {
		t.Fatalf("OpenRepositorySessions() error = %v", err)
	}
	assertRepositoryGenerationViews(t, generation, records)
	assertRepositoryGenerationCloses(t, generation, first, second)
}

func assertRepositoryGenerationViews(
	t *testing.T,
	generation *githubsource.RepositorySessionGeneration,
	records []githubsource.Registration,
) {
	t.Helper()
	if generation.Repository() != records[0].RepositoryURL ||
		!reflect.DeepEqual(generation.Registrations(), records) || len(generation.Adapters()) != 2 {
		t.Fatalf("generation = repository %q registrations %#v adapters %d",
			generation.Repository(), generation.Registrations(), len(generation.Adapters()))
	}
	returned := generation.Registrations()
	returned[0] = githubsource.Registration{}
	if !reflect.DeepEqual(generation.Registrations(), records) {
		t.Fatal("Registrations() exposed mutable generation state")
	}
	adapters := generation.Adapters()
	adapters[0] = nil
	if generation.Adapters()[0] == nil {
		t.Fatal("Adapters() exposed mutable generation state")
	}
}

func assertRepositoryGenerationCloses(
	t *testing.T,
	generation *githubsource.RepositorySessionGeneration,
	sessions ...*managerSessionFake,
) {
	t.Helper()
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	for _, session := range sessions {
		if !session.closed {
			t.Fatal("Close() did not close every repository session")
		}
	}
}

func TestManagerClosesPartialRepositoryGenerationWhenOpeningFails(t *testing.T) {
	records := repositorySessionRecords()
	wantErr := errors.New("open failed")
	first := &managerSessionFake{}
	factory := repositorySessionFactory([]githubsource.ManagedSession{first}, 2, wantErr)
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory, &registrationRepositoryFake{})

	generation, err := manager.OpenRepositorySessions(
		context.Background(), records, "member-1", &recordingEventHandler{events: &[]string{}},
	)
	if generation != nil || !errors.Is(err, wantErr) {
		t.Fatalf("OpenRepositorySessions() = (%v, %v), want nil generation and %v", generation, err, wantErr)
	}
	if !first.closed {
		t.Fatal("partial repository generation session was not closed")
	}
}

func TestManagerReturnsPartialRepositoryGenerationWhenCleanupFails(t *testing.T) {
	records := repositorySessionRecords()
	openErr := errors.New("open failed")
	closeErr := errors.New("close failed")
	first := &managerSessionFake{closeErr: closeErr}
	factory := repositorySessionFactory([]githubsource.ManagedSession{first}, 2, openErr)
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory, &registrationRepositoryFake{})

	generation, err := manager.OpenRepositorySessions(
		context.Background(), records, "member-1", &recordingEventHandler{events: &[]string{}},
	)
	if generation == nil || !errors.Is(err, openErr) || !errors.Is(err, closeErr) {
		t.Fatalf("OpenRepositorySessions() = (%v, %v), want owned partial generation and both failures", generation, err)
	}
	first.closeErr = nil
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("retry partial generation cleanup: %v", err)
	}
	if first.closes != 2 {
		t.Fatalf("partial session close attempts = %d, want 2", first.closes)
	}
}

func TestManagerRejectsInvalidRepositorySessionRequests(t *testing.T) {
	valid := repositorySessionRecords()[:1]
	handler := &recordingEventHandler{events: &[]string{}}
	requests := []struct {
		name    string
		records []githubsource.Registration
		owner   string
		handler githubsource.EventHandler
	}{
		{name: "empty records", owner: "member-1", handler: handler},
		{name: "empty owner", records: valid, handler: handler},
		{name: "nil handler", records: valid, owner: "member-1"},
		{name: "empty repository", records: []githubsource.Registration{{}}, owner: "member-1", handler: handler},
		{name: "mixed repositories", records: repositorySessionRecordsWithMismatch(), owner: "member-1", handler: handler},
	}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client,
		&registrationRepositoryFake{})
	for _, request := range requests {
		if _, err := manager.OpenRepositorySessions(
			context.Background(), request.records, request.owner, request.handler,
		); err == nil {
			t.Fatalf("%s request unexpectedly succeeded", request.name)
		}
	}
}

func TestManagerReportsRepositorySessionFactoryAndNilSessionFailures(t *testing.T) {
	records := repositorySessionRecords()[:1]
	wantErr := errors.New("session boundary failed")
	factory := newManagerFactoryFake(nil)
	factory.err = wantErr
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory.Client, &registrationRepositoryFake{})
	if _, err := manager.OpenRepositorySessions(
		context.Background(), records, "member-1", &recordingEventHandler{events: &[]string{}},
	); !errors.Is(err, wantErr) {
		t.Fatalf("factory error = %v, want %v", err, wantErr)
	}

	factory = newManagerFactoryFake([]transport.Repository{managerTransportRepository(records[0])})
	manager = newManagerFixture(t, &managerDiscovererFake{}, factory.Client, &registrationRepositoryFake{})
	if _, err := manager.OpenRepositorySessions(
		context.Background(), records, "member-1", &recordingEventHandler{events: &[]string{}},
	); err == nil {
		t.Fatal("nil session unexpectedly succeeded")
	}
}

func repositorySessionRecords() []githubsource.Registration {
	repositoryURL := "https://github.com/octo/repo"
	records := []githubsource.Registration{managerRecord(repositoryURL, 11), managerRecord(repositoryURL, 12)}
	records[1].RunnerPool = "windows"
	return records
}

func repositorySessionRecordsWithMismatch() []githubsource.Registration {
	records := repositorySessionRecords()
	records[1].RepositoryURL += "-other"
	return records
}

func repositorySessionFactory(
	sessions []githubsource.ManagedSession,
	failAt int,
	openErr error,
) githubsource.ManagerClientFactoryFunc {
	opened := 0
	return func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{OpenSession: func(
			context.Context,
			transport.Registration,
			string,
		) (githubsource.ManagedSession, error) {
			opened++
			if opened == failAt {
				return nil, openErr
			}
			return sessions[opened-1], nil
		}}, nil
	}
}
