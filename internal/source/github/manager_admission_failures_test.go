package github_test

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestRegistrationSessionCloseReportsBoundaryFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	record := managerRecord("https://github.com/octo/repo", 11)
	managed := &managerSessionFake{closeErr: wantErr}
	session := openRegistrationSession(t, registrationSessionManager(t, record, managed), record)

	if err := session.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want %v", err, wantErr)
	}
}

func TestRegistrationSessionCloseTreatsAlreadyAbsentSessionAsClosed(t *testing.T) {
	t.Parallel()
	for _, closeErr := range []error{upstream.BadRequestError, upstream.NotFoundError} {
		record := managerRecord("https://github.com/octo/repo", 11)
		managed := &managerSessionFake{closeErr: closeErr}
		session := openRegistrationSession(t, registrationSessionManager(t, record, managed), record)
		if err := session.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v, want already-absent success", err)
		}
	}
}

func TestOpenRegistrationSessionRejectsInvalidRequestsBeforeGitHub(t *testing.T) {
	t.Parallel()

	valid := managerRecord("https://github.com/octo/repo", 11)
	incomplete := valid
	incomplete.RepositoryURL = ""
	tests := []struct {
		name   string
		record githubsource.Registration
		config githubsource.RunnerPoolConfig
		owner  string
	}{
		{name: "incomplete identity", record: incomplete, config: configuredRunnerPool(), owner: "member-1"},
		{name: "missing owner", record: valid, config: configuredRunnerPool()},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t),
				&registrationRepositoryFake{})
			if _, err := manager.OpenRegistrationSession(context.Background(), testCase.record, testCase.config,
				testCase.owner, &recordingEventHandler{}); err == nil {
				t.Fatal("OpenRegistrationSession() accepted an invalid request")
			}
		})
	}
}

func TestOpenRegistrationSessionRetainsStateAfterInconclusiveFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("GitHub unavailable")
	record := managerRecord("https://github.com/octo/repo", 11)
	for _, testCase := range inconclusiveSessionFactories(record, wantErr) {
		t.Run(testCase.name, func(t *testing.T) {
			repository := &registrationRepositoryFake{
				records: []githubsource.Registration{record}, owned: []githubsource.Registration{record},
			}
			manager := newManagerFixture(t, &managerDiscovererFake{}, testCase.factory, repository)
			session, err := manager.OpenRegistrationSession(context.Background(), record, configuredRunnerPool(),
				"member-1", &recordingEventHandler{})
			if err == nil || errors.Is(err, githubsource.ErrRegistrationNotAdmitted) || session != nil {
				t.Fatalf("OpenRegistrationSession() = (%v, %v), want inconclusive failure", session, err)
			}
			assertExactRegistrationState(t, repository.records, repository.owned, record)
		})
	}
}

type namedManagerFactory struct {
	name    string
	factory githubsource.ManagerClientFactoryFunc
}

func inconclusiveSessionFactories(record githubsource.Registration, wantErr error) []namedManagerFactory {
	return []namedManagerFactory{
		{name: "factory", factory: func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
			return githubsource.ManagerClient{}, wantErr
		}},
		{name: "missing resolver", factory: fixedManagerClientFactory(githubsource.ManagerClient{})},
		{name: "resolver", factory: fixedManagerClientFactory(githubsource.ManagerClient{
			ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) { return 0, wantErr },
		})},
		{name: "session", factory: fixedManagerClientFactory(admittedManagerClient(record, nil, wantErr))},
		{name: "nil session", factory: fixedManagerClientFactory(admittedManagerClient(record, nil, nil))},
	}
}

func TestOpenRegistrationSessionReportsTerminalCleanupFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("delete failed")
	record := managerRecord("https://github.com/octo/repo", 11)
	factories := []githubsource.ManagerClientFactoryFunc{
		func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
			return githubsource.ManagerClient{}, upstream.NotFoundError
		},
		fixedManagerClientFactory(admittedManagerClient(record, nil, upstream.NotFoundError)),
	}
	for _, factory := range factories {
		repository := &registrationRepositoryFake{
			records: []githubsource.Registration{record}, owned: []githubsource.Registration{record}, deleteErr: wantErr,
		}
		manager := newManagerFixture(t, &managerDiscovererFake{}, factory, repository)
		if _, err := manager.OpenRegistrationSession(context.Background(), record, configuredRunnerPool(),
			"member-1", &recordingEventHandler{}); !errors.Is(err, wantErr) {
			t.Fatalf("OpenRegistrationSession() error = %v, want %v", err, wantErr)
		}
	}
}

func TestOpenRegistrationSessionForgetsTerminallyMissingRepository(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	repository := &registrationRepositoryFake{
		records: []githubsource.Registration{record}, owned: []githubsource.Registration{record},
	}
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context, transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{}, upstream.NotFoundError
	}, repository)

	session, err := manager.OpenRegistrationSession(context.Background(), record, configuredRunnerPool(),
		"member-1", &recordingEventHandler{})
	assertRegistrationNotAdmitted(t, session, err)
	if len(repository.records) != 0 || len(repository.owned) != 0 {
		t.Fatalf("registration state = active %#v owned %#v, want empty", repository.records, repository.owned)
	}
}

func TestOpenRegistrationSessionReportsStaleActiveDeletionFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("delete failed")
	record := managerRecord("https://github.com/octo/repo", 11)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}, deleteErr: wantErr}
	client := githubsource.ManagerClient{ResolveRunnerGroup: func(
		context.Context, string,
	) (transport.RunnerGroupID, error) {
		return record.RunnerGroupID + 1, nil
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, fixedManagerClientFactory(client), repository)

	if _, err := manager.OpenRegistrationSession(context.Background(), record, configuredRunnerPool(),
		"member-1", &recordingEventHandler{}); !errors.Is(err, wantErr) {
		t.Fatalf("OpenRegistrationSession() error = %v, want %v", err, wantErr)
	}
}

func TestRefreshMembersRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	manager := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t),
		&registrationRepositoryFake{})
	if err := manager.RefreshMembers(context.Background(), githubsource.ManagerConfig{
		RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux"}},
	}); err == nil {
		t.Fatal("RefreshMembers() accepted an invalid configuration")
	}
}

func TestRefreshMembersUsesCommittedRegistrationWithoutRunnerGroupLookup(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	resolverCalls := 0
	client := githubsource.ManagerClient{ResolveRunnerGroup: func(
		context.Context, string,
	) (transport.RunnerGroupID, error) {
		resolverCalls++
		return record.RunnerGroupID + 1, nil
	}}
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, fixedManagerClientFactory(client), repository)
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("RefreshMembers() error = %v", err)
	}
	if resolverCalls != 0 {
		t.Fatalf("runner-group resolver calls = %d, want zero", resolverCalls)
	}
}

func TestRefreshMembersHandlesTerminalAndCommittedRoutesIndependently(t *testing.T) {
	t.Parallel()

	terminal := managerRecord("https://github.com/octo/missing", 11)
	stale := managerRecord("https://github.com/octo/stale", 12)
	repository := &registrationRepositoryFake{
		records: []githubsource.Registration{terminal, stale},
		owned:   []githubsource.Registration{terminal, stale},
	}
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		_ context.Context, target transport.Repository,
	) (githubsource.ManagerClient, error) {
		if target.URL == terminal.RepositoryURL {
			return githubsource.ManagerClient{}, upstream.NotFoundError
		}
		return githubsource.ManagerClient{GenerateBootstrap: func(
			context.Context, transport.Registration, string,
		) ([]byte, error) {
			return []byte("opaque"), nil
		}}, nil
	}, repository)

	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("RefreshMembers() error = %v", err)
	}
	assertOnlyRegistration(t, repository, stale)
	assertBootstrapAvailable(t, manager, stale)
}

func TestRefreshMembersRetainsEstablishedRoutesUntilReplacementIsComplete(t *testing.T) {
	t.Parallel()

	established := managerRecord("https://github.com/octo/established", 11)
	replacement := managerRecord("https://github.com/octo/replacement", 12)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{established}}
	wantErr := errors.New("repository client unavailable")
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		_ context.Context,
		target transport.Repository,
	) (githubsource.ManagerClient, error) {
		if target.URL == replacement.RepositoryURL {
			return githubsource.ManagerClient{}, wantErr
		}
		return githubsource.ManagerClient{GenerateBootstrap: func(
			context.Context, transport.Registration, string,
		) ([]byte, error) {
			return []byte("opaque"), nil
		}}, nil
	}, repository)

	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("initial RefreshMembers() error = %v", err)
	}
	repository.records = []githubsource.Registration{replacement}
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); !errors.Is(err, wantErr) {
		t.Fatalf("replacement RefreshMembers() error = %v, want %v", err, wantErr)
	}
	assertBootstrapAvailable(t, manager, established)
}

func assertOnlyRegistration(
	t *testing.T,
	repository *registrationRepositoryFake,
	want githubsource.Registration,
) {
	t.Helper()
	if len(repository.records) != 1 || repository.records[0] != want ||
		len(repository.owned) != 1 || repository.owned[0] != want {
		t.Fatalf("registration state = active %#v owned %#v", repository.records, repository.owned)
	}
}

func assertBootstrapAvailable(t *testing.T, manager *githubsource.Manager, registration githubsource.Registration) {
	t.Helper()
	job := sourceJob(t, registration.RepositoryURL, registration.RunnerPool, 61)
	if _, err := manager.Source().GenerateBootstrap(context.Background(), job, "runner-61"); err != nil {
		t.Fatalf("generate bootstrap from committed route: %v", err)
	}
}

func TestRefreshMembersReportsTerminalCleanupAndRouteReplacementFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("delete failed")
	record := managerRecord("https://github.com/octo/repo", 11)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}, deleteErr: wantErr}
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context, transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{}, upstream.NotFoundError
	}, repository)
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); !errors.Is(err, wantErr) {
		t.Fatalf("terminal cleanup error = %v, want %v", err, wantErr)
	}

	invalid := record
	invalid.ScaleSetID = 0
	repository = &registrationRepositoryFake{records: []githubsource.Registration{invalid}}
	client := admittedManagerClient(invalid, &managerSessionFake{}, nil)
	manager = newManagerFixture(t, &managerDiscovererFake{}, fixedManagerClientFactory(client), repository)
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err == nil {
		t.Fatal("RefreshMembers() accepted an invalid member route")
	}
}

func configuredRunnerPool() githubsource.RunnerPoolConfig {
	return githubsource.RunnerPoolConfig{RunnerPool: "linux", RunnerGroup: "default"}
}

func fixedManagerClientFactory(client githubsource.ManagerClient) githubsource.ManagerClientFactoryFunc {
	return func(context.Context, transport.Repository) (githubsource.ManagerClient, error) { return client, nil }
}

func admittedManagerClient(
	record githubsource.Registration,
	session githubsource.ManagedSession,
	openErr error,
) githubsource.ManagerClient {
	return githubsource.ManagerClient{
		ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) {
			return record.RunnerGroupID, nil
		},
		OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
			return session, openErr
		},
	}
}

func failOnManagerFactoryCall(t *testing.T) githubsource.ManagerClientFactoryFunc {
	t.Helper()
	return func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		t.Fatal("GitHub factory was called")
		return githubsource.ManagerClient{}, nil
	}
}
