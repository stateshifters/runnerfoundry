package github_test

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestActiveRegistrationsReadsActiveStateIndependently(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client,
		&registrationRepositoryFake{records: []githubsource.Registration{record}, ownedErr: errors.New("owned unavailable")})

	registrations, err := manager.ActiveRegistrations(context.Background())
	if err != nil {
		t.Fatalf("ActiveRegistrations() error = %v", err)
	}
	if len(registrations) != 1 || registrations[0] != record {
		t.Fatalf("ActiveRegistrations() = %#v, want %#v", registrations, record)
	}
}

func TestReconcileDoesNotReadOwnershipWhenPruningIsDisabled(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}, ownedErr: errors.New("owned unavailable")}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)

	active, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("Reconcile() active = %#v, want none after convergence", active)
	}
}

func TestRefreshMembersRejectsUnconfiguredRecordsWithoutGitHubCall(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	factoryCalls := 0
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		factoryCalls++
		return githubsource.ManagerClient{}, nil
	}, &registrationRepositoryFake{records: []githubsource.Registration{record}})

	if err := manager.RefreshMembers(context.Background(), githubsource.ManagerConfig{}); err != nil {
		t.Fatalf("RefreshMembers() error = %v", err)
	}
	if factoryCalls != 0 {
		t.Fatalf("GitHub factory calls = %d, want 0", factoryCalls)
	}
}

func TestReconcileReturnsActiveWithPruneFailureWhenOwnershipReadFails(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("owned unavailable")
	repositoryURL := "https://github.com/octo/repo"
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: repositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	repository := &registrationRepositoryFake{ownedErr: wantErr}
	manager := newManagerFixture(t, discoverer, factory.Client, repository)

	active, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{
		Prune:       true,
		RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}},
	})
	if len(active) != 1 || active[0].RepositoryURL != repositoryURL {
		t.Fatalf("Reconcile() active = %#v, want converged registration", active)
	}
	if !errors.Is(err, wantErr) || !githubsource.IsPruneFailure(err) {
		t.Fatalf("Reconcile() error = %v, want PruneFailure wrapping %v", err, wantErr)
	}
}

func TestOpenRegistrationSessionForgetsOnlyTerminallyMissingRegistration(t *testing.T) {
	t.Parallel()

	record, sibling := registrationPair()
	repository := &registrationRepositoryFake{
		records: []githubsource.Registration{record, sibling},
		owned:   []githubsource.Registration{record, sibling},
	}
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{
			ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) {
				return record.RunnerGroupID, nil
			},
			OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
				return nil, upstream.NotFoundError
			},
		}, nil
	}, repository)

	session, err := manager.OpenRegistrationSession(context.Background(), record,
		githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool, RunnerGroup: "default"},
		"member-1", &recordingEventHandler{})
	assertRegistrationNotAdmitted(t, session, err)
	assertExactRegistrationState(t, repository.records, repository.owned, sibling)
}

func TestOpenRegistrationSessionExposesAdmittedRouteAndAdapter(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	managed := &managerSessionFake{}
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{
			ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) {
				return record.RunnerGroupID, nil
			},
			OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
				return managed, nil
			},
		}, nil
	}, &registrationRepositoryFake{})

	session, err := manager.OpenRegistrationSession(context.Background(), record,
		githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool, RunnerGroup: "default"},
		"member-1", &recordingEventHandler{})
	if err != nil {
		t.Fatalf("OpenRegistrationSession() error = %v", err)
	}
	if session == nil || session.Registration() != record || session.Adapter() == nil {
		t.Fatalf("RegistrationSession = %#v, want admitted route and adapter", session)
	}
	if err = session.Close(context.Background()); err != nil || managed.closes != 1 {
		t.Fatalf("Close() = %v, attempts = %d", err, managed.closes)
	}
}

func TestOpenRegistrationSessionCopiesInitialObservationToAdapter(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	sessionID := uuid.MustParse("51d707c7-e6fa-4339-99cd-a21b29313811")
	statistics := &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 4}
	managed := &sessionWithInitialStatistics{
		managerSessionFake: &managerSessionFake{}, sessionID: sessionID, statistics: statistics,
	}
	manager := registrationSessionManager(t, record, managed)

	session := openRegistrationSession(t, manager, record)
	statistics.TotalAssignedJobs = 9
	adapter := session.Adapter()
	if adapter.InitialSessionID != sessionID || adapter.InitialStatistics == nil ||
		adapter.InitialStatistics.TotalAssignedJobs != 4 {
		t.Fatalf("adapter initial observation = (%s, %#v), want (%s, 4 assigned)",
			adapter.InitialSessionID, adapter.InitialStatistics, sessionID)
	}
}

func TestOpenRegistrationSessionRetainsMissingInitialStatisticsAsPending(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 11)
	sessionID := uuid.MustParse("61d707c7-e6fa-4339-99cd-a21b29313812")
	managed := &sessionWithInitialStatistics{managerSessionFake: &managerSessionFake{}, sessionID: sessionID}
	manager := registrationSessionManager(t, record, managed)

	adapter := openRegistrationSession(t, manager, record).Adapter()
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

func TestOpenRegistrationSessionRestoresCommittedRecordWithoutLocalConfiguration(t *testing.T) {
	t.Parallel()

	factoryCalls := 0
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		factoryCalls++
		return githubsource.ManagerClient{OpenSession: func(
			context.Context, transport.Registration, string,
		) (githubsource.ManagedSession, error) {
			return &managerSessionFake{}, nil
		}}, nil
	}, &registrationRepositoryFake{})
	record := managerRecord("https://github.com/octo/repo", 11)

	session, err := manager.OpenRegistrationSession(context.Background(), record,
		githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool},
		"member-1", &recordingEventHandler{})
	if err != nil || session == nil {
		t.Fatalf("OpenRegistrationSession() = (%v, %v), want restored session", session, err)
	}
	if factoryCalls != 1 {
		t.Fatalf("GitHub factory calls = %d, want 1", factoryCalls)
	}
}

func TestOpenRegistrationSessionRetiresOnlyActiveRouteOnRunnerGroupMismatch(t *testing.T) {
	t.Parallel()

	record, sibling := registrationPair()
	repository := &registrationRepositoryFake{
		records: []githubsource.Registration{record, sibling},
		owned:   []githubsource.Registration{record, sibling},
	}
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{ResolveRunnerGroup: func(
			context.Context, string,
		) (transport.RunnerGroupID, error) {
			return record.RunnerGroupID + 1, nil
		}}, nil
	}, repository)

	session, err := manager.OpenRegistrationSession(context.Background(), record,
		githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool, RunnerGroup: "default"},
		"member-1", &recordingEventHandler{})
	assertRegistrationNotAdmitted(t, session, err)
	if len(repository.records) != 1 || repository.records[0] != sibling {
		t.Fatalf("active registrations = %#v, want sibling only", repository.records)
	}
	if len(repository.owned) != 2 {
		t.Fatalf("owned registrations = %#v, want ownership retained", repository.owned)
	}
}

func registrationPair() (record githubsource.Registration, sibling githubsource.Registration) {
	record = managerRecord("https://github.com/octo/repo", 11)
	sibling = managerRecord(record.RepositoryURL, 12)
	sibling.RunnerPool = "windows"
	return record, sibling
}

func assertRegistrationNotAdmitted(t *testing.T, session *githubsource.RegistrationSession, err error) {
	t.Helper()
	if !errors.Is(err, githubsource.ErrRegistrationNotAdmitted) || session != nil {
		t.Fatalf("OpenRegistrationSession() = (%v, %v), want not-admitted sentinel", session, err)
	}
}

func assertExactRegistrationState(
	t *testing.T,
	active []githubsource.Registration,
	owned []githubsource.Registration,
	want githubsource.Registration,
) {
	t.Helper()
	if len(active) != 1 || active[0] != want || len(owned) != 1 || owned[0] != want {
		t.Fatalf("registration state = active %#v owned %#v, want %#v only", active, owned, want)
	}
}

func registrationSessionManager(
	t *testing.T,
	record githubsource.Registration,
	managed githubsource.ManagedSession,
) *githubsource.Manager {
	t.Helper()
	return newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{
			ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) {
				return record.RunnerGroupID, nil
			},
			OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
				return managed, nil
			},
		}, nil
	}, &registrationRepositoryFake{})
}

func openRegistrationSession(
	t *testing.T,
	manager *githubsource.Manager,
	record githubsource.Registration,
) *githubsource.RegistrationSession {
	t.Helper()
	session, err := manager.OpenRegistrationSession(context.Background(), record,
		githubsource.RunnerPoolConfig{RunnerPool: record.RunnerPool, RunnerGroup: "default"},
		"member-1", &recordingEventHandler{})
	if err != nil {
		t.Fatalf("OpenRegistrationSession() error = %v", err)
	}
	return session
}
