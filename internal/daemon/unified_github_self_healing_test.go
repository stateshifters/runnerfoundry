package daemon

import (
	"context"
	"testing"
	"time"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestUnadmittedRegistrationRemovesDesiredRoute(t *testing.T) {
	record := githubsource.Registration{RepositoryURL: "https://github.example/octo/missing", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	source := newGitHubSourceRuntime(&githubManagerFake{}, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	key := registrationKey(record)
	term := branchLeadershipTerm(1)
	source.term = term
	source.leaderInstalled = true
	source.desired[key] = record
	source.sessionActivationActive = true
	source.sessionActivationTotal = 1
	if err := source.installRegistrationSession(context.Background(), term, githubRegistrationOpenResult{
		epoch: 1, key: key, requested: record, err: githubsource.ErrRegistrationNotAdmitted,
	}); err != nil {
		t.Fatalf("install unadmitted registration result: %v", err)
	}
	if _, retained := source.desired[key]; retained {
		t.Fatalf("unadmitted registration remained desired: %#v", source.desired)
	}
	if source.sessionActivationActive || source.sessionActivationTotal != 0 {
		t.Fatalf("activation remained pending: active=%t total=%d",
			source.sessionActivationActive, source.sessionActivationTotal)
	}
}

func TestNilRegistrationSessionIsRetryableManagerFailure(t *testing.T) {
	record := githubsource.Registration{RepositoryURL: "https://github.example/octo/retry", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	source := newGitHubSourceRuntime(&githubManagerFake{}, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	key := registrationKey(record)
	term := branchLeadershipTerm(1)
	source.term = term
	source.leaderInstalled = true
	source.desired[key] = record
	err := source.installRegistrationSession(context.Background(), term, githubRegistrationOpenResult{
		epoch: 1, key: key, requested: record,
	})
	if err == nil {
		t.Fatal("nil registration session was accepted")
	}
	if source.desired[key] != record {
		t.Fatalf("retryable registration was removed: %#v", source.desired)
	}
}

func TestRemovingDesiredRegistrationWithoutActivationLeavesProgressInactive(t *testing.T) {
	record := githubsource.Registration{RepositoryURL: "https://github.example/octo/inactive", RunnerPool: "linux"}
	source := newGitHubSourceRuntime(&githubManagerFake{}, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	key := registrationKey(record)
	source.desired[key] = record

	source.removeDesiredRegistration(key)

	if len(source.desired) != 0 || source.sessionActivationActive {
		t.Fatalf("desired routes = %#v, activation active = %t", source.desired, source.sessionActivationActive)
	}
}
