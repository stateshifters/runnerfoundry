package github_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestManagerAllowsRegistrationRepositoryWithoutDeclarationPersistence(t *testing.T) {
	t.Parallel()
	unsupported := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t), &snapshotRepositoryFake{})
	if unsupported.UsesSharedRunnerPoolDeclarations() {
		t.Fatal("manager reported unsupported declaration persistence as available")
	}
	if err := unsupported.PublishRunnerPoolDeclarations(context.Background(), "member", githubsource.ManagerConfig{}); err != nil {
		t.Fatalf("publish through legacy test repository: %v", err)
	}
	if _, err := unsupported.ClusterRunnerPools(context.Background(), time.Time{}); !errors.Is(err, githubsource.ErrRunnerPoolDeclarationPersistenceUnavailable) {
		t.Fatalf("ClusterRunnerPools() error = %v", err)
	}
}

func TestManagerRejectsInvalidRunnerPoolDeclarations(t *testing.T) {
	t.Parallel()
	manager := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t), &registrationRepositoryFake{})
	invalid := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux"}}}
	if err := manager.PublishRunnerPoolDeclarations(context.Background(), "member", invalid); err == nil {
		t.Fatal("invalid declaration configuration was accepted")
	}
}

func TestManagerReportsRunnerPoolDeclarationRepositoryFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("declarations unavailable")
	repository := &registrationRepositoryFake{declarationErr: wantErr}
	manager := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t), repository)
	if !manager.UsesSharedRunnerPoolDeclarations() {
		t.Fatal("manager did not report declaration persistence")
	}
	valid := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}}}
	if err := manager.PublishRunnerPoolDeclarations(context.Background(), "member", valid); !errors.Is(err, wantErr) {
		t.Fatalf("publish error = %v, want %v", err, wantErr)
	}
	if _, err := manager.ClusterRunnerPools(context.Background(), time.Time{}); !errors.Is(err, wantErr) {
		t.Fatalf("load error = %v, want %v", err, wantErr)
	}
}

func TestManagerPublishesMemberDeclarationsAndBuildsStableLiveUnion(t *testing.T) {
	t.Parallel()
	repository := &registrationRepositoryFake{}
	manager := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t), repository)
	spock := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
		{RunnerPool: "windows-latest", RunnerGroup: "default"},
	}}
	serenity := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "windows-latest", RunnerGroup: "default"},
		{RunnerPool: "windows-slim", RunnerGroup: "serenity"},
	}}
	if err := manager.PublishRunnerPoolDeclarations(context.Background(), "spock", spock); err != nil {
		t.Fatalf("publish Spock declarations: %v", err)
	}
	if err := manager.PublishRunnerPoolDeclarations(context.Background(), "serenity", serenity); err != nil {
		t.Fatalf("publish Serenity declarations: %v", err)
	}
	snapshot, err := manager.ClusterRunnerPools(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("read cluster declarations: %v", err)
	}
	want := []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
		{RunnerPool: "windows-latest", RunnerGroup: "default"},
		{RunnerPool: "windows-slim", RunnerGroup: "serenity"},
	}
	if !reflect.DeepEqual(snapshot.RunnerPools, want) || len(snapshot.ConflictedRunnerPools) != 0 {
		t.Fatalf("snapshot = %#v, want Runner Pools %#v", snapshot, want)
	}
}

func TestManagerIsolatesConflictingRunnerGroupDeclarations(t *testing.T) {
	t.Parallel()
	repository := &registrationRepositoryFake{declarations: []githubsource.MemberRunnerPoolDeclaration{
		{MemberID: "spock", RunnerPool: "windows", RunnerGroup: "default"},
		{MemberID: "serenity", RunnerPool: "windows", RunnerGroup: "serenity"},
		{MemberID: "spock", RunnerPool: "macos", RunnerGroup: "default"},
		{MemberID: "serenity", RunnerPool: "macos", RunnerGroup: "serenity"},
		{MemberID: "serenity", RunnerPool: "linux", RunnerGroup: "default"},
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, failOnManagerFactoryCall(t), repository)
	snapshot, err := manager.ClusterRunnerPools(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("read cluster declarations: %v", err)
	}
	if !reflect.DeepEqual(snapshot.RunnerPools, []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}}) ||
		!reflect.DeepEqual(snapshot.ConflictedRunnerPools, []core.RunnerPool{"macos", "windows"}) {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
