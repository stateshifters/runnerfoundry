package github_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestManagerReportsRunnerPoolFailureOnceWithRepositoryContext(t *testing.T) {
	repository := transport.Repository{InstallationID: 1, URL: "https://github.com/octo/repository"}
	factory := newManagerFactoryFake([]transport.Repository{repository})
	factory.clients[repository.URL].ensureErr = errors.New("GitHub rejected runner scale set")
	manager, activities := newObservedManager(t, &managerDiscovererFake{repositories: []transport.Repository{repository}},
		factory.Client, &registrationRepositoryFake{})

	_, reconcileErr := manager.Reconcile(context.Background(), githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: "linux", RunnerGroup: "default",
	}}})
	if reconcileErr == nil {
		t.Fatal("Reconcile() succeeded despite the configured runner scale-set failure")
	}

	failures := make([]githubsource.ManagerActivity, 0)
	for _, activity := range *activities {
		if activity.Err != nil {
			failures = append(failures, activity)
		}
	}
	if len(failures) != 1 {
		t.Fatalf("reported failures = %#v, want one owning-boundary failure", failures)
	}
	failure := failures[0]
	if failure.Stage != "configure_runner_pool" || failure.Repository != repository.URL || failure.RunnerPool != "linux" {
		t.Fatalf("failure context = %#v", failure)
	}
}

func managerRepositories(count int) []transport.Repository {
	repositories := make([]transport.Repository, count)
	for index := range repositories {
		repositories[index] = transport.Repository{InstallationID: 1, URL: fmt.Sprintf("https://github.com/octo/repository-%03d", index+1)}
	}
	return repositories
}

func collectManagerProgress(activities []githubsource.ManagerActivity, infoStage, debugStage string) ([]int, []string) {
	progress := make([]int, 0)
	repositories := make([]string, 0)
	for _, activity := range activities {
		if activity.Visibility == githubsource.ActivityInfo && activity.Stage == infoStage && activity.Outcome == "progress" {
			progress = append(progress, activity.Current)
		}
		if activity.Visibility == githubsource.ActivityDebug && activity.Stage == debugStage && activity.Outcome == "started" {
			repositories = append(repositories, activity.Repository)
		}
	}
	return progress, repositories
}

func findManagerActivity(activities []githubsource.ManagerActivity, stage, outcome string) githubsource.ManagerActivity {
	for _, activity := range activities {
		if activity.Stage == stage && activity.Outcome == outcome {
			return activity
		}
	}
	return githubsource.ManagerActivity{}
}

func newObservedManager(t *testing.T, discoverer githubsource.RepositoryDiscoverer,
	factory githubsource.ManagerClientFactoryFunc, repository githubsource.RegistrationRepository,
) (*githubsource.Manager, *[]githubsource.ManagerActivity) {
	t.Helper()
	activities := make([]githubsource.ManagerActivity, 0)
	manager, err := githubsource.NewManager(discoverer, factory, repository, githubsource.ManagerObserver{
		Report:      func(activity githubsource.ManagerActivity) { activities = append(activities, activity) },
		CurrentTime: func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	return manager, &activities
}

func TestManagerReportsBoundedRepositoryReconciliationProgress(t *testing.T) {
	repositories := managerRepositories(150)
	factory := newManagerFactoryFake(repositories)
	manager, activities := newObservedManager(t, &managerDiscovererFake{repositories: repositories}, factory.Client, &registrationRepositoryFake{})
	configured := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}}}
	if _, err := manager.Reconcile(context.Background(), configured); err != nil {
		t.Fatalf("reconcile repositories: %v", err)
	}
	progress, debugRepositories := collectManagerProgress(*activities, "configure_repositories", "configure_repository")
	want := []int{1, 8, 15, 23, 30, 38, 45, 53, 60, 68, 75, 83, 90, 98, 105, 113, 120, 128, 135, 143, 150}
	if !reflect.DeepEqual(progress, want) || len(debugRepositories) != len(repositories) {
		t.Fatalf("progress=%#v debug repositories=%d", progress, len(debugRepositories))
	}
}

func TestManagerReportsCompletedReconciliationStages(t *testing.T) {
	repositories := []transport.Repository{{InstallationID: 1, URL: "https://github.com/octo/repository"}}
	factory := newManagerFactoryFake(repositories)
	manager, activities := newObservedManager(t, &managerDiscovererFake{repositories: repositories}, factory.Client, &registrationRepositoryFake{})
	configured := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}}}
	if _, err := manager.Reconcile(context.Background(), configured); err != nil {
		t.Fatalf("reconcile repositories: %v", err)
	}
	completed := make([]string, 0)
	var prunedRepository string
	for _, activity := range *activities {
		if activity.Visibility == githubsource.ActivityInfo && activity.Outcome == "completed" {
			completed = append(completed, activity.Stage)
		}
		if activity.Stage == "prune_repository" && activity.Outcome == "completed" {
			prunedRepository = activity.Repository
		}
	}
	want := []string{"load_registration_snapshot", "discover_repositories", "configure_repositories", "prune_registrations", "persist_registrations", "reconciliation"}
	if !reflect.DeepEqual(completed, want) {
		t.Fatalf("completed stages = %#v, want %#v", completed, want)
	}
	if prunedRepository != repositories[0].URL {
		t.Fatalf("debug pruned repository = %q", prunedRepository)
	}
}

func TestManagerReportsEachConfiguredRunnerPoolAtDebug(t *testing.T) {
	repository := transport.Repository{InstallationID: 1, URL: "https://github.com/octo/repository"}
	factory := newManagerFactoryFake([]transport.Repository{repository})
	manager, activities := newObservedManager(t, &managerDiscovererFake{repositories: []transport.Repository{repository}}, factory.Client, &registrationRepositoryFake{})
	configured := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}, {RunnerPool: "windows", RunnerGroup: "default"}}}
	if _, err := manager.Reconcile(context.Background(), configured); err != nil {
		t.Fatalf("reconcile repositories: %v", err)
	}
	runnerPools := make([]core.RunnerPool, 0)
	for _, activity := range *activities {
		if activity.Visibility == githubsource.ActivityDebug && activity.Stage == "configure_runner_pool" && activity.Outcome == "started" {
			runnerPools = append(runnerPools, activity.RunnerPool)
		}
	}
	if !reflect.DeepEqual(runnerPools, []core.RunnerPool{"linux", "windows"}) {
		t.Fatalf("debug Runner Pools = %#v", runnerPools)
	}
}
