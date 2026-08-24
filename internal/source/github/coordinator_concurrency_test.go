package github

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestMultiAppCoordinatorBoundsFailedDiscoveryWithoutCancellingHealthyApp(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	first := coordinatorManager(t, store, 11, nil)
	first.discoverer = blockingCoordinatorDiscoverer{}
	second := coordinatorManager(t, store, 22, []transport.Repository{{InstallationID: 2,
		URL: "https://github.com/example/healthy"}})
	coordinator, err := NewMultiAppCoordinator(store, first, second)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	coordinator.operationTimeout = 20 * time.Millisecond
	records, reconcileErr := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if reconcileErr == nil || len(records) != 1 || records[0].GitHubAppID != 22 {
		t.Fatalf("bounded failed App result = %#v, %v", records, reconcileErr)
	}
}

func TestMultiAppCoordinatorSharesRepositoryConcurrencyAcrossApps(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	first := coordinatorManager(t, store, 11, coordinatorRepositories("first", 1))
	second := coordinatorManager(t, store, 22, coordinatorRepositories("second", 2))
	probe := newCoordinatorConcurrencyProbe()
	first.factory = probe.clientFactory(11)
	second.factory = probe.clientFactory(22)
	coordinator, err := NewMultiAppCoordinator(store, first, second)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	completed := make(chan error, 1)
	go func() {
		_, reconcileErr := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
		completed <- reconcileErr
	}()
	for index := 0; index < repositoryConcurrency; index++ {
		<-probe.started
	}
	select {
	case <-probe.started:
		t.Fatal("more than four repositories reconciled concurrently across Apps")
	case <-time.After(20 * time.Millisecond):
	}
	close(probe.release)
	if err = <-completed; err != nil || probe.maximum.Load() != repositoryConcurrency {
		t.Fatalf("shared reconciliation bound: maximum=%d error=%v", probe.maximum.Load(), err)
	}
}

func TestMultiAppCoordinatorLetsHealthyAppReconcileWhileAnotherAppSaturatesItsAdmission(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	blocked := coordinatorManager(t, store, 11, coordinatorRepositories("blocked", 1))
	blocked.factory = func(context.Context, transport.Repository) (ManagerClient, error) {
		return ManagerClient{EnsureRunnerPool: func(ctx context.Context, _, _ string) (transport.Registration, error) {
			<-ctx.Done()
			return transport.Registration{}, fmt.Errorf("blocked App repository: %w", ctx.Err())
		}}, nil
	}
	healthy := coordinatorManager(t, store, 22, []transport.Repository{{
		InstallationID: 2,
		URL:            "https://github.com/example/healthy",
	}})
	coordinator, err := NewMultiAppCoordinator(store, blocked, healthy)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	coordinator.operationTimeout = 20 * time.Millisecond
	records, reconcileErr := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if reconcileErr == nil {
		t.Fatal("blocked App reconciliation was not reported")
	}
	if len(records) != 1 || records[0].GitHubAppID != 22 {
		t.Fatalf("healthy App registrations = %#v, want one App 22 registration", records)
	}
}

func TestMultiAppCoordinatorStartsRepositoryTimeoutAfterAdmission(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	manager := coordinatorManager(t, store, 22, []transport.Repository{{
		InstallationID: 2,
		URL:            "https://github.com/example/healthy",
	}})
	coordinator, err := NewMultiAppCoordinator(store, manager)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	coordinator.operationTimeout = 20 * time.Millisecond
	for index := 0; index < repositoryConcurrency; index++ {
		manager.repositorySlots <- struct{}{}
	}
	time.AfterFunc(30*time.Millisecond, func() {
		for index := 0; index < repositoryConcurrency; index++ {
			<-manager.repositorySlots
		}
	})
	records, reconcileErr := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if reconcileErr != nil || len(records) != 1 || records[0].GitHubAppID != 22 {
		t.Fatalf("post-admission reconciliation = %#v, %v", records, reconcileErr)
	}
}

func TestRepositoryQuotasReserveAdmissionForEveryConfiguredApp(t *testing.T) {
	for name, testCase := range map[string]struct {
		appCount int
		want     []int
	}{
		"one App uses all capacity":        {appCount: 1, want: []int{4}},
		"two Apps divide capacity":         {appCount: 2, want: []int{2, 2}},
		"remainder follows precedence":     {appCount: 3, want: []int{2, 1, 1}},
		"four Apps reserve one each":       {appCount: 4, want: []int{1, 1, 1, 1}},
		"more Apps still reserve capacity": {appCount: 5, want: []int{1, 1, 1, 1, 1}},
	} {
		t.Run(name, func(t *testing.T) {
			quotas := repositoryQuotas(testCase.appCount)
			if fmt.Sprint(quotas) != fmt.Sprint(testCase.want) {
				t.Fatalf("repository quotas = %v, want %v", quotas, testCase.want)
			}
		})
	}
}

func coordinatorRepositories(prefix string, installationID transport.InstallationID) []transport.Repository {
	result := make([]transport.Repository, 4)
	for index := range result {
		result[index] = transport.Repository{InstallationID: installationID,
			URL: fmt.Sprintf("https://github.com/example/%s-%d", prefix, index)}
	}
	return result
}

type coordinatorConcurrencyProbe struct {
	started chan struct{}
	release chan struct{}
	active  atomic.Int32
	maximum atomic.Int32
}

func newCoordinatorConcurrencyProbe() *coordinatorConcurrencyProbe {
	return &coordinatorConcurrencyProbe{started: make(chan struct{}, 8), release: make(chan struct{})}
}

func (probe *coordinatorConcurrencyProbe) clientFactory(appID AppID) ManagerClientFactoryFunc {
	return func(context.Context, transport.Repository) (ManagerClient, error) {
		return ManagerClient{EnsureRunnerPool: func(context.Context, string, string) (transport.Registration, error) {
			current := probe.active.Add(1)
			probe.recordMaximum(current)
			probe.started <- struct{}{}
			<-probe.release
			probe.active.Add(-1)
			return transport.Registration{ID: transport.ScaleSetID(appID), RunnerGroupID: 1}, nil
		}}, nil
	}
}

func (probe *coordinatorConcurrencyProbe) recordMaximum(current int32) {
	for observed := probe.maximum.Load(); current > observed; observed = probe.maximum.Load() {
		if probe.maximum.CompareAndSwap(observed, current) {
			return
		}
	}
}

type blockingCoordinatorDiscoverer struct{}

func (blockingCoordinatorDiscoverer) Discover(ctx context.Context) (transport.DiscoveryResult, error) {
	<-ctx.Done()
	return transport.DiscoveryResult{}, fmt.Errorf("blocked discovery: %w", ctx.Err())
}
