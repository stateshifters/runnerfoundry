package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestGitHubSourceRuntimePublishesAggregateCapacityBeforePolling(t *testing.T) {
	t.Parallel()

	messageClient := &capacityMessageClientFake{}
	adapter := &githubsource.Adapter{Client: messageClient, Handler: eventHandlerFake{}, RunnerPool: "linux"}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		adapter,
		{Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{}, RunnerPool: "linux"},
	}}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager, capacities, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
			RunnerPool: "linux", RunnerGroup: "default",
		}}}, time.Minute, func() time.Time { return now })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	if err := source.PublishCapacity(context.Background(), now.Add(-time.Minute)); err != nil {
		t.Fatalf("publish capacity: %v", err)
	}
	if adapter.MaxCapacity != 7 {
		t.Fatalf("adapter max capacity = %d, want aggregate 7", adapter.MaxCapacity)
	}
	if len(capacities.queries) != 1 {
		t.Fatalf("aggregate queries = %d, want one per Runner Pool", len(capacities.queries))
	}
}

func TestGitHubSourceRuntimeReconcilesDiscoveryOnlyWhenDue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	if err := source.PublishCapacity(context.Background(), now); err != nil {
		t.Fatalf("publish before discovery is due: %v", err)
	}
	now = now.Add(time.Minute)
	if err := source.PublishCapacity(context.Background(), now); err != nil {
		t.Fatalf("publish when discovery is due: %v", err)
	}
	if manager.reconciles != 2 || manager.opens != 2 {
		t.Fatalf("reconciles=%d opens=%d, want 2 each", manager.reconciles, manager.opens)
	}
}

func TestGitHubSourceRuntimeRefreshesMemberRoutesAtDiscoveryCadence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })

	if err := source.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh member routes: %v", err)
	}
	if err := source.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh member routes before due: %v", err)
	}
	now = now.Add(time.Minute)
	if err := source.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh member routes when due: %v", err)
	}
	if got := occurrences(manager.events, "refresh"); got != 2 {
		t.Fatalf("route refreshes = %d, want 2", got)
	}
}

func TestGitHubSourceRuntimePollsSessionsIndependently(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first session failed")
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: &capacityMessageClientFake{err: firstErr}, Handler: eventHandlerFake{}, RunnerPool: "first"},
		{Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{}, RunnerPool: "second"},
	}}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	err := source.Poll(context.Background())
	if !errors.Is(err, firstErr) {
		t.Fatalf("poll error = %v, want first session failure", err)
	}
	if manager.adapters[1].LastMessageID != 0 {
		t.Fatalf("second adapter state = %#v", manager.adapters[1])
	}
}

func TestGitHubSourceRuntimeKeepsInstalledAdaptersWhenOldSessionCloseFails(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("old session close failed")
	messageClient := &capacityMessageClientFake{}
	manager := &githubManagerFake{
		adapters: []*githubsource.Adapter{{Client: messageClient, Handler: eventHandlerFake{}, RunnerPool: "linux"}},
		openErr:  closeErr,
	}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })

	if err := source.OpenLeader(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("open error = %v, want old close error", err)
	}
	if err := source.Poll(context.Background()); err != nil {
		t.Fatalf("poll installed replacement: %v", err)
	}
	if messageClient.calls != 1 {
		t.Fatal("replacement adapter was not polled")
	}
}

func TestGitHubSourceRuntimeReportsLifecycleAndCapacityFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("GitHub failed")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{refreshErr: wantErr}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	if err := source.RefreshMembers(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("refresh error = %v", err)
	}
	manager.refreshErr = nil
	manager.reconcileErr = wantErr
	if err := source.OpenLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("reconcile error = %v", err)
	}
	if err := source.PublishCapacity(context.Background(), now); !errors.Is(err, wantErr) {
		t.Fatalf("due discovery error = %v", err)
	}
	manager.reconcileErr = nil
	manager.refreshErr = wantErr
	if err := source.OpenLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("open refresh error = %v", err)
	}
}

func TestGitHubSourceRuntimeReportsCapacityAndCloseFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("GitHub failed")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	manager.refreshErr = nil
	manager.adapters = []*githubsource.Adapter{{Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{}, RunnerPool: "linux"}}
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.capacities = &capacityAggregatorFake{err: wantErr}
	if err := source.PublishCapacity(context.Background(), now); !errors.Is(err, wantErr) {
		t.Fatalf("capacity error = %v", err)
	}
	manager.closeErr = wantErr
	if err := source.CloseLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v", err)
	}
}

type githubManagerFake struct {
	adapters     []*githubsource.Adapter
	reconciles   int
	opens        int
	openErr      error
	reconcileErr error
	refreshErr   error
	closeErr     error
	events       []string
}

func (fake *githubManagerFake) Reconcile(context.Context, githubsource.ManagerConfig) error {
	fake.reconciles++
	fake.events = append(fake.events, "reconcile")
	return fake.reconcileErr
}
func (fake *githubManagerFake) RefreshMembers(context.Context) error {
	fake.events = append(fake.events, "refresh")
	return fake.refreshErr
}
func (fake *githubManagerFake) OpenLeader(context.Context, string, githubsource.EventHandler) ([]*githubsource.Adapter, error) {
	fake.opens++
	fake.events = append(fake.events, "open")
	return fake.adapters, fake.openErr
}
func (fake *githubManagerFake) CloseLeader(context.Context) error {
	fake.events = append(fake.events, "close")
	return fake.closeErr
}

type capacityAggregatorFake struct {
	capacities map[core.RunnerPool]int
	queries    []core.RunnerPool
	err        error
}

func (fake *capacityAggregatorFake) AggregateRunnerPoolCapacity(
	_ context.Context,
	runnerPool core.RunnerPool,
	_ time.Time,
) (int, error) {
	fake.queries = append(fake.queries, runnerPool)
	return fake.capacities[runnerPool], fake.err
}

type capacityMessageClientFake struct {
	maxCapacity int
	calls       int
	err         error
}

func (fake *capacityMessageClientFake) GetMessage(_ context.Context, _, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	fake.maxCapacity = maxCapacity
	fake.calls++
	if fake.err != nil {
		return nil, fake.err
	}
	return nil, githubsource.ErrNoMessage
}

func (*capacityMessageClientFake) DeleteMessage(context.Context, int) error { return nil }

type eventHandlerFake struct{}

func (eventHandlerFake) Request(context.Context, core.JobRequest) error { return nil }
func (eventHandlerFake) Started(context.Context, core.SourceReference, time.Time) error {
	return nil
}
func (eventHandlerFake) Completed(context.Context, core.SourceReference, core.Result) error {
	return nil
}
