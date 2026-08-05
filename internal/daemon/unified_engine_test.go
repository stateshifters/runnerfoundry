package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestUnifiedEngineRunsOneLifecycleForEveryStateBackend(t *testing.T) {
	t.Parallel()

	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			events := make([]string, 0)
			engine := unifiedEngineFixture(&events)

			if err := engine.Cycle(context.Background()); err != nil {
				t.Fatalf("cycle %s runtime: %v", backend, err)
			}

			want := []string{
				"heartbeat", "expire", "refresh-members", "leadership", "open-leader",
				"recover-acquisitions", "poll", "retry-results", "request:linux", "start:linux",
				"timeout:linux", "reconcile:linux",
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events = %#v, want %#v", events, want)
			}
		})
	}
}

func TestUnifiedEngineKeepsMemberWorkIndependentOfLeadership(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	engine.leadership = leadershipFake{leader: false, events: &events}

	if err := engine.Cycle(context.Background()); err != nil {
		t.Fatalf("cycle follower runtime: %v", err)
	}

	want := []string{
		"heartbeat", "expire", "refresh-members", "leadership", "retry-results",
		"request:linux", "start:linux", "timeout:linux", "reconcile:linux",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestUnifiedEngineClosesLeaderResourcesWhenLeadershipIsLost(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	leadership := &changingLeadershipFake{results: []bool{true, false}, events: &events}
	engine := unifiedEngineFixture(&events)
	engine.leadership = leadership

	if err := engine.Cycle(context.Background()); err != nil {
		t.Fatalf("leader cycle: %v", err)
	}
	if err := engine.Cycle(context.Background()); err != nil {
		t.Fatalf("follower cycle: %v", err)
	}
	if occurrences(events, "close-leader") != 1 {
		t.Fatalf("events = %#v, want leader resources closed once", events)
	}
}

func TestUnifiedEngineContinuesIndependentMaintenanceAfterFailures(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	heartbeatErr := errors.New("heartbeat unavailable")
	engine.membership = membershipFake{events: &events, heartbeatErr: heartbeatErr}
	engine.pools[0].runtime = poolRuntimeFake{events: &events, requestErr: errors.New("claim unavailable")}

	err := engine.Cycle(context.Background())
	if err == nil || !errors.Is(err, heartbeatErr) {
		t.Fatalf("cycle error = %v, want heartbeat error", err)
	}
	if occurrences(events, "reconcile:linux") != 1 || occurrences(events, "retry-results") != 1 {
		t.Fatalf("events = %#v, want independent maintenance to continue", events)
	}
}

func TestUnifiedEngineCloseReleasesOnlyOwnedSourceLeadership(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	if err := engine.Cycle(context.Background()); err != nil {
		t.Fatalf("cycle runtime: %v", err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatalf("close runtime twice: %v", err)
	}
	if occurrences(events, "close-leader") != 1 {
		t.Fatalf("events = %#v, want one close", events)
	}
}

func TestUnifiedEngineJoinsIndependentOperationFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("operation failed")
	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	engine.membership = membershipFake{events: &events, heartbeatErr: wantErr, expireErr: wantErr}
	engine.source = sourceRuntimeFake{
		events: &events, refreshErr: wantErr, pollErr: wantErr,
	}
	engine.lifecycle = lifecycleRuntimeFake{events: &events, recoverErr: wantErr, retryErr: wantErr}
	engine.pools[0].runtime = poolRuntimeFake{
		events: &events, requestErr: wantErr, startErr: wantErr, timeoutErr: wantErr, reconcileErr: wantErr,
	}
	if err := engine.Cycle(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestUnifiedEngineReportsLeadershipLifecycleFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("leadership failed")
	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	engine.leadership = leadershipFake{leader: true, events: &events, err: wantErr}
	if err := engine.Cycle(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("inspect leadership error = %v", err)
	}
	engine.leadership = leadershipFake{leader: true, events: &events}
	engine.source = sourceRuntimeFake{events: &events, openErr: wantErr}
	if err := engine.Cycle(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("open leadership error = %v", err)
	}
}

func TestUnifiedEngineReportsLeadershipCloseFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("leadership failed")
	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	engine.source = sourceRuntimeFake{events: &events, closeErr: wantErr}
	mustRunEngineCycle(t, engine)
	engine.leadership = leadershipFake{events: &events}
	assertEngineCycleError(t, engine, wantErr, "lost leadership close")
	assertEngineCycleError(t, engine, wantErr, "lost leadership close retry")
	if occurrences(events, "close-leader") != 2 {
		t.Fatalf("close attempts = %d, want retry", occurrences(events, "close-leader"))
	}
	engine.leadership = leadershipFake{leader: true, events: &events}
	assertEngineCycleError(t, engine, wantErr, "regained leadership close")
	engine.source = sourceRuntimeFake{events: &events}
	mustRunEngineCycle(t, engine)
	if err := engine.openLeadership(context.Background()); err != nil {
		t.Fatalf("open existing leadership: %v", err)
	}
	engine.source = sourceRuntimeFake{events: &events, closeErr: wantErr}
	if err := engine.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("engine close error = %v", err)
	}
}

func mustRunEngineCycle(t *testing.T, engine *UnifiedEngine) {
	t.Helper()
	if err := engine.Cycle(context.Background()); err != nil {
		t.Fatalf("run engine cycle: %v", err)
	}
}

func assertEngineCycleError(t *testing.T, engine *UnifiedEngine, want error, operation string) {
	t.Helper()
	if err := engine.Cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("%s error = %v, want %v", operation, err, want)
	}
}

func unifiedEngineFixture(events *[]string) *UnifiedEngine {
	return NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: func() time.Time { return time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC) },
		MemberTTL:   time.Minute,
		Membership:  membershipFake{events: events},
		Leadership:  leadershipFake{leader: true, events: events},
		Source:      sourceRuntimeFake{events: events},
		Lifecycle:   lifecycleRuntimeFake{events: events},
		Pools: []RunnerPoolRuntime{{
			RunnerPool: core.RunnerPool("linux"), JobStartTimeout: 10 * time.Minute,
			runtime: poolRuntimeFake{events: events},
		}},
	})
}

type membershipFake struct {
	events       *[]string
	heartbeatErr error
	expireErr    error
}

func (fake membershipFake) Heartbeat(context.Context) error {
	*fake.events = append(*fake.events, "heartbeat")
	return fake.heartbeatErr
}
func (fake membershipFake) Expire(context.Context, time.Time) (int, error) {
	*fake.events = append(*fake.events, "expire")
	return 0, fake.expireErr
}

type leadershipFake struct {
	leader bool
	events *[]string
	err    error
}

func (fake leadershipFake) IsLeader(context.Context) (bool, error) {
	*fake.events = append(*fake.events, "leadership")
	return fake.leader, fake.err
}
func (leadershipFake) Close(context.Context) error { return nil }

type changingLeadershipFake struct {
	results []bool
	events  *[]string
}

func (fake *changingLeadershipFake) IsLeader(context.Context) (bool, error) {
	*fake.events = append(*fake.events, "leadership")
	result := fake.results[0]
	fake.results = fake.results[1:]
	return result, nil
}

type sourceRuntimeFake struct {
	events     *[]string
	refreshErr error
	openErr    error
	pollErr    error
	closeErr   error
}

func (fake sourceRuntimeFake) RefreshMembers(context.Context) error {
	*fake.events = append(*fake.events, "refresh-members")
	return fake.refreshErr
}
func (fake sourceRuntimeFake) OpenLeader(context.Context) error {
	*fake.events = append(*fake.events, "open-leader")
	return fake.openErr
}
func (fake sourceRuntimeFake) Poll(context.Context, time.Time) error {
	*fake.events = append(*fake.events, "poll")
	return fake.pollErr
}
func (fake sourceRuntimeFake) CloseLeader(context.Context) error {
	*fake.events = append(*fake.events, "close-leader")
	return fake.closeErr
}

type lifecycleRuntimeFake struct {
	events     *[]string
	recoverErr error
	retryErr   error
}

func (fake lifecycleRuntimeFake) RecoverInterruptedAcquisitions(context.Context) error {
	*fake.events = append(*fake.events, "recover-acquisitions")
	return fake.recoverErr
}
func (fake lifecycleRuntimeFake) RetryPendingResults(context.Context) error {
	*fake.events = append(*fake.events, "retry-results")
	return fake.retryErr
}

type poolRuntimeFake struct {
	events       *[]string
	requestErr   error
	startErr     error
	timeoutErr   error
	reconcileErr error
}

func (fake poolRuntimeFake) RequestWork(context.Context, core.RunnerPool) error {
	*fake.events = append(*fake.events, "request:linux")
	return fake.requestErr
}
func (fake poolRuntimeFake) StartAcquired(context.Context, core.RunnerPool) error {
	*fake.events = append(*fake.events, "start:linux")
	return fake.startErr
}
func (fake poolRuntimeFake) FailUnstarted(context.Context, core.RunnerPool, time.Duration) error {
	*fake.events = append(*fake.events, "timeout:linux")
	return fake.timeoutErr
}
func (fake poolRuntimeFake) Reconcile(context.Context, core.RunnerPool) error {
	*fake.events = append(*fake.events, "reconcile:linux")
	return fake.reconcileErr
}

func occurrences(events []string, target string) int {
	count := 0
	for _, event := range events {
		if event == target {
			count++
		}
	}
	return count
}
