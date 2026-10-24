package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestProductionRuntimeBuildAndCloseUseInjectedComposition(t *testing.T) {
	runtime, builds := newTestProductionRuntime(t)
	first, err := runtime.Build(context.Background(), config.Config{})
	if err != nil || first == nil {
		t.Fatalf("first runtime build = (%v, %v)", first, err)
	}
	second, err := runtime.Build(context.Background(), config.Config{})
	if err != nil || second == nil || *builds != 2 {
		t.Fatalf("replacement runtime build = (%v, %v), builds %d", second, err, *builds)
	}
	if err = runtime.Close(context.Background()); err != nil {
		t.Fatalf("close production runtime: %v", err)
	}
}

func TestProductionRuntimeReusesMemberIdentityAcrossReload(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	runtime.state.member = daemonTestID(7)
	members := make([]core.MemberID, 0, 2)
	runtime.build = func(
		_ context.Context,
		_ config.Config,
		state *productionState,
		_ *localLeadershipGate,
		_ func(error),
	) (*maintenanceGeneration, error) {
		members = append(members, state.member)
		return newTestMaintenanceGeneration(
			&maintenanceEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, func(error) {}, nil,
			nil,
		), nil
	}

	if _, err := runtime.Build(context.Background(), config.Config{}); err != nil {
		t.Fatalf("initial build: %v", err)
	}
	if _, err := runtime.Build(context.Background(), config.Config{}); err != nil {
		t.Fatalf("reload build: %v", err)
	}
	if len(members) != 2 || members[0] != daemonTestID(7) || members[1] != daemonTestID(7) {
		t.Fatalf("reload member identities = %v", members)
	}
}

func TestProductionRuntimeCloseWithdrawsMember(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("member withdrawal failed")
	store := &shutdownStoreFake{builderStoreFake: newBuilderStoreFake(), withdrawErr: wantErr}
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	runtime := &productionRuntime{logging: logging, state: &productionState{
		store: runtimeStatePorts{members: store}, member: daemonTestID(7),
	}}

	if err := runtime.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v, want transition failure", err)
	}
	if len(store.withdrawn) != 1 || store.withdrawn[0] != daemonTestID(7) {
		t.Fatalf("withdrawn member capacities = %#v, want member 7", store.withdrawn)
	}
}

func newTestProductionRuntime(t *testing.T) (*productionRuntime, *int) {
	t.Helper()
	builds := 0
	buildGeneration := func(
		context.Context,
		config.Config,
		*productionState,
		*localLeadershipGate,
		func(error),
	) (*maintenanceGeneration, error) {
		builds++
		return newTestMaintenanceGeneration(
			&maintenanceEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, func(error) {}, nil,
			nil,
		), nil
	}
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	return &productionRuntime{
		logging: logging, state: &productionState{store: builderStatePorts(newBuilderStoreFake()), member: daemonTestID(1)}, processGate: &localLeadershipGate{}, report: func(error) {}, firstBuild: true,
		build: buildGeneration, reporter: &productionRuntimeReporter{},
	}, &builds
}

type shutdownStoreFake struct {
	*builderStoreFake
	withdrawn   []core.MemberID
	withdrawErr error
}

func (store *shutdownStoreFake) WithdrawMemberCapacity(_ context.Context, member core.MemberID) error {
	store.withdrawn = append(store.withdrawn, member)
	return store.withdrawErr
}

type blockingShutdownStore struct {
	*builderStoreFake
	started chan<- struct{}
	release <-chan struct{}
}

func successfulSupervisor(
	context.Context,
	<-chan struct{},
	func() (config.Config, error),
	generationBuilder,
	serviceNotifier,
	func(error),
	ActiveRunnerPolicy,
) error {
	return nil
}

func blockingSupervisor(started chan<- struct{}, release <-chan struct{}, resultErr error) daemonSupervisor {
	return func(
		ctx context.Context,
		_ <-chan struct{},
		_ func() (config.Config, error),
		_ generationBuilder,
		_ serviceNotifier,
		_ func(error),
		_ ActiveRunnerPolicy,
	) error {
		<-ctx.Done()
		close(started)
		<-release
		return resultErr
	}
}

func (store *blockingShutdownStore) WithdrawMemberCapacity(context.Context, core.MemberID) error {
	close(store.started)
	<-store.release
	return nil
}

func TestRunUsesInjectedApplicationBoundaries(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	dependencies, loads := testRunBoundaries(t, runtime)
	application := NewApplication()
	application.dependencies = dependencies
	if err := application.Run(context.Background(), Options{ConfigPath: "config.yml"}); err != nil || *loads != 2 {
		t.Fatalf("Run = %v, loads %d", err, *loads)
	}
}

func TestProductionShutdownTimerCanBeStoppedWithoutWaiting(t *testing.T) {
	t.Parallel()

	timer := productionDaemonApplicationDependencies().newShutdownTimer(time.Hour)
	if timer.expired == nil || timer.stop == nil {
		t.Fatal("production shutdown timer is incomplete")
	}
	timer.stop()
}

func TestRunRejectsInvalidActiveRunnerPolicyBeforeLoadingConfiguration(t *testing.T) {
	loaded := false
	dependencies := daemonApplicationDependencies{loadConfig: func(string, config.ReadFile) (config.Config, error) {
		loaded = true
		return config.Config{}, nil
	}}

	if err := runDaemon(context.Background(), Options{ActiveRunnerPolicy: "destroy"}, dependencies); err == nil {
		t.Fatal("Run accepted an invalid active runner policy")
	}
	if loaded {
		t.Fatal("Run loaded configuration before validating active runner policy")
	}
}

func TestRunReportsConfigLoadFailure(t *testing.T) {
	wantErr := errors.New("application failed")
	dependencies := daemonApplicationDependencies{loadConfig: func(string, config.ReadFile) (config.Config, error) {
		return config.Config{}, wantErr
	}}
	if err := runDaemon(context.Background(), Options{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("load error = %v", err)
	}
}

func TestRunReportsRuntimeOpenFailure(t *testing.T) {
	wantErr := errors.New("application failed")
	dependencies := daemonApplicationDependencies{
		loadConfig: func(string, config.ReadFile) (config.Config, error) { return config.Config{}, nil },
		openRuntime: func(context.Context, config.Config, productionDependencies) (*productionRuntime, error) {
			return nil, wantErr
		},
	}
	if err := runDaemon(context.Background(), Options{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("open error = %v", err)
	}
}

func TestRunReportsSupervisorFailure(t *testing.T) {
	wantErr := errors.New("application failed")
	runtime, _ := newTestProductionRuntime(t)
	dependencies, _ := testRunBoundaries(t, runtime)
	dependencies.supervise = func(
		context.Context, <-chan struct{}, func() (config.Config, error), generationBuilder, serviceNotifier, func(error),
		ActiveRunnerPolicy,
	) error {
		return wantErr
	}
	if err := runDaemon(context.Background(), Options{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("supervise error = %v", err)
	}
}

func TestLeaveShutdownReturnsSuccessfullyWhenRuntimeCleanupExceedsBudget(t *testing.T) {
	t.Parallel()

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	store := &blockingShutdownStore{
		builderStoreFake: newBuilderStoreFake(),
		started:          cleanupStarted,
		release:          releaseCleanup,
	}
	runtime, _ := newTestProductionRuntime(t)
	runtime.state.store.members = store
	dependencies, _ := testRunBoundaries(t, runtime)
	expired := make(chan time.Time, 1)
	timerCreated := make(chan time.Duration, 1)
	dependencies.newShutdownTimer = func(duration time.Duration) shutdownTimer {
		timerCreated <- duration
		return shutdownTimer{expired: expired, stop: func() {}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runDaemon(ctx, Options{}, dependencies) }()
	<-cleanupStarted
	cancel()
	if duration := <-timerCreated; duration != 30*time.Second {
		t.Fatalf("leave shutdown budget = %s, want 30s", duration)
	}
	expired <- time.Time{}
	if err := <-result; err != nil {
		t.Fatalf("leave shutdown timeout = %v, want success", err)
	}
	close(releaseCleanup)
}

func TestLeaveShutdownReturnsSuccessfullyWhenSupervisorCleanupExceedsBudget(t *testing.T) {
	t.Parallel()

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	runtime, _ := newTestProductionRuntime(t)
	dependencies, _ := testRunBoundaries(t, runtime)
	dependencies.supervise = blockingSupervisor(cleanupStarted, releaseCleanup, nil)
	expired := make(chan time.Time, 1)
	dependencies.newShutdownTimer = func(time.Duration) shutdownTimer {
		return shutdownTimer{expired: expired, stop: func() {}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runDaemon(ctx, Options{}, dependencies) }()
	cancel()
	<-cleanupStarted
	expired <- time.Time{}
	if err := <-result; err != nil {
		t.Fatalf("leave shutdown timeout = %v, want success", err)
	}
	close(releaseCleanup)
}

func TestLeaveShutdownReturnsCleanupErrorBeforeBudgetExpires(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("cleanup failed")
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	timerCreated := make(chan struct{})
	timerStopped := make(chan struct{})
	runtime, _ := newTestProductionRuntime(t)
	dependencies, _ := testRunBoundaries(t, runtime)
	dependencies.supervise = blockingSupervisor(cleanupStarted, releaseCleanup, wantErr)
	dependencies.newShutdownTimer = func(time.Duration) shutdownTimer {
		close(timerCreated)
		return shutdownTimer{expired: make(chan time.Time), stop: func() { close(timerStopped) }}
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runDaemon(ctx, Options{}, dependencies) }()
	cancel()
	<-cleanupStarted
	<-timerCreated
	close(releaseCleanup)
	if err := <-result; !errors.Is(err, wantErr) {
		t.Fatalf("leave shutdown cleanup = %v, want %v", err, wantErr)
	}
	<-timerStopped
}

func TestTerminateShutdownDoesNotUseLeaveBudget(t *testing.T) {
	t.Parallel()

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	store := &blockingShutdownStore{
		builderStoreFake: newBuilderStoreFake(),
		started:          cleanupStarted,
		release:          releaseCleanup,
	}
	runtime, _ := newTestProductionRuntime(t)
	runtime.state.store.members = store
	dependencies, _ := testRunBoundaries(t, runtime)
	dependencies.supervise = successfulSupervisor
	dependencies.newShutdownTimer = func(time.Duration) shutdownTimer {
		t.Fatal("terminate shutdown created the leave timer")
		return shutdownTimer{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- runDaemon(ctx, Options{ActiveRunnerPolicy: TerminateActiveRunners}, dependencies)
	}()
	<-cleanupStarted
	cancel()
	select {
	case err := <-result:
		t.Fatalf("terminate shutdown returned before cleanup completed: %v", err)
	default:
	}
	close(releaseCleanup)
	if err := <-result; err != nil {
		t.Fatalf("terminate shutdown = %v", err)
	}
}

func testRunBoundaries(t *testing.T, runtime *productionRuntime) (daemonApplicationDependencies, *int) {
	t.Helper()
	loads := 0
	dependencies := daemonApplicationDependencies{}
	dependencies.loadConfig = func(string, config.ReadFile) (config.Config, error) { loads++; return config.Config{}, nil }
	dependencies.openRuntime = func(context.Context, config.Config, productionDependencies) (*productionRuntime, error) {
		return runtime, nil
	}
	dependencies.supervise = func(
		ctx context.Context,
		_ <-chan struct{},
		load func() (config.Config, error),
		build generationBuilder,
		_ serviceNotifier,
		_ func(error),
		policy ActiveRunnerPolicy,
	) error {
		if policy != LeaveActiveRunners {
			t.Fatalf("default active runner policy = %q", policy)
		}
		configured, loadErr := load()
		if loadErr != nil {
			return loadErr
		}
		if _, loadErr = load(); loadErr != nil {
			return loadErr
		}
		_, buildErr := build(ctx, configured)
		return buildErr
	}
	return dependencies, &loads
}
