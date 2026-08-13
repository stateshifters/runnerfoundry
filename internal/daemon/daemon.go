package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
)

const leaveShutdownTimeout = 30 * time.Second

type Options struct {
	ConfigPath         string
	ReloadRequests     <-chan struct{}
	ActiveRunnerPolicy ActiveRunnerPolicy
}

type serviceNotifier interface {
	Ready(ctx context.Context) error
	Reloading(ctx context.Context) error
	Status(ctx context.Context, status operationalStatus) error
}

type daemonSupervisor func(
	context.Context,
	<-chan struct{},
	func() (config.Config, error),
	generationBuilder,
	serviceNotifier,
	func(error),
	ActiveRunnerPolicy,
) error

type daemonApplicationDependencies struct {
	production       productionDependencies
	loadConfig       func(string, config.ReadFile) (config.Config, error)
	openRuntime      func(context.Context, config.Config, productionDependencies) (*productionRuntime, error)
	supervise        daemonSupervisor
	newShutdownTimer func(time.Duration) shutdownTimer
}

type shutdownTimer struct {
	expired <-chan time.Time
	stop    func()
}

func productionDaemonApplicationDependencies() daemonApplicationDependencies {
	production := newProductionDependencies()
	return daemonApplicationDependencies{
		production: production, loadConfig: loadConfigWithReader,
		openRuntime: openProductionRuntimeWithDependencies, supervise: supervise,
		newShutdownTimer: func(duration time.Duration) shutdownTimer {
			timer := time.NewTimer(duration)
			return shutdownTimer{expired: timer.C, stop: func() { timer.Stop() }}
		},
	}
}

// Application owns the immutable operating-system and service dependencies used by a daemon process.
type Application struct {
	dependencies daemonApplicationDependencies
}

// NewApplication creates a production daemon application without opening runtime resources.
func NewApplication() Application {
	return Application{dependencies: productionDaemonApplicationDependencies()}
}

// Run validates options, opens one runtime, and supervises it until the context ends.
func (application Application) Run(ctx context.Context, options Options) error {
	return runDaemon(ctx, options, application.dependencies)
}

func runDaemon(ctx context.Context, options Options, dependencies daemonApplicationDependencies) error {
	policy, err := resolveActiveRunnerPolicy(options.ActiveRunnerPolicy)
	if err != nil {
		return err
	}
	initial, err := dependencies.loadConfig(options.ConfigPath, dependencies.production.readFile)
	if err != nil {
		return err
	}
	runtime, err := dependencies.openRuntime(ctx, initial, dependencies.production)
	if err != nil {
		return err
	}
	completed := superviseRuntime(ctx, options, initial, runtime, policy, dependencies)
	return waitForRuntimeShutdown(ctx, policy, dependencies.newShutdownTimer, completed)
}

func superviseRuntime(
	ctx context.Context,
	options Options,
	initial config.Config,
	runtime *productionRuntime,
	policy ActiveRunnerPolicy,
	dependencies daemonApplicationDependencies,
) <-chan error {
	load := newGenerationConfigLoader(initial, func() (config.Config, error) {
		return dependencies.loadConfig(options.ConfigPath, dependencies.production.readFile)
	})
	build := func(buildContext context.Context, configured config.Config) (generation, error) {
		return runtime.Build(buildContext, configured)
	}
	notifier := newSystemdNotifier()
	if runtime.reporter != nil {
		runtime.reporter.notifier = notifier
	}
	completed := make(chan error, 1)
	go func() {
		superviseErr := dependencies.supervise(
			ctx, options.ReloadRequests, load, build, notifier, runtime.report, policy,
		)
		completed <- errors.Join(superviseErr, runtime.Close(context.WithoutCancel(ctx)))
	}()
	return completed
}

func waitForRuntimeShutdown(
	ctx context.Context,
	policy ActiveRunnerPolicy,
	newTimer func(time.Duration) shutdownTimer,
	completed <-chan error,
) error {
	if policy == TerminateActiveRunners {
		return <-completed
	}
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
	}
	timer := newTimer(leaveShutdownTimeout)
	defer timer.stop()
	select {
	case err := <-completed:
		return err
	case <-timer.expired:
		return nil
	}
}

type productionRuntime struct {
	logging     *loggingManager
	state       *productionState
	processGate *localLeadershipGate
	report      func(error)
	reporter    *productionRuntimeReporter
	build       func(context.Context, config.Config, *productionState, *localLeadershipGate, func(error)) (*maintenanceGeneration, error)
	firstBuild  bool
}

type runtimeGeneration struct{ generation }

func openProductionRuntimeWithDependencies(
	ctx context.Context,
	configured config.Config,
	dependencies productionDependencies,
) (*productionRuntime, error) {
	logging, err := newLoggingManager(configured.Log, dependencies.logDestinationFactory)
	if err != nil {
		return nil, fmt.Errorf("configure logging: %w", err)
	}
	state, err := dependencies.openState(ctx, configured, dependencies)
	if err != nil {
		return nil, errors.Join(err, logging.Close())
	}
	runtime := &productionRuntime{
		logging: logging, state: state, processGate: &localLeadershipGate{}, firstBuild: true,
		build: func(buildContext context.Context, buildConfig config.Config, buildState *productionState, processGate *localLeadershipGate, report func(error)) (*maintenanceGeneration, error) {
			return buildProductionRuntimeGeneration(
				buildContext, buildConfig, buildState, processGate, report, dependencies,
			)
		},
	}
	runtime.reporter = newProductionRuntimeReporter(logging, state, dependencies.currentTime)
	runtime.report = runtime.reporter.reportMaintenance
	return runtime, nil
}

func buildProductionRuntimeGeneration(
	ctx context.Context,
	configured config.Config,
	state *productionState,
	processGate *localLeadershipGate,
	report func(error),
	dependencies productionDependencies,
) (*maintenanceGeneration, error) {
	return dependencies.buildGeneration(ctx, configured, state, processGate, report, dependencies)
}

func (runtime *productionRuntime) Build(ctx context.Context, configured config.Config) (*runtimeGeneration, error) {
	if runtime.firstBuild {
		runtime.firstBuild = false
		built, err := runtime.build(ctx, configured, runtime.state, runtime.processGate, runtime.report)
		if err != nil {
			return nil, err
		}
		return &runtimeGeneration{generation: built}, nil
	}
	prepared, err := runtime.logging.Prepare(configured.Log)
	if err != nil {
		return nil, fmt.Errorf("prepare replacement logging: %w", err)
	}
	replacement, err := runtime.build(ctx, configured, runtime.state, runtime.processGate, runtime.report)
	if err != nil {
		return nil, errors.Join(err, prepared.Close())
	}
	return &runtimeGeneration{generation: &loggingGeneration{generation: replacement, prepared: prepared}}, nil
}

func (runtime *productionRuntime) Close(ctx context.Context) error {
	return errors.Join(
		runtime.withdrawMember(ctx),
		closeRuntimeResources(ctx, runtime.state.closers),
		runtime.logging.Close(),
	)
}

func (runtime *productionRuntime) withdrawMember(ctx context.Context) error {
	if err := runtime.state.store.WithdrawMemberCapacity(ctx, runtime.state.member); err != nil {
		return fmt.Errorf("withdraw member %q capacity: %w", runtime.state.member, err)
	}
	return nil
}

func newGenerationConfigLoader(
	initial config.Config,
	loadReplacement func() (config.Config, error),
) func() (config.Config, error) {
	firstLoad := true
	return func() (config.Config, error) {
		if firstLoad {
			firstLoad = false
			return initial, nil
		}
		replacement, loadErr := loadReplacement()
		if loadErr != nil {
			return config.Config{}, loadErr
		}
		if !reflect.DeepEqual(replacement.State, initial.State) {
			return config.Config{}, errors.New("state backend changes require a process restart")
		}
		return replacement, nil
	}
}
