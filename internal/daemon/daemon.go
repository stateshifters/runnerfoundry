package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/meza/RunnerFoundry/internal/config"
)

type Options struct {
	ConfigPath         string
	ReloadRequests     <-chan struct{}
	ActiveRunnerPolicy ActiveRunnerPolicy
}

type serviceNotifier interface {
	Ready(ctx context.Context) error
	Reloading(ctx context.Context) error
}

type daemonApplicationDependencies struct {
	production  productionDependencies
	loadConfig  func(string, config.ReadFile) (config.Config, error)
	openRuntime func(context.Context, config.Config, productionDependencies) (*productionRuntime, error)
	supervise   func(context.Context, <-chan struct{}, func() (config.Config, error), generationBuilder, serviceNotifier, func(error), ActiveRunnerPolicy) error
}

func productionDaemonApplicationDependencies() daemonApplicationDependencies {
	production := newProductionDependencies()
	return daemonApplicationDependencies{
		production: production, loadConfig: loadConfigWithReader,
		openRuntime: openProductionRuntimeWithDependencies, supervise: supervise,
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

func runDaemon(ctx context.Context, options Options, dependencies daemonApplicationDependencies) (resultErr error) {
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
	defer func() { resultErr = errors.Join(resultErr, runtime.Close(context.WithoutCancel(ctx))) }()
	load := newGenerationConfigLoader(initial, func() (config.Config, error) {
		return dependencies.loadConfig(options.ConfigPath, dependencies.production.readFile)
	})
	build := func(buildContext context.Context, configured config.Config) (generation, error) {
		return runtime.Build(buildContext, configured)
	}
	return dependencies.supervise(ctx, options.ReloadRequests, load, build, newSystemdNotifier(), runtime.report, policy)
}

type productionRuntime struct {
	logging     *loggingManager
	state       *productionState
	processGate *localLeadershipGate
	report      func(error)
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
	runtime.report = newProductionRuntimeReporter(ctx, logging, state)
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

func newProductionRuntimeReporter(
	ctx context.Context,
	logging *loggingManager,
	state *productionState,
) func(error) {
	return func(err error) {
		if err == nil {
			logging.Logger().InfoContext(ctx, "RunnerFoundry runtime operation recovered",
				slog.String("operation", "maintenance_cycle"), slog.String("member", string(state.member)),
				slog.String("outcome", "recovered"))
			return
		}
		category := "operation"
		if errors.Is(err, context.Canceled) {
			category = "canceled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			category = "deadline"
		}
		logging.Logger().ErrorContext(ctx, "RunnerFoundry runtime operation failed",
			slog.String("operation", "maintenance_cycle"), slog.String("member", string(state.member)),
			slog.String("outcome", "failed"), slog.String("error_category", category), slog.Any("error", err))
	}
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
