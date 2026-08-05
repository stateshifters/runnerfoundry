package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
)

type Options struct {
	ConfigPath     string
	ReloadRequests <-chan struct{}
}

type serviceNotifier interface {
	Ready(ctx context.Context) error
	Reloading(ctx context.Context) error
}

var (
	loadDaemonConfig        = loadConfig
	openDaemonProduction    = openProductionRuntime
	superviseDaemonRuntime  = supervise
	newDaemonLoggingManager = newLoggingManager
	openDaemonState         = openProductionState
	buildDaemonGeneration   = buildProductionGeneration
)

func Run(ctx context.Context, options Options) (resultErr error) {
	initial, err := loadDaemonConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	runtime, err := openDaemonProduction(ctx, initial)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, runtime.Close(context.WithoutCancel(ctx))) }()
	load := newGenerationConfigLoader(initial, func() (config.Config, error) {
		return loadDaemonConfig(options.ConfigPath)
	})
	build := func(buildContext context.Context, configured config.Config) (generation, error) {
		return runtime.Build(buildContext, configured)
	}
	return superviseDaemonRuntime(ctx, options.ReloadRequests, load, build, newSystemdNotifier(), runtime.report)
}

type productionRuntime struct {
	logging     *loggingManager
	state       *productionState
	processGate *localLeadershipGate
	report      func(error)
	firstBuild  bool
}

type runtimeGeneration struct{ generation }

func openProductionRuntime(ctx context.Context, configured config.Config) (*productionRuntime, error) {
	logging, err := newDaemonLoggingManager(configured.Log, newProductionLogDestinationFactory())
	if err != nil {
		return nil, fmt.Errorf("configure logging: %w", err)
	}
	state, err := openDaemonState(ctx, configured)
	if err != nil {
		return nil, errors.Join(err, logging.Close())
	}
	runtime := &productionRuntime{logging: logging, state: state, processGate: &localLeadershipGate{}, firstBuild: true}
	runtime.report = func(err error) {
		logging.Logger().ErrorContext(ctx, "RunnerFoundry runtime error", "error", err)
	}
	return runtime, nil
}

func (runtime *productionRuntime) Build(ctx context.Context, configured config.Config) (*runtimeGeneration, error) {
	if runtime.firstBuild {
		runtime.firstBuild = false
		built, err := buildDaemonGeneration(ctx, configured, runtime.state, runtime.processGate, runtime.report)
		if err != nil {
			return nil, err
		}
		return &runtimeGeneration{generation: built}, nil
	}
	prepared, err := runtime.logging.Prepare(configured.Log)
	if err != nil {
		return nil, fmt.Errorf("prepare replacement logging: %w", err)
	}
	replacement, err := buildDaemonGeneration(ctx, configured, runtime.state, runtime.processGate, runtime.report)
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
	result := core.Result{
		Outcome: core.OutcomeFailure,
		Origin:  core.ResultOriginService,
		Reason:  "RunnerFoundry member shut down",
	}
	_, resultErr := runtime.state.store.SetMemberAttemptsResultPending(ctx, runtime.state.member, result)
	expireErr := runtime.state.store.ExpireMember(ctx, runtime.state.member)
	return errors.Join(resultErr, expireErr)
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
