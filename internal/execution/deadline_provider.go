package execution

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

const (
	providerOperationTimeout = 30 * time.Second
	providerStartTimeout     = 5 * time.Minute
)

// WithOperationDeadlines enforces provider-neutral bounds without extending an
// earlier caller deadline.
func WithOperationDeadlines(
	provider ExecutionProvider,
	name ProviderName,
	observers ...func(ProviderActivity),
) OperationDeadlineProvider {
	return WithOperationDeadlinesAndMetadata(provider, ProviderMetadata{Name: name}, observers...)
}

type ProviderMetadata struct {
	Name           ProviderName
	ComposeService string
}

func WithOperationDeadlinesAndMetadata(
	provider ExecutionProvider,
	metadata ProviderMetadata,
	observers ...func(ProviderActivity),
) OperationDeadlineProvider {
	var observer func(ProviderActivity)
	if len(observers) != 0 {
		observer = observers[0]
	}
	return OperationDeadlineProvider{provider: provider, metadata: metadata, observer: observer, currentTime: time.Now}
}

// ProviderActivity describes one provider call without exposing its command or
// bootstrap material.
type ProviderActivity struct {
	Operation      string
	Outcome        string
	Provider       ProviderName
	ComposeService string
	JobID          core.JobID
	RunnerID       string
	RunnerPool     core.RunnerPool
	EndpointID     InventoryEndpointID
	Duration       time.Duration
	Err            error
	OriginPC       uintptr
}

// OperationDeadlineProvider applies fixed provider-neutral operation bounds.
type OperationDeadlineProvider struct {
	provider    ExecutionProvider
	metadata    ProviderMetadata
	observer    func(ProviderActivity)
	currentTime func() time.Time
}

func (provider OperationDeadlineProvider) EndpointID() InventoryEndpointID {
	return provider.provider.EndpointID()
}

func (provider OperationDeadlineProvider) Validate(ctx context.Context) error {
	started := provider.begin("validate", "", provider.EndpointID(), core.JobID{}, "")
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	err := provider.wrap("validate", provider.EndpointID(), provider.provider.Validate(ctx))
	provider.finish("validate", "", provider.EndpointID(), core.JobID{}, "", started, err)
	return err
}

func (provider OperationDeadlineProvider) Start(ctx context.Context, request StartRequest) error {
	started := provider.begin("start", request.RunnerPool, request.EndpointID, request.JobID, string(request.RunnerName))
	ctx, cancel := context.WithTimeout(ctx, providerStartTimeout)
	defer cancel()
	err := provider.wrap("start", request.EndpointID, provider.provider.Start(ctx, request))
	provider.finish("start", request.RunnerPool, request.EndpointID, request.JobID, string(request.RunnerName), started, err)
	return err
}

func (provider OperationDeadlineProvider) Refresh(ctx context.Context) error {
	started := provider.begin("refresh", "", provider.EndpointID(), core.JobID{}, "")
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	err := provider.wrap("refresh", provider.EndpointID(), provider.provider.Refresh(ctx))
	provider.finish("refresh", "", provider.EndpointID(), core.JobID{}, "", started, err)
	return err
}

func (provider OperationDeadlineProvider) Inspect(ctx context.Context) ([]RunnerInstance, error) {
	started := provider.begin("inspect", "", provider.EndpointID(), core.JobID{}, "")
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	instances, err := provider.provider.Inspect(ctx)
	err = provider.wrap("inspect", provider.EndpointID(), err)
	provider.finish("inspect", "", provider.EndpointID(), core.JobID{}, "", started, err)
	return instances, err
}

func (provider OperationDeadlineProvider) InspectEndpoint(
	ctx context.Context,
	endpointID InventoryEndpointID,
) ([]RunnerInstance, error) {
	started := provider.begin("inspect", "", endpointID, core.JobID{}, "")
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	instances, err := provider.provider.InspectEndpoint(ctx, endpointID)
	err = provider.wrap("inspect", endpointID, err)
	provider.finish("inspect", "", endpointID, core.JobID{}, "", started, err)
	return instances, err
}

func (provider OperationDeadlineProvider) KnowsEndpoint(endpointID InventoryEndpointID) bool {
	return provider.provider.KnowsEndpoint(endpointID)
}

func (provider OperationDeadlineProvider) CanInspectEndpointForMember(
	endpointID InventoryEndpointID,
	memberID core.MemberID,
) bool {
	return provider.provider.CanInspectEndpointForMember(endpointID, memberID)
}

func (provider OperationDeadlineProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	started := provider.begin("destroy", instance.RunnerPool, instance.EndpointID, core.JobID{}, instance.ID)
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	err := provider.wrap("destroy", instance.EndpointID, provider.provider.Destroy(ctx, instance))
	provider.finish("destroy", instance.RunnerPool, instance.EndpointID, core.JobID{}, instance.ID, started, err)
	return err
}

func (provider OperationDeadlineProvider) begin(
	operation string,
	runnerPool core.RunnerPool,
	endpointID InventoryEndpointID,
	jobID core.JobID,
	runnerID string,
) time.Time {
	started := provider.currentTime()
	provider.report(ProviderActivity{Operation: operation, Outcome: "started", Provider: provider.metadata.Name,
		ComposeService: provider.metadata.ComposeService,
		JobID:          jobID, RunnerID: runnerID, RunnerPool: runnerPool, EndpointID: endpointID,
		OriginPC: providerOperationOrigin()})
	return started
}

func (provider OperationDeadlineProvider) finish(
	operation string,
	runnerPool core.RunnerPool,
	endpointID InventoryEndpointID,
	jobID core.JobID,
	runnerID string,
	started time.Time,
	err error,
) {
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	provider.report(ProviderActivity{Operation: operation, Outcome: outcome, Provider: provider.metadata.Name,
		ComposeService: provider.metadata.ComposeService, JobID: jobID,
		RunnerID: runnerID, RunnerPool: runnerPool, EndpointID: endpointID,
		Duration: provider.currentTime().Sub(started), Err: err,
		OriginPC: providerOperationOrigin()})
}

func providerOperationOrigin() uintptr {
	programCounters := make([]uintptr, 1)
	runtime.Callers(3, programCounters)
	return programCounters[0]
}

func (provider OperationDeadlineProvider) report(activity ProviderActivity) {
	if provider.observer != nil {
		provider.observer(activity)
	}
}

func (OperationDeadlineProvider) wrap(operation string, endpointID InventoryEndpointID, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("provider %s at endpoint %q: %w", operation, endpointID, err)
}
