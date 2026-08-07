package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

const (
	providerOperationTimeout = 30 * time.Second
	providerStartTimeout     = 5 * time.Minute
)

// WithOperationDeadlines enforces provider-neutral bounds without extending an
// earlier caller deadline.
func WithOperationDeadlines(provider ExecutionProvider) OperationDeadlineProvider {
	return OperationDeadlineProvider{provider: provider}
}

// OperationDeadlineProvider applies fixed provider-neutral operation bounds.
type OperationDeadlineProvider struct {
	provider ExecutionProvider
}

func (provider OperationDeadlineProvider) EndpointID() InventoryEndpointID {
	return provider.provider.EndpointID()
}

func (provider OperationDeadlineProvider) Validate(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	return provider.wrap("validate", provider.EndpointID(), provider.provider.Validate(ctx))
}

func (provider OperationDeadlineProvider) Start(ctx context.Context, request StartRequest) error {
	ctx, cancel := context.WithTimeout(ctx, providerStartTimeout)
	defer cancel()
	return provider.wrap("start", request.EndpointID, provider.provider.Start(ctx, request))
}

func (provider OperationDeadlineProvider) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	return provider.wrap("refresh", provider.EndpointID(), provider.provider.Refresh(ctx))
}

func (provider OperationDeadlineProvider) Inspect(ctx context.Context) ([]RunnerInstance, error) {
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	instances, err := provider.provider.Inspect(ctx)
	return instances, provider.wrap("inspect", provider.EndpointID(), err)
}

func (provider OperationDeadlineProvider) InspectEndpoint(
	ctx context.Context,
	endpointID InventoryEndpointID,
) ([]RunnerInstance, error) {
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	instances, err := provider.provider.InspectEndpoint(ctx, endpointID)
	return instances, provider.wrap("inspect", endpointID, err)
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
	ctx, cancel := context.WithTimeout(ctx, providerOperationTimeout)
	defer cancel()
	return provider.wrap("destroy", instance.EndpointID, provider.provider.Destroy(ctx, instance))
}

func (OperationDeadlineProvider) wrap(operation string, endpointID InventoryEndpointID, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("provider %s at endpoint %q: %w", operation, endpointID, err)
}
