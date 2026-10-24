package daemon

import (
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

var errUnknownSourceAdapter = errors.New("unknown source adapter")

type sourceOperationsRegistration struct {
	adapter    core.SourceAdapter
	operations core.RunnerBootstrapProvider
}

type sourceOperationsResolver struct {
	byAdapter map[core.SourceAdapter]core.SourceOperations
}

func newSourceOperationsResolver(
	registrations []sourceOperationsRegistration,
) (*sourceOperationsResolver, error) {
	byAdapter := make(map[core.SourceAdapter]core.SourceOperations, len(registrations))
	for _, registration := range registrations {
		operations := core.SourceOperations{RunnerBootstrapProvider: registration.operations}
		if registration.adapter == "" || !operations.IsValid() {
			return nil, errors.New("source adapter namespace and operations are required")
		}
		if _, duplicate := byAdapter[registration.adapter]; duplicate {
			return nil, fmt.Errorf("duplicate source adapter namespace %q", registration.adapter)
		}
		byAdapter[registration.adapter] = operations
	}
	return &sourceOperationsResolver{byAdapter: byAdapter}, nil
}

func (resolver *sourceOperationsResolver) ResolveSourceOperations(
	adapter core.SourceAdapter,
) (core.SourceOperations, error) {
	operations, configured := resolver.byAdapter[adapter]
	if !configured {
		return core.SourceOperations{}, fmt.Errorf("%w %q", errUnknownSourceAdapter, adapter)
	}
	return operations, nil
}

var _ core.SourceOperationsResolver = (*sourceOperationsResolver)(nil)
