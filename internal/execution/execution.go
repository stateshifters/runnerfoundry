package execution

import (
	"context"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

// InventoryEndpointID identifies one concrete provider endpoint. It is stable
// across configuration reloads so processing recovery can revisit the exact
// endpoint that accepted a claim.
type InventoryEndpointID = core.InventoryEndpointID

// HostIdentity is the stable member-local host name used when deriving provider
// endpoint identities across reloads.
type HostIdentity string

// RunnerLauncher validates and starts ephemeral runner environments.
type RunnerLauncher interface {
	EndpointID() InventoryEndpointID
	Validate(ctx context.Context) error
	Start(ctx context.Context, request StartRequest) error
}

// InventorySnapshot owns the cycle-wide inventory view. Refresh establishes a
// complete view for the current maintenance cycle. Inspect returns that view,
// but may obtain an initial view before the first successful Refresh.
// Implementations fail closed rather than return partial observations.
type InventorySnapshot interface {
	Refresh(ctx context.Context) error
	Inspect(ctx context.Context) ([]RunnerInstance, error)
}

// EndpointInventory provides fresh endpoint-specific recovery evidence and
// destroys only instances that belong to a known endpoint.
type EndpointInventory interface {
	EndpointID() InventoryEndpointID
	InspectEndpoint(ctx context.Context, endpointID InventoryEndpointID) ([]RunnerInstance, error)
	KnowsEndpoint(endpointID InventoryEndpointID) bool
	CanInspectEndpointForMember(endpointID InventoryEndpointID, memberID core.MemberID) bool
	Destroy(ctx context.Context, instance RunnerInstance) error
}

// RunnerInventory combines cycle snapshots with endpoint-specific recovery and
// destruction.
type RunnerInventory interface {
	InventorySnapshot
	EndpointInventory
}

// ExecutionProvider combines the launch and inventory boundaries implemented by
// a configured provider. Implementations and deadline wrappers must support
// concurrent calls for different Runner Pools. Calls for one Runner Pool are
// serialized by the daemon's pool worker and maintenance gate.
type ExecutionProvider interface {
	RunnerLauncher
	RunnerInventory
}

// Bootstrap is source-owned runner initialization data. Execution providers
// transport it without interpreting it.
type Bootstrap struct {
	content []byte
}

// NewBootstrap takes a defensive copy of source-owned bootstrap data.
func NewBootstrap(content []byte) Bootstrap {
	return Bootstrap{content: append([]byte(nil), content...)}
}

// StartRequest binds a runner to exactly one configured Runner Pool and
// inventory endpoint. Bootstrap remains opaque to the execution provider.
type StartRequest struct {
	JobID      core.JobID
	RunnerPool core.RunnerPool
	EndpointID InventoryEndpointID
	RunnerName core.RunnerName
	Bootstrap  Bootstrap
}

// ProviderName identifies the configured execution implementation in operator
// diagnostics without relying on reflection or endpoint parsing.
type ProviderName string

// RunnerState is the provider-neutral lifecycle observation used by recovery.
type RunnerState string

const (
	RunnerStateRunning   RunnerState = "running"
	RunnerStateCompleted RunnerState = "completed"
	RunnerStateUnknown   RunnerState = "unknown"
)

// RunnerInstance is one provider observation. Aggregate inventories attach a
// transient origin so Destroy can route it back to the observing endpoint.
type RunnerInstance struct {
	ID         string
	RunnerName core.RunnerName
	RunnerPool core.RunnerPool
	EndpointID InventoryEndpointID
	State      RunnerState
	origin     *runnerInventoryOrigin
}

type runnerInventoryOrigin struct {
	inventory RunnerInventory
}
