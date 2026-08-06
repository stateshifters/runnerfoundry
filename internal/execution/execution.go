package execution

import (
	"context"

	"github.com/meza/RunnerFoundry/internal/core"
)

// InventoryEndpointID identifies one concrete provider endpoint. It is stable
// across configuration reloads so processing recovery can revisit the exact
// endpoint that accepted a claim.
type InventoryEndpointID = core.InventoryEndpointID

type HostIdentity string

// RunnerLauncher validates and starts ephemeral runner environments.
type RunnerLauncher interface {
	EndpointID() InventoryEndpointID
	Validate(ctx context.Context) error
	Start(ctx context.Context, request StartRequest) error
}

// RunnerInventory observes and destroys ephemeral runner environments.
// Refresh obtains one complete endpoint snapshot for a maintenance cycle;
// Inspect partitions that snapshot without I/O. InspectEndpoint deliberately
// obtains fresh endpoint-specific evidence for recovery. Implementations fail
// closed when an endpoint is unavailable or returns an incomplete snapshot.
type InventorySnapshot interface {
	Refresh(ctx context.Context) error
	Inspect(ctx context.Context) ([]RunnerInstance, error)
}

type EndpointInventory interface {
	EndpointID() InventoryEndpointID
	InspectEndpoint(ctx context.Context, endpointID InventoryEndpointID) ([]RunnerInstance, error)
	KnowsEndpoint(endpointID InventoryEndpointID) bool
	Destroy(ctx context.Context, instance RunnerInstance) error
}

type RunnerInventory interface {
	InventorySnapshot
	EndpointInventory
}

// ExecutionProvider combines the launch and inventory boundaries implemented by
// a configured provider.
type ExecutionProvider interface {
	RunnerLauncher
	RunnerInventory
}

// Bootstrap is source-owned runner initialization data. Execution providers
// transport it without interpreting it.
type Bootstrap struct {
	content []byte
}

func NewBootstrap(content []byte) Bootstrap {
	return Bootstrap{content: append([]byte(nil), content...)}
}

// StartRequest binds a runner to exactly one configured Runner Pool and
// inventory endpoint. Bootstrap remains opaque to the execution provider.
type StartRequest struct {
	RunnerPool core.RunnerPool
	EndpointID InventoryEndpointID
	RunnerName core.RunnerName
	Bootstrap  Bootstrap
}

type RunnerState string

const (
	RunnerStateRunning   RunnerState = "running"
	RunnerStateCompleted RunnerState = "completed"
	RunnerStateUnknown   RunnerState = "unknown"
)

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
