package execution

import "context"

// RunnerLauncher validates and starts ephemeral runner environments.
type RunnerLauncher interface {
	Validate(ctx context.Context) error
	Start(ctx context.Context, request StartRequest) error
}

// RunnerInventory observes and destroys ephemeral runner environments.
type RunnerInventory interface {
	Inspect(ctx context.Context, runnerPool string) ([]RunnerInstance, error)
	Destroy(ctx context.Context, instance RunnerInstance) error
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

type StartRequest struct {
	RunnerPool string
	RunnerName string
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
	RunnerName string
	State      RunnerState
	origin     *runnerInventoryOrigin
}

type runnerInventoryOrigin struct {
	inventory RunnerInventory
}
