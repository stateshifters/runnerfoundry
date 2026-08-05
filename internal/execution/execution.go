package execution

import "context"

// ExecutionProvider owns the lifecycle of ephemeral runner environments.
type ExecutionProvider interface {
	Validate(ctx context.Context) error
	Start(ctx context.Context, request StartRequest) error
	Inspect(ctx context.Context, runnerPool string) ([]RunnerInstance, error)
	Destroy(ctx context.Context, instance RunnerInstance) error
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
}
