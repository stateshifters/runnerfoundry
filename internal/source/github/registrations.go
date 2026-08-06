package github

import (
	"context"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

// Registration is the durable GitHub routing record owned by this source.
type Registration struct {
	RepositoryURL  string
	RunnerPool     core.RunnerPool
	InstallationID transport.InstallationID
	RunnerGroupID  transport.RunnerGroupID
	ScaleSetID     transport.ScaleSetID
}

// RegistrationSnapshot atomically separates current routes from the durable
// ownership evidence used to safely prune GitHub runner scale sets.
type RegistrationSnapshot struct {
	Active []Registration
	Owned  []Registration
}

// RegistrationRepository replaces a complete discovery snapshot in one
// transaction and publishes only committed snapshots to member routes.
type RegistrationRepository interface {
	ReplaceRegistrationSnapshot(ctx context.Context, snapshot RegistrationSnapshot) error
	RegistrationSnapshot(ctx context.Context) (RegistrationSnapshot, error)
}
