package github

import (
	"context"

	"github.com/meza/RunnerFoundry/internal/core"
)

// Registration is the durable GitHub routing record owned by this source.
type Registration struct {
	RepositoryURL  string
	RunnerPool     core.RunnerPool
	InstallationID int64
	RunnerGroupID  int64
	ScaleSetID     int64
}

// RegistrationRepository replaces a complete discovery snapshot in one
// transaction and publishes only committed snapshots to member routes.
type RegistrationRepository interface {
	ReplaceRegistrations(ctx context.Context, registrations []Registration) error
	Registrations(ctx context.Context) ([]Registration, error)
}
