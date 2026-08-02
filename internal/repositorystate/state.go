// Package repositorystate defines the durable GitHub repository authority and
// registration snapshot shared by standalone and clustered state stores.
package repositorystate

import (
	"context"
	"errors"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
)

// ErrControllerFenced indicates that a newer controller epoch owns mutations.
var ErrControllerFenced = errors.New("repository-state controller epoch changed")

type Control struct {
	AppID                   int64
	DiscoveryGeneration     int64
	LastSuccessfulDiscovery time.Time
	ControllerEpoch         int64
	Initialized             bool
}

type Repository struct {
	AppID              int64
	URL                string
	InstallationID     githubapp.InstallationID
	LastSeenGeneration int64
}

type Registration struct {
	AppID            int64
	RepositoryURL    string
	ScaleSetName     string
	RunnerGroup      string
	GitHubScaleSetID int
}

type Snapshot struct {
	Control       Control
	Repositories  []Repository
	Registrations []Registration
}

// Store persists one GitHub App's authoritative discovery and registration
// snapshot. Every mutation is fenced by the epoch returned by StartController.
type Store interface {
	StartController(ctx context.Context, appID int64) (Snapshot, error)
	LoadSnapshot(ctx context.Context, appID int64) (Snapshot, error)
	ApplyDiscovery(
		ctx context.Context,
		appID int64,
		controllerEpoch int64,
		installationIDs []githubapp.InstallationID,
		failedInstallationIDs []githubapp.InstallationID,
		repositories []Repository,
		completedAt time.Time,
	) (Snapshot, error)
	UpsertRegistration(ctx context.Context, appID int64, controllerEpoch int64, registration Registration) error
	DeleteRegistration(ctx context.Context, appID int64, controllerEpoch int64, repositoryURL string, scaleSetName string) error
}
