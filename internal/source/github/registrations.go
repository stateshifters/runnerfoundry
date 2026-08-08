package github

import (
	"context"
	"fmt"
	"sort"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func canonicalRegistrationSnapshot(snapshot RegistrationSnapshot) (RegistrationSnapshot, error) {
	active := append([]Registration(nil), snapshot.Active...)
	sort.Slice(active, func(firstIndex, secondIndex int) bool {
		return lessActiveRegistration(active[firstIndex], active[secondIndex])
	})
	for index := 1; index < len(active); index++ {
		previous := active[index-1]
		current := active[index]
		if previous.RepositoryURL == current.RepositoryURL && previous.RunnerPool == current.RunnerPool {
			return RegistrationSnapshot{}, fmt.Errorf(
				"duplicate GitHub registration for repository %q and Runner Pool %q",
				current.RepositoryURL,
				current.RunnerPool,
			)
		}
	}
	owned := append([]Registration(nil), snapshot.Owned...)
	sort.Slice(owned, func(firstIndex, secondIndex int) bool {
		return lessOwnedRegistration(owned[firstIndex], owned[secondIndex])
	})
	return RegistrationSnapshot{Active: active, Owned: owned}, nil
}

func lessActiveRegistration(first, second Registration) bool {
	if first.RepositoryURL != second.RepositoryURL {
		return first.RepositoryURL < second.RepositoryURL
	}
	if first.RunnerPool != second.RunnerPool {
		return first.RunnerPool < second.RunnerPool
	}
	if first.InstallationID != second.InstallationID {
		return first.InstallationID < second.InstallationID
	}
	if first.RunnerGroupID != second.RunnerGroupID {
		return first.RunnerGroupID < second.RunnerGroupID
	}
	return first.ScaleSetID < second.ScaleSetID
}

func lessOwnedRegistration(first, second Registration) bool {
	if first.RepositoryURL != second.RepositoryURL {
		return first.RepositoryURL < second.RepositoryURL
	}
	if first.ScaleSetID != second.ScaleSetID {
		return first.ScaleSetID < second.ScaleSetID
	}
	return lessActiveRegistration(first, second)
}

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
