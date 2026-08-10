package github

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

// RunnerPoolDeclaration is one member's configured GitHub Runner Pool and
// runner-group name. It is source-owned cluster intent, not executable capacity.
type RunnerPoolDeclaration struct {
	RunnerPool  core.RunnerPool
	RunnerGroup string
}

// MemberRunnerPoolDeclaration identifies the live member that published a
// GitHub Runner Pool declaration.
type MemberRunnerPoolDeclaration struct {
	MemberID    core.MemberID
	RunnerPool  core.RunnerPool
	RunnerGroup string
}

// RunnerPoolDeclarationRepository persists member-local GitHub declarations
// and reads declarations belonging to members inside the cluster liveness window.
type RunnerPoolDeclarationRepository interface {
	ReplaceGitHubRunnerPoolDeclarations(ctx context.Context, memberID core.MemberID, declarations []RunnerPoolDeclaration) error
	LiveGitHubRunnerPoolDeclarations(ctx context.Context, liveMemberCutoff time.Time) ([]MemberRunnerPoolDeclaration, error)
}

var ErrRunnerPoolDeclarationPersistenceUnavailable = errors.New("GitHub Runner Pool declaration persistence is unavailable")

func (manager *Manager) UsesSharedRunnerPoolDeclarations() bool {
	_, available := manager.registrations.(RunnerPoolDeclarationRepository)
	return available
}

// RunnerPoolDeclarationSnapshot is the usable cluster declaration union plus
// pools whose live members disagree about the runner-group name.
type RunnerPoolDeclarationSnapshot struct {
	RunnerPools           []RunnerPoolConfig
	ConflictedRunnerPools []core.RunnerPool
}

func (manager *Manager) PublishRunnerPoolDeclarations(
	ctx context.Context,
	memberID core.MemberID,
	config ManagerConfig,
) error {
	if err := validateManagerConfig(config); err != nil {
		return err
	}
	repository, available := manager.registrations.(RunnerPoolDeclarationRepository)
	if !available {
		return nil
	}
	declarations := make([]RunnerPoolDeclaration, 0, len(config.RunnerPools))
	for _, runnerPool := range config.RunnerPools {
		declarations = append(declarations, RunnerPoolDeclaration(runnerPool))
	}
	if err := repository.ReplaceGitHubRunnerPoolDeclarations(ctx, memberID, declarations); err != nil {
		return fmt.Errorf("publish GitHub Runner Pool declarations: %w", err)
	}
	return nil
}

func (manager *Manager) ClusterRunnerPools(
	ctx context.Context,
	liveMemberCutoff time.Time,
) (RunnerPoolDeclarationSnapshot, error) {
	repository, available := manager.registrations.(RunnerPoolDeclarationRepository)
	if !available {
		return RunnerPoolDeclarationSnapshot{}, ErrRunnerPoolDeclarationPersistenceUnavailable
	}
	declarations, err := repository.LiveGitHubRunnerPoolDeclarations(ctx, liveMemberCutoff)
	if err != nil {
		return RunnerPoolDeclarationSnapshot{}, fmt.Errorf("load live GitHub Runner Pool declarations: %w", err)
	}
	groups, conflicts := groupRunnerPoolDeclarations(declarations)
	return runnerPoolDeclarationSnapshot(groups, conflicts), nil
}

func groupRunnerPoolDeclarations(
	declarations []MemberRunnerPoolDeclaration,
) (map[core.RunnerPool]string, map[core.RunnerPool]struct{}) {
	groups := make(map[core.RunnerPool]string)
	conflicts := make(map[core.RunnerPool]struct{})
	for _, declaration := range declarations {
		group, exists := groups[declaration.RunnerPool]
		if exists && group != declaration.RunnerGroup {
			conflicts[declaration.RunnerPool] = struct{}{}
			continue
		}
		groups[declaration.RunnerPool] = declaration.RunnerGroup
	}
	return groups, conflicts
}

func runnerPoolDeclarationSnapshot(
	groups map[core.RunnerPool]string,
	conflicts map[core.RunnerPool]struct{},
) RunnerPoolDeclarationSnapshot {
	snapshot := RunnerPoolDeclarationSnapshot{}
	for runnerPool, runnerGroup := range groups {
		if _, conflicted := conflicts[runnerPool]; conflicted {
			snapshot.ConflictedRunnerPools = append(snapshot.ConflictedRunnerPools, runnerPool)
			continue
		}
		snapshot.RunnerPools = append(snapshot.RunnerPools, RunnerPoolConfig{RunnerPool: runnerPool, RunnerGroup: runnerGroup})
	}
	sort.Slice(snapshot.RunnerPools, func(first, second int) bool {
		return snapshot.RunnerPools[first].RunnerPool < snapshot.RunnerPools[second].RunnerPool
	})
	sort.Slice(snapshot.ConflictedRunnerPools, func(first, second int) bool {
		return snapshot.ConflictedRunnerPools[first] < snapshot.ConflictedRunnerPools[second]
	})
	return snapshot
}

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
	for index := 1; index < len(owned); index++ {
		previous := owned[index-1]
		current := owned[index]
		if previous.RepositoryURL == current.RepositoryURL && previous.ScaleSetID == current.ScaleSetID {
			return RegistrationSnapshot{}, fmt.Errorf(
				"duplicate GitHub ownership for repository %q and scale set %d",
				current.RepositoryURL,
				current.ScaleSetID,
			)
		}
	}
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

// RegistrationSnapshot is a point-in-time view of active routes and durable
// ownership evidence. It is a read model; records are persisted independently.
type RegistrationSnapshot struct {
	Active []Registration
	Owned  []Registration
}

// RegistrationRepository persists GitHub communication state without exposing
// the selected database backend. Upserts commit an active route and its
// ownership evidence together. Guarded deletes match the complete stored
// fingerprint so a stale source operation cannot remove a newer replacement.
type registrationReader interface {
	Registrations(ctx context.Context) ([]Registration, error)
	OwnedRegistrations(ctx context.Context) ([]Registration, error)
}

type registrationWriter interface {
	UpsertRegistration(ctx context.Context, registration Registration) error
	DeleteActiveRegistration(ctx context.Context, registration Registration) error
	DeleteOwnedRegistration(ctx context.Context, registration Registration) error
	DeleteRegistrationState(ctx context.Context, registration Registration) error
}

type RegistrationRepository interface {
	registrationReader
	registrationWriter
}

// ActiveRegistrations returns the canonical committed routes without reading
// durable prune ownership.
func (manager *Manager) ActiveRegistrations(ctx context.Context) ([]Registration, error) {
	active, err := manager.registrations.Registrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("load active GitHub registrations: %w", err)
	}
	canonical, err := canonicalRegistrationSnapshot(RegistrationSnapshot{Active: active})
	if err != nil {
		return nil, fmt.Errorf("validate active GitHub registrations: %w", err)
	}
	return canonical.Active, nil
}

// RegistrationSnapshot returns the currently committed registration facts.
// The returned slices are defensive copies so callers cannot mutate durable
// repository-owned state.
func (manager *Manager) RegistrationSnapshot(ctx context.Context) (RegistrationSnapshot, error) {
	active, err := manager.registrations.Registrations(ctx)
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("load active GitHub registrations: %w", err)
	}
	owned, err := manager.registrations.OwnedRegistrations(ctx)
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("load owned GitHub registrations: %w", err)
	}
	canonical, err := canonicalRegistrationSnapshot(RegistrationSnapshot{Active: active, Owned: owned})
	if err != nil {
		return RegistrationSnapshot{}, fmt.Errorf("validate GitHub registration snapshot: %w", err)
	}
	return canonical, nil
}
