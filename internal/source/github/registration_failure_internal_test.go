package github

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestPruneFailureClassification(t *testing.T) {
	wantErr := errors.New("prune failed")
	failure := PruneFailure{Err: wantErr}
	if failure.Error() != wantErr.Error() || !IsPruneFailure(failure) || IsPruneFailure(wantErr) {
		t.Fatalf("prune failure classification = (%q, %t, %t)",
			failure.Error(), IsPruneFailure(failure), IsPruneFailure(wantErr))
	}
}

func TestPruningReportsOwnershipReadAndDeleteFailures(t *testing.T) {
	wantErr := errors.New("persistence failed")
	record := Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	repository := &registrationFailureRepository{active: []Registration{record}, ownedErr: wantErr}
	manager := newRegistrationFailureManager(t, repository)
	if err := manager.pruneReconciledRegistrations(context.Background(), nil, []Registration{record}); !errors.Is(err, wantErr) {
		t.Fatalf("ownership read error = %v, want %v", err, wantErr)
	}
	repository.ownedErr = nil
	repository.deleteErr = wantErr
	if err := manager.deletePrunedOwnership(context.Background(), []Registration{record}, nil); !errors.Is(err, wantErr) {
		t.Fatalf("ownership delete error = %v, want %v", err, wantErr)
	}
}

func TestPruningRejectsDuplicateOwnershipEvidence(t *testing.T) {
	t.Parallel()

	record := Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	repository := &registrationFailureRepository{owned: []Registration{record, record}}
	manager := newRegistrationFailureManager(t, repository)

	err := manager.pruneReconciledRegistrations(context.Background(), nil, nil)
	if err == nil || !IsPruneFailure(err) {
		t.Fatalf("duplicate ownership error = %v, want PruneFailure", err)
	}
}

type registrationFailureRepository struct {
	active    []Registration
	owned     []Registration
	ownedErr  error
	deleteErr error
}

func (repository *registrationFailureRepository) Registrations(context.Context) ([]Registration, error) {
	return repository.active, nil
}
func (repository *registrationFailureRepository) OwnedRegistrations(context.Context) ([]Registration, error) {
	return repository.owned, repository.ownedErr
}
func (*registrationFailureRepository) UpsertRegistration(context.Context, Registration) error {
	return nil
}
func (*registrationFailureRepository) DeleteActiveRegistration(context.Context, Registration) error {
	return nil
}
func (repository *registrationFailureRepository) DeleteOwnedRegistration(context.Context, Registration) error {
	return repository.deleteErr
}
func (*registrationFailureRepository) DeleteRegistrationState(context.Context, Registration) error {
	return nil
}

type registrationFailureDiscoverer struct{}

func (registrationFailureDiscoverer) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{}, nil
}

func newRegistrationFailureManager(t *testing.T, repository RegistrationRepository) *Manager {
	t.Helper()
	manager, err := NewManager(registrationFailureDiscoverer{}, func(context.Context, transport.Repository) (ManagerClient, error) {
		return ManagerClient{}, nil
	}, repository, ManagerObserver{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return manager
}
