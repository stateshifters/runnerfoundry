package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

// AppScopedRegistrationRepository binds otherwise global adapter storage to
// one credential identity and prevents cross-App reads and guarded mutations.
type AppScopedRegistrationRepository struct {
	repository RegistrationRepository
	appID      AppID
}

func (scoped *AppScopedRegistrationRepository) githubAppID() AppID { return scoped.appID }

func (scoped *AppScopedRegistrationRepository) usesSharedRunnerPoolDeclarations() bool {
	_, available := scoped.repository.(RunnerPoolDeclarationRepository)
	return available
}

func (scoped *AppScopedRegistrationRepository) ReplaceGitHubRunnerPoolDeclarations(
	ctx context.Context,
	memberID core.MemberID,
	declarations []RunnerPoolDeclaration,
) error {
	repository, ok := scoped.repository.(RunnerPoolDeclarationRepository)
	if !ok {
		return ErrRunnerPoolDeclarationPersistenceUnavailable
	}
	if err := repository.ReplaceGitHubRunnerPoolDeclarations(ctx, memberID, declarations); err != nil {
		return fmt.Errorf("replace App-scoped GitHub Runner Pool declarations: %w", err)
	}
	return nil
}

func (scoped *AppScopedRegistrationRepository) LiveGitHubRunnerPoolDeclarations(
	ctx context.Context,
	liveMemberCutoff time.Time,
) ([]MemberRunnerPoolDeclaration, error) {
	repository, ok := scoped.repository.(RunnerPoolDeclarationRepository)
	if !ok {
		return nil, ErrRunnerPoolDeclarationPersistenceUnavailable
	}
	declarations, err := repository.LiveGitHubRunnerPoolDeclarations(ctx, liveMemberCutoff)
	if err != nil {
		return nil, fmt.Errorf("load App-scoped GitHub Runner Pool declarations: %w", err)
	}
	return declarations, nil
}

// ScopeRegistrationRepository isolates one App's registration and ownership
// records while retaining repository URL and Runner Pool as the global route key.
func ScopeRegistrationRepository(repository RegistrationRepository, appID AppID) (*AppScopedRegistrationRepository, error) {
	if repository == nil {
		return nil, errors.New("GitHub registration repository is required")
	}
	if appID <= 0 {
		return nil, errors.New("GitHub App ID must be positive")
	}
	return &AppScopedRegistrationRepository{repository: repository, appID: appID}, nil
}

func (scoped *AppScopedRegistrationRepository) Registrations(ctx context.Context) ([]Registration, error) {
	records, err := scoped.repository.Registrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("load App-scoped active GitHub registrations: %w", err)
	}
	return scoped.filter(records), nil
}

func (scoped *AppScopedRegistrationRepository) OwnedRegistrations(ctx context.Context) ([]Registration, error) {
	records, err := scoped.repository.OwnedRegistrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("load App-scoped GitHub ownership: %w", err)
	}
	return scoped.filter(records), nil
}

func (scoped *AppScopedRegistrationRepository) UpsertRegistration(ctx context.Context, record Registration) error {
	stamped, err := scoped.stamp(record)
	if err != nil {
		return err
	}
	if err = scoped.repository.UpsertRegistration(ctx, stamped); err != nil {
		return fmt.Errorf("upsert App-scoped GitHub registration: %w", err)
	}
	return nil
}

func (scoped *AppScopedRegistrationRepository) DeleteActiveRegistration(ctx context.Context, record Registration) error {
	stamped, err := scoped.stamp(record)
	if err != nil {
		return err
	}
	if err = scoped.repository.DeleteActiveRegistration(ctx, stamped); err != nil {
		return fmt.Errorf("delete App-scoped active GitHub registration: %w", err)
	}
	return nil
}

func (scoped *AppScopedRegistrationRepository) DeleteOwnedRegistration(ctx context.Context, record Registration) error {
	stamped, err := scoped.stamp(record)
	if err != nil {
		return err
	}
	if err = scoped.repository.DeleteOwnedRegistration(ctx, stamped); err != nil {
		return fmt.Errorf("delete App-scoped GitHub ownership: %w", err)
	}
	return nil
}

func (scoped *AppScopedRegistrationRepository) DeleteRegistrationState(ctx context.Context, record Registration) error {
	stamped, err := scoped.stamp(record)
	if err != nil {
		return err
	}
	if err = scoped.repository.DeleteRegistrationState(ctx, stamped); err != nil {
		return fmt.Errorf("delete App-scoped GitHub registration state: %w", err)
	}
	return nil
}

func (scoped *AppScopedRegistrationRepository) filter(records []Registration) []Registration {
	filtered := make([]Registration, 0, len(records))
	for _, record := range records {
		if record.GitHubAppID == scoped.appID {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func (scoped *AppScopedRegistrationRepository) stamp(record Registration) (Registration, error) {
	if record.GitHubAppID != 0 && record.GitHubAppID != scoped.appID {
		return Registration{}, fmt.Errorf("GitHub registration belongs to App %d, not App %d", record.GitHubAppID, scoped.appID)
	}
	record.GitHubAppID = scoped.appID
	return record, nil
}
