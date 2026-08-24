package github

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

const githubAppOperationTimeout = time.Minute

// MultiAppCoordinator presents several credential-isolated GitHub managers as
// one source runtime. Manager order is repository precedence order; all calls
// are supervised beneath the caller's single GitHub leadership term.
type MultiAppCoordinator struct {
	registrations    RegistrationRepository
	managers         []*Manager
	byAppID          map[AppID]*Manager
	source           *Source
	routesByApp      map[AppID][]MemberRoute
	operationTimeout time.Duration
}

// DegradedReconciliationError reports App-scoped failures while confirming
// that the accompanying registration snapshot was loaded successfully and is
// safe for the source runtime to apply.
type DegradedReconciliationError struct{ Err error }

func (failure DegradedReconciliationError) Error() string        { return failure.Err.Error() }
func (failure DegradedReconciliationError) Unwrap() error        { return failure.Err }
func (DegradedReconciliationError) RegistrationsAreUsable() bool { return true }

// NewMultiAppCoordinator validates the configured App runtimes without
// starting discovery. Each manager must use an App-scoped registration store.
func NewMultiAppCoordinator(
	registrations RegistrationRepository,
	managers ...*Manager,
) (*MultiAppCoordinator, error) {
	if registrations == nil || len(managers) == 0 {
		return nil, errors.New("GitHub registration repository and at least one App manager are required")
	}
	coordinator := &MultiAppCoordinator{registrations: registrations, managers: managers,
		byAppID: make(map[AppID]*Manager, len(managers)), source: &Source{routes: make(map[routeKey]memberRoute)},
		routesByApp: make(map[AppID][]MemberRoute, len(managers)), operationTimeout: githubAppOperationTimeout}
	sharedRepositorySlots := make(chan struct{}, repositoryConcurrency)
	appRepositoryQuotas := repositoryQuotas(len(managers))
	for index, manager := range managers {
		if manager == nil || manager.githubAppID() <= 0 {
			return nil, errors.New("every GitHub App manager must have a positive scoped App ID")
		}
		appID := manager.githubAppID()
		if _, duplicate := coordinator.byAppID[appID]; duplicate {
			return nil, fmt.Errorf("GitHub App ID %d is duplicated", appID)
		}
		coordinator.byAppID[appID] = manager
		manager.repositorySlots = sharedRepositorySlots
		manager.appRepositorySlots = make(chan struct{}, appRepositoryQuotas[index])
	}
	return coordinator, nil
}

func (coordinator *MultiAppCoordinator) Source() *Source { return coordinator.source }

func (coordinator *MultiAppCoordinator) GitHubAppIDs() []AppID {
	appIDs := make([]AppID, 0, len(coordinator.managers))
	for _, manager := range coordinator.managers {
		appIDs = append(appIDs, manager.githubAppID())
	}
	return appIDs
}

func (coordinator *MultiAppCoordinator) ActiveRegistrations(ctx context.Context) ([]Registration, error) {
	records, err := coordinator.registrations.Registrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("load active GitHub registrations: %w", err)
	}
	active := make([]Registration, 0, len(records))
	var cleanupErr error
	for _, record := range records {
		if _, configured := coordinator.byAppID[record.GitHubAppID]; configured {
			active = append(active, record)
			continue
		}
		if deleteErr := coordinator.registrations.DeleteActiveRegistration(ctx, record); deleteErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove route for absent GitHub App %d: %w",
				record.GitHubAppID, deleteErr))
		}
	}
	return active, cleanupErr
}

func (coordinator *MultiAppCoordinator) Reconcile(ctx context.Context, config ManagerConfig) ([]Registration, error) {
	if err := validateManagerConfig(config); err != nil {
		return nil, err
	}
	outcomes := coordinator.discoverApps(ctx)
	resultErr := coordinator.resolvePrecedence(outcomes)
	resultErr = errors.Join(resultErr, coordinator.reconcileApps(ctx, outcomes, config))
	return coordinator.registrationResult(ctx, resultErr)
}

func (coordinator *MultiAppCoordinator) resolvePrecedence(outcomes []appReconciliationOutcome) error {
	claimed := make(map[string]struct{})
	var resultErr error
	for index := range outcomes {
		outcome := &outcomes[index]
		resultErr = errors.Join(resultErr, outcome.err)
		if outcome.authoritative {
			outcome.repositories = coordinator.filterOverlaps(outcome.manager, outcome.repositories, claimed)
			for _, repository := range outcome.repositories {
				claimed[repository.URL] = struct{}{}
			}
		} else {
			for _, record := range outcome.previous {
				claimed[record.RepositoryURL] = struct{}{}
			}
		}
	}
	return resultErr
}

func (coordinator *MultiAppCoordinator) registrationResult(
	ctx context.Context,
	resultErr error,
) ([]Registration, error) {
	active, activeErr := coordinator.ActiveRegistrations(ctx)
	resultErr = errors.Join(resultErr, activeErr)
	if resultErr == nil || active == nil {
		return active, resultErr
	}
	return active, DegradedReconciliationError{Err: resultErr}
}

type appReconciliationOutcome struct {
	manager       *Manager
	previous      []Registration
	repositories  []transport.Repository
	authoritative bool
	err           error
}

func (coordinator *MultiAppCoordinator) discoverApps(ctx context.Context) []appReconciliationOutcome {
	outcomes := make([]appReconciliationOutcome, len(coordinator.managers))
	var group sync.WaitGroup
	for index, manager := range coordinator.managers {
		group.Add(1)
		go func() {
			defer group.Done()
			operationContext, cancel := context.WithTimeout(ctx, coordinator.operationTimeout)
			defer cancel()
			outcome := appReconciliationOutcome{manager: manager}
			outcome.previous, outcome.err = manager.ActiveRegistrations(operationContext)
			if outcome.err == nil {
				result, discoverErr := manager.discoverRepositories(operationContext)
				outcome.repositories = result.Repositories
				outcome.err = errors.Join(discoverErr, discoveryFailures(result.InstallationFailures))
				outcome.authoritative = outcome.err == nil
			}
			if outcome.err != nil {
				outcome.err = fmt.Errorf("reconcile GitHub App %d discovery: %w", manager.githubAppID(), outcome.err)
			}
			outcomes[index] = outcome
		}()
	}
	group.Wait()
	return outcomes
}

func (coordinator *MultiAppCoordinator) filterOverlaps(
	manager *Manager,
	repositories []transport.Repository,
	claimed map[string]struct{},
) []transport.Repository {
	filtered := make([]transport.Repository, 0, len(repositories))
	for _, repository := range repositories {
		if _, overlap := claimed[repository.URL]; overlap {
			manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
				Stage: "repository_overlap", Outcome: "ignored", Repository: repository.URL})
			continue
		}
		filtered = append(filtered, repository)
	}
	return filtered
}

func (coordinator *MultiAppCoordinator) reconcileApps(
	ctx context.Context,
	outcomes []appReconciliationOutcome,
	config ManagerConfig,
) error {
	errorsByApp := make([]error, len(outcomes))
	var group sync.WaitGroup
	for index := range outcomes {
		outcome := outcomes[index]
		if !outcome.authoritative {
			continue
		}
		group.Add(1)
		go func() {
			defer group.Done()
			if err := coordinator.reconcileApp(ctx, outcome, config); err != nil {
				errorsByApp[index] = fmt.Errorf("reconcile GitHub App %d repositories: %w", outcome.manager.githubAppID(), err)
			}
		}()
	}
	group.Wait()
	return errors.Join(errorsByApp...)
}

func (coordinator *MultiAppCoordinator) reconcileApp(
	ctx context.Context,
	outcome appReconciliationOutcome,
	config ManagerConfig,
) error {
	started := outcome.manager.observer.currentTime()
	outcome.manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "reconciliation", Outcome: "started"})
	_, err := outcome.manager.reconcileDiscovered(
		ctx,
		outcome.previous,
		outcome.repositories,
		config,
		coordinator.operationTimeout,
	)
	activityOutcome := "completed"
	if err != nil {
		activityOutcome = "failed"
	}
	outcome.manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_reconciliation",
		Stage: "reconciliation", Outcome: activityOutcome, Err: err,
		Duration: outcome.manager.observer.currentTime().Sub(started)})
	return err
}

func (coordinator *MultiAppCoordinator) RefreshMembers(ctx context.Context, config ManagerConfig) error {
	var resultErr error
	for _, manager := range coordinator.managers {
		records, err := manager.ActiveRegistrations(ctx)
		if err != nil {
			resultErr = errors.Join(resultErr, err)
			continue
		}
		routes, err := manager.buildMemberRoutes(ctx, records, config)
		if err != nil {
			resultErr = errors.Join(resultErr, err)
			continue
		}
		coordinator.routesByApp[manager.githubAppID()] = routes
	}
	routes := make([]MemberRoute, 0)
	for _, manager := range coordinator.managers {
		routes = append(routes, coordinator.routesByApp[manager.githubAppID()]...)
	}
	if err := coordinator.source.ReplaceRoutes(routes); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("replace GitHub member routes: %w", err))
	}
	return resultErr
}

func (coordinator *MultiAppCoordinator) OpenRegistrationSession(
	ctx context.Context,
	record Registration,
	configured RunnerPoolConfig,
	owner string,
	handler EventHandler,
) (*RegistrationSession, error) {
	manager := coordinator.byAppID[record.GitHubAppID]
	if manager == nil {
		return nil, fmt.Errorf("GitHub App %d is not configured for repository %q", record.GitHubAppID, record.RepositoryURL)
	}
	return manager.OpenRegistrationSession(ctx, record, configured, owner, handler)
}

func (coordinator *MultiAppCoordinator) UsesSharedRunnerPoolDeclarations() bool {
	return coordinator.managers[0].UsesSharedRunnerPoolDeclarations()
}

func (coordinator *MultiAppCoordinator) PublishRunnerPoolDeclarations(
	ctx context.Context,
	memberID core.MemberID,
	config ManagerConfig,
) error {
	return coordinator.managers[0].PublishRunnerPoolDeclarations(ctx, memberID, config)
}

func (coordinator *MultiAppCoordinator) ClusterRunnerPools(
	ctx context.Context,
	liveMemberCutoff time.Time,
) (RunnerPoolDeclarationSnapshot, error) {
	return coordinator.managers[0].ClusterRunnerPools(ctx, liveMemberCutoff)
}
