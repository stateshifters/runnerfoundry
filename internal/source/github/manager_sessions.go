package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

// RegistrationSession owns one admitted registration's GitHub message session
// and adapter. Registration returns the immutable route admitted at open time.
type RegistrationSession struct {
	registration Registration
	session      ManagedSession
	adapter      *Adapter
}

// ErrRegistrationNotAdmitted marks a definitive rejection that callers should
// detect with errors.Is. OpenRegistrationSession returns no session with this
// error after any required exact guarded cleanup succeeds; callers should stop
// retrying that registration without reporting an operational failure.
var ErrRegistrationNotAdmitted = errors.New("GitHub registration was not admitted")

func (session *RegistrationSession) Registration() Registration { return session.registration }

func (session *RegistrationSession) Adapter() *Adapter { return session.adapter }

func (session *RegistrationSession) Close(ctx context.Context) error {
	if err := session.session.Close(ctx); err != nil {
		if errors.Is(err, upstream.BadRequestError) || errors.Is(err, upstream.NotFoundError) {
			return nil
		}
		return fmt.Errorf("close GitHub registration session: %w", err)
	}
	return nil
}

// OpenRegistrationSession restores one durable registration directly from its
// committed identity. A terminal repository or scale-set absence deletes only
// the exact guarded active and ownership state. Inconclusive failures retain
// registration state for retry. On success, the caller owns the returned
// session and must close it.
func (manager *Manager) OpenRegistrationSession(
	ctx context.Context,
	record Registration,
	runnerPool RunnerPoolConfig,
	owner string,
	handler EventHandler,
) (*RegistrationSession, error) {
	if err := validateRegistrationSessionRequest(record, owner, handler); err != nil {
		return nil, err
	}
	client, err := manager.registrationSessionClient(ctx, record)
	if err != nil {
		return nil, err
	}
	if runnerPool.RunnerGroup != "" {
		admitted, matchErr := manager.registrationMatchesRunnerGroup(ctx, client, record, runnerPool.RunnerGroup)
		if matchErr != nil || !admitted {
			return nil, matchErr
		}
	}
	return manager.openAdmittedRegistration(ctx, client, record, owner, handler)
}

func (manager *Manager) registrationSessionClient(ctx context.Context, record Registration) (ManagerClient, error) {
	client, err := manager.repositoryClient(ctx, repositoryFromRegistration(record))
	if err == nil {
		return client, nil
	}
	if errors.Is(err, upstream.NotFoundError) {
		if deleteErr := manager.deleteMissingRegistration(ctx, record); deleteErr != nil {
			return ManagerClient{}, deleteErr
		}
		return ManagerClient{}, fmt.Errorf("GitHub repository %q was not found: %w",
			record.RepositoryURL, ErrRegistrationNotAdmitted)
	}
	return ManagerClient{}, fmt.Errorf("create GitHub session client for repository %q: %w", record.RepositoryURL, err)
}

func (manager *Manager) registrationMatchesRunnerGroup(
	ctx context.Context,
	client ManagerClient,
	record Registration,
	runnerGroupName string,
) (bool, error) {
	if client.ResolveRunnerGroup == nil {
		return false, errors.New("GitHub runner-group resolution is required")
	}
	runnerGroupID, err := client.ResolveRunnerGroup(ctx, runnerGroupName)
	if err != nil {
		return false, fmt.Errorf("resolve GitHub runner group %q: %w", runnerGroupName, err)
	}
	if runnerGroupID == record.RunnerGroupID {
		return true, nil
	}
	if err = manager.registrations.DeleteActiveRegistration(ctx, record); err != nil {
		return false, fmt.Errorf("forget stale GitHub registration for Runner Pool %q: %w", record.RunnerPool, err)
	}
	return false, fmt.Errorf("runner group %q no longer matches registration: %w",
		runnerGroupName, ErrRegistrationNotAdmitted)
}

func (manager *Manager) openAdmittedRegistration(
	ctx context.Context,
	client ManagerClient,
	record Registration,
	owner string,
	handler EventHandler,
) (*RegistrationSession, error) {
	started := manager.observer.currentTime()
	manager.reportSessionActivity(record, 1, 1, "started", started, nil)
	opened, err := manager.openLeaderRecordWithClient(ctx, client, record, owner, handler)
	if err != nil {
		resultErr := manager.registrationOpenFailure(ctx, record, err)
		manager.reportSessionActivity(record, 1, 1, "failed", started, resultErr)
		return nil, resultErr
	}
	manager.reportSessionActivity(record, 1, 1, "completed", started, nil)
	return &RegistrationSession{registration: record, session: opened.session, adapter: opened.adapter}, nil
}

func (manager *Manager) registrationOpenFailure(ctx context.Context, record Registration, err error) error {
	if !errors.Is(err, upstream.NotFoundError) {
		return fmt.Errorf("open GitHub session for repository %q Runner Pool %q: %w",
			record.RepositoryURL, record.RunnerPool, err)
	}
	if deleteErr := manager.deleteMissingRegistration(ctx, record); deleteErr != nil {
		return deleteErr
	}
	return fmt.Errorf("GitHub runner scale set %d was not found: %w",
		record.ScaleSetID, ErrRegistrationNotAdmitted)
}

func validateRegistrationSessionRequest(
	record Registration,
	owner string,
	handler EventHandler,
) error {
	if record.RepositoryURL == "" || record.InstallationID == 0 || record.RunnerGroupID == 0 || record.ScaleSetID == 0 {
		return errors.New("complete GitHub registration identity is required")
	}
	if record.RunnerPool == "" || owner == "" || handler == nil {
		return errors.New("GitHub Runner Pool, leader owner, and event handler are required")
	}
	return nil
}

func (manager *Manager) deleteMissingRegistration(ctx context.Context, record Registration) error {
	if err := manager.registrations.DeleteRegistrationState(ctx, record); err != nil {
		return fmt.Errorf("forget missing GitHub registration for Runner Pool %q: %w", record.RunnerPool, err)
	}
	return nil
}

func (manager *Manager) reportSessionActivity(record Registration, current, total int, outcome string,
	started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_sessions",
		Stage: "open_session", Outcome: outcome, Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		InstallationID: int64(record.InstallationID), RunnerGroupID: int64(record.RunnerGroupID),
		ScaleSetID: int64(record.ScaleSetID),
		Current:    current, Total: total, Duration: duration, Err: err})
}

func (manager *Manager) openLeaderRecordWithClient(
	ctx context.Context,
	client ManagerClient,
	record Registration,
	owner string,
	handler EventHandler,
) (openedLeader, error) {
	session, err := client.OpenSession(ctx, transportRegistration(record), owner)
	if err != nil {
		return openedLeader{}, err
	}
	if session == nil {
		return openedLeader{}, errors.New("GitHub client returned nil message session")
	}
	sessionID, initialStatistics, initialStatisticsPending := managerSessionInitialObservation(session)
	return openedLeader{session: session, adapter: &Adapter{Client: session, Handler: handler, Observer: manager.observer,
		Repository: record.RepositoryURL, RunnerPool: record.RunnerPool, InstallationID: int64(record.InstallationID),
		RunnerGroupID: int64(record.RunnerGroupID), ScaleSetID: int64(record.ScaleSetID),
		InitialSessionID: sessionID, InitialStatistics: initialStatistics,
		InitialStatisticsPending: initialStatisticsPending}}, nil
}

func managerSessionInitialObservation(session ManagedSession) (uuid.UUID, *upstream.RunnerScaleSetStatistic, bool) {
	initial, available := session.(interface {
		SessionID() uuid.UUID
		InitialStatistics() *upstream.RunnerScaleSetStatistic
	})
	if !available {
		return uuid.Nil, nil, false
	}
	statistics := initial.InitialStatistics()
	if statistics == nil {
		return initial.SessionID(), nil, true
	}
	copied := *statistics
	return initial.SessionID(), &copied, true
}

func repositoryFromRegistration(record Registration) transport.Repository {
	return transport.Repository{InstallationID: record.InstallationID, URL: record.RepositoryURL}
}

func transportRegistration(record Registration) transport.Registration {
	return transport.Registration{ID: record.ScaleSetID, Name: string(record.RunnerPool), RunnerGroupID: record.RunnerGroupID}
}
