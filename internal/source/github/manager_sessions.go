package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

// OpenLeader opens one actions/scaleset message session per persisted registration.
func (manager *Manager) OpenLeader(ctx context.Context, owner string, handler EventHandler) ([]*Adapter, error) {
	if owner == "" || handler == nil {
		return nil, errors.New("GitHub leader owner and event handler are required")
	}
	cleanupErr := manager.retryRetiring(ctx)
	snapshot, err := manager.registrations.RegistrationSnapshot(ctx)
	if err != nil {
		return nil, errors.Join(cleanupErr, fmt.Errorf("list GitHub registrations: %w", err))
	}
	sessions, adapters, err := manager.openLeaderRecords(ctx, snapshot.Active, owner, handler)
	if err != nil {
		return nil, errors.Join(cleanupErr, err)
	}
	oldSessions := manager.leaders
	manager.leaders = sessions
	return adapters, errors.Join(cleanupErr, manager.retireSessions(ctx, oldSessions))
}

func (manager *Manager) openLeaderRecords(ctx context.Context, records []Registration, owner string,
	handler EventHandler,
) ([]ManagedSession, []*Adapter, error) {
	sessions := make([]ManagedSession, 0, len(records))
	adapters := make([]*Adapter, 0, len(records))
	total := len(records)
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
		Stage: "open_sessions", Outcome: "started", Total: total})
	for index, record := range records {
		current := index + 1
		if shouldReportManagerProgress(current, total) {
			manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
				Stage: "open_sessions", Outcome: "progress", Current: current, Total: total})
		}
		opened, err := manager.openLeaderRecordWithActivity(ctx, record, owner, handler, current, total)
		if err != nil {
			manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
				Stage: "open_sessions", Outcome: "failed", Current: current, Total: total, Err: err})
			return nil, nil, manager.closeOpened(ctx, sessions, err)
		}
		sessions = append(sessions, opened.session)
		adapters = append(adapters, opened.adapter)
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
		Stage: "open_sessions", Outcome: "completed", Count: len(sessions), Total: total})
	return sessions, adapters, nil
}

func (manager *Manager) openLeaderRecordWithActivity(ctx context.Context, record Registration, owner string,
	handler EventHandler, current, total int,
) (openedLeader, error) {
	started := manager.observer.currentTime()
	manager.reportSessionActivity(record, current, total, "started", started, nil)
	opened, err := manager.openLeaderRecord(ctx, record, owner, handler)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.reportSessionActivity(record, current, total, outcome, started, err)
	return opened, err
}

func (manager *Manager) reportSessionActivity(record Registration, current, total int, outcome string,
	started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_sessions",
		Stage: "open_session", Outcome: outcome, Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		Current: current, Total: total, Duration: duration, Err: err})
}

func (manager *Manager) openLeaderRecord(ctx context.Context, record Registration, owner string,
	handler EventHandler,
) (openedLeader, error) {
	client, err := manager.factory(ctx, repositoryFromRegistration(record))
	if err != nil {
		return openedLeader{}, err
	}
	session, err := client.OpenSession(ctx, transportRegistration(record), owner)
	if err != nil {
		return openedLeader{}, err
	}
	if session == nil {
		return openedLeader{}, errors.New("GitHub client returned nil message session")
	}
	return openedLeader{session: session, adapter: &Adapter{Client: session, Handler: handler, Observer: manager.observer,
		Repository: record.RepositoryURL, RunnerPool: record.RunnerPool}}, nil
}

func repositoryFromRegistration(record Registration) transport.Repository {
	return transport.Repository{InstallationID: record.InstallationID, URL: record.RepositoryURL}
}

func transportRegistration(record Registration) transport.Registration {
	return transport.Registration{ID: record.ScaleSetID, Name: string(record.RunnerPool), RunnerGroupID: record.RunnerGroupID}
}

func (manager *Manager) closeOpened(ctx context.Context, sessions []ManagedSession, cause error) error {
	return errors.Join(cause, manager.retireSessions(ctx, sessions))
}

// CloseLeader closes every actions/scaleset message session.
func (manager *Manager) CloseLeader(ctx context.Context) error {
	sessions := append(manager.retiring, manager.leaders...)
	manager.leaders = nil
	manager.retiring = nil
	return manager.retireSessions(ctx, sessions)
}

func (manager *Manager) retireSessions(ctx context.Context, sessions []ManagedSession) error {
	if len(sessions) == 0 {
		return nil
	}
	started := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
		Stage: "retire_sessions", Outcome: "started", Total: len(sessions)})
	var joined error
	for index, session := range sessions {
		current := index + 1
		if err := manager.retireSession(ctx, session, current, len(sessions)); err != nil {
			manager.retiring = append(manager.retiring, session)
			joined = errors.Join(joined, fmt.Errorf("close GitHub message session: %w", err))
		}
		if shouldReportManagerProgress(current, len(sessions)) {
			manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
				Stage: "retire_sessions", Outcome: "progress", Current: current, Total: len(sessions)})
		}
	}
	outcome := "completed"
	if joined != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_sessions",
		Stage: "retire_sessions", Outcome: outcome, Count: len(sessions), Total: len(sessions),
		Duration: manager.observer.currentTime().Sub(started), Err: joined})
	return joined
}

func (manager *Manager) retireSession(ctx context.Context, session ManagedSession, current, total int) error {
	started := manager.observer.currentTime()
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_sessions",
		Stage: "retire_session", Outcome: "started", Current: current, Total: total})
	err := session.Close(ctx)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_sessions",
		Stage: "retire_session", Outcome: outcome, Current: current, Total: total,
		Duration: manager.observer.currentTime().Sub(started), Err: err})
	if err != nil {
		return fmt.Errorf("retire GitHub session: %w", err)
	}
	return nil
}

func (manager *Manager) retryRetiring(ctx context.Context) error {
	sessions := manager.retiring
	manager.retiring = nil
	return manager.retireSessions(ctx, sessions)
}
