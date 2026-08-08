package github

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

// RepositorySessionGeneration owns the complete set of message sessions for
// one repository snapshot. Its views are defensive copies. Close attempts all
// sessions in registration order and retains failed sessions for a later retry.
type RepositorySessionGeneration struct {
	repository    string
	registrations []Registration
	adapters      []*Adapter
	sessions      []ManagedSession
	retire        func(context.Context, ManagedSession, int, int) error
	closeMu       sync.Mutex
}

func (generation *RepositorySessionGeneration) Repository() string {
	return generation.repository
}

func (generation *RepositorySessionGeneration) Registrations() []Registration {
	return append([]Registration(nil), generation.registrations...)
}

func (generation *RepositorySessionGeneration) Adapters() []*Adapter {
	return append([]*Adapter(nil), generation.adapters...)
}

// Close releases every session in the generation. Sessions that fail to close
// remain owned by the generation and are retried on the next call.
func (generation *RepositorySessionGeneration) Close(ctx context.Context) error {
	generation.closeMu.Lock()
	defer generation.closeMu.Unlock()
	remaining := make([]ManagedSession, 0)
	var joined error
	for index, session := range generation.sessions {
		if err := generation.retire(ctx, session, index+1, len(generation.sessions)); err != nil {
			remaining = append(remaining, session)
			joined = errors.Join(joined, fmt.Errorf("close GitHub message session: %w", err))
		}
	}
	generation.sessions = remaining
	return joined
}

// OpenRepositorySessions opens an all-or-nothing message-session generation
// for the supplied committed registrations. Records must be non-empty and all
// belong to the same repository. Runner Pool sessions open sequentially in
// record order; any partial generation is closed before an error is returned.
func (manager *Manager) OpenRepositorySessions(
	ctx context.Context,
	records []Registration,
	owner string,
	handler EventHandler,
) (*RepositorySessionGeneration, error) {
	repository, err := validateRepositorySessionRecords(records, owner, handler)
	if err != nil {
		return nil, err
	}
	client, err := manager.factory(ctx, repositoryFromRegistration(records[0]))
	if err != nil {
		return nil, fmt.Errorf("create GitHub session client for repository %q: %w", repository, err)
	}
	generation := &RepositorySessionGeneration{
		repository: repository, registrations: append([]Registration(nil), records...),
		adapters: make([]*Adapter, 0, len(records)), sessions: make([]ManagedSession, 0, len(records)),
		retire: manager.retireRepositorySession,
	}
	for index, record := range records {
		if openErr := manager.openRepositorySession(ctx, generation, client, record, owner, handler, index, len(records)); openErr != nil {
			return failedRepositoryGeneration(ctx, generation, record, openErr)
		}
	}
	return generation, nil
}

func (manager *Manager) openRepositorySession(
	ctx context.Context,
	generation *RepositorySessionGeneration,
	client ManagerClient,
	record Registration,
	owner string,
	handler EventHandler,
	index int,
	total int,
) error {
	started := manager.observer.currentTime()
	manager.reportSessionActivity(record, index+1, total, "started", started, nil)
	opened, err := manager.openLeaderRecordWithClient(ctx, client, record, owner, handler)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	} else {
		generation.sessions = append(generation.sessions, opened.session)
		generation.adapters = append(generation.adapters, opened.adapter)
	}
	manager.reportSessionActivity(record, index+1, total, outcome, started, err)
	return err
}

func failedRepositoryGeneration(
	ctx context.Context,
	generation *RepositorySessionGeneration,
	record Registration,
	openErr error,
) (*RepositorySessionGeneration, error) {
	cleanupErr := generation.Close(ctx)
	resultErr := errors.Join(
		fmt.Errorf("open GitHub session for repository %q Runner Pool %q: %w",
			generation.Repository(), record.RunnerPool, openErr),
		cleanupErr,
	)
	if cleanupErr != nil {
		return generation, resultErr
	}
	return nil, resultErr
}

func validateRepositorySessionRecords(
	records []Registration,
	owner string,
	handler EventHandler,
) (string, error) {
	if len(records) == 0 || owner == "" || handler == nil {
		return "", errors.New("GitHub repository registrations, leader owner, and event handler are required")
	}
	repository := records[0].RepositoryURL
	if repository == "" {
		return "", errors.New("GitHub repository registration URL is required")
	}
	for _, record := range records[1:] {
		if record.RepositoryURL != repository {
			return "", errors.New("GitHub repository session registrations must belong to one repository")
		}
	}
	return repository, nil
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

func (manager *Manager) retireRepositorySession(
	ctx context.Context,
	session ManagedSession,
	current int,
	total int,
) error {
	started := manager.observer.currentTime()
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_sessions",
		Stage: "retire_session", Outcome: "started", Current: current, Total: total})
	err := session.Close(ctx)
	outcome := "completed"
	if err != nil {
		outcome = "failed"
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_sessions",
		Stage: "retire_session", Outcome: outcome, Current: current, Total: total,
		Duration: manager.observer.currentTime().Sub(started), Err: err})
	if err != nil {
		return fmt.Errorf("retire GitHub session: %w", err)
	}
	return nil
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
