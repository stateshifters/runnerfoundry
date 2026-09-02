package cluster

import (
	"context"
	"errors"
	"fmt"
)

type LeaderElector struct {
	sessions sessionOpener
}

func NewLeaderElector(connections *Connections) *LeaderElector {
	return &LeaderElector{sessions: connections}
}

func leaderElectorWithSessions(sessions sessionOpener) *LeaderElector {
	return &LeaderElector{sessions: sessions}
}

type LeaderLease struct {
	session *databaseSession
}

func (elector *LeaderElector) TryAcquire(ctx context.Context) (*LeaderLease, error) {
	session, err := elector.sessions.openSession(ctx)
	if err != nil {
		return nil, err
	}
	var acquired bool
	if err = session.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&acquired); err != nil {
		return nil, errors.Join(fmt.Errorf("try clustered leader lease: %w", err), closeSession(ctx, session))
	}
	if !acquired {
		return nil, errors.Join(ErrNotLeader, closeSession(ctx, session))
	}
	return &LeaderLease{session: session}, nil
}

func (lease *LeaderLease) Close(ctx context.Context) error {
	if lease == nil || lease.session == nil {
		return nil
	}
	if err := lease.session.Close(ctx); err != nil {
		return fmt.Errorf("close PostgreSQL leader lease: %w", err)
	}
	return nil
}

func (lease *LeaderLease) Alive(ctx context.Context) bool {
	if lease == nil || lease.session == nil || lease.session.IsClosed() {
		return false
	}
	return lease.session.Ping(ctx) == nil
}

func closeSession(ctx context.Context, session interface {
	Close(ctx context.Context) error
}) error {
	if err := session.Close(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("close PostgreSQL connection: %w", err)
	}
	return nil
}
