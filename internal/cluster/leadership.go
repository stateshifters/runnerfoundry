package cluster

import (
	"context"
	"fmt"
)

// LeaderLease owns the dedicated PostgreSQL connection holding the cluster's
// session advisory lock. The caller must close the lease to release leadership.
type LeaderLease struct {
	connection leaderConnection
}

// Lease describes ownership and liveness of a cluster leadership lease.
type Lease interface {
	// Alive reports whether the lease's connection is open and responds to a
	// PostgreSQL ping using ctx.
	Alive(ctx context.Context) bool
	// Close releases leadership by closing the lease's dedicated connection.
	Close(ctx context.Context) error
}

// TryLeader attempts to acquire the cluster leadership lease without waiting.
// It returns ErrNotLeader when another session holds the lease; callers should
// detect that result with errors.Is and remain on standby or call WaitForLeader.
// A returned lease owns its connection until Close is called.
func (store *Store) TryLeader(ctx context.Context) (*LeaderLease, error) {
	connection, err := store.directConnector(ctx)
	if err != nil {
		return nil, err
	}
	var acquired bool
	if err = connection.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&acquired); err != nil {
		closeConnection(ctx, connection)
		return nil, fmt.Errorf("try clustered leader lease: %w", err)
	}
	if !acquired {
		closeConnection(ctx, connection)
		return nil, ErrNotLeader
	}
	return &LeaderLease{connection: connection}, nil
}

// WaitForLeader blocks on PostgreSQL's session advisory lock. Canceling ctx
// closes the waiting connection; a returned lease owns its connection until Close.
func (store *Store) WaitForLeader(ctx context.Context) (*LeaderLease, error) {
	connection, err := store.directConnector(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, leaderAdvisoryLockKey); err != nil {
		closeConnection(ctx, connection)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("wait for clustered leader lease: %w", ctx.Err())
		}
		return nil, fmt.Errorf("wait for clustered leader lease: %w", err)
	}
	return &LeaderLease{connection: connection}, nil
}

// Close releases leadership by closing the lease's dedicated connection. A nil
// or empty lease is already closed and returns nil.
func (lease *LeaderLease) Close(ctx context.Context) error {
	if lease == nil || lease.connection == nil {
		return nil
	}
	if err := lease.connection.Close(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("close PostgreSQL leader lease: %w", err)
	}
	return nil
}

func closeConnection(ctx context.Context, connection interface {
	Close(ctx context.Context) error
}) {
	_ = connection.Close(context.WithoutCancel(ctx)) //nolint:errcheck,gosec // Deferred connection cleanup is best effort.
}

func rollbackTransaction(ctx context.Context, transaction databaseTransaction) {
	_ = transaction.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck,gosec // Rollback is best effort after commit or failure.
}

// Alive reports whether the lease's connection is open and responds to a
// PostgreSQL ping using ctx. A nil or empty lease is not alive.
func (lease *LeaderLease) Alive(ctx context.Context) bool {
	if lease == nil || lease.connection == nil || lease.connection.IsClosed() {
		return false
	}
	return lease.connection.Ping(ctx) == nil
}
