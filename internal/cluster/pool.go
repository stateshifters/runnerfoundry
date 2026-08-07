package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type roleConnection interface {
	Exec(ctx context.Context, statement string, arguments ...any) (pgconn.CommandTag, error)
}

// OpenPool opens the primary PostgreSQL endpoint for concurrent runtime state
// access. Every connection assumes the configured stable role before use.
func (store *Store) OpenPool(ctx context.Context) (*pgxpool.Pool, error) {
	role, err := resolvedConnectionRole(store.settings, store.connectionResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve PostgreSQL role: %w", err)
	}
	connectionConfig, err := primaryConnectionConfigWithResolver(store.settings, store.connectionResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve primary PostgreSQL connection: %w", err)
	}
	poolConfig, err := store.parsePoolConfig(connectionConfig.ConnString())
	if err != nil {
		return nil, errors.New("prepare PostgreSQL pool")
	}
	poolConfig.AfterConnect = func(connectCtx context.Context, connection *pgx.Conn) error {
		return store.assumePoolRole(connectCtx, connection, role)
	}
	pool, err := store.connectPool(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", newPostgreSQLConnectionError(err))
	}
	return pool, nil
}

func assumePostgreSQLRole(ctx context.Context, connection roleConnection, role string) error {
	if _, err := connection.Exec(ctx, "SET ROLE "+quotePostgreSQLIdentifier(role)); err != nil {
		return errors.New("assume PostgreSQL role")
	}
	return nil
}
