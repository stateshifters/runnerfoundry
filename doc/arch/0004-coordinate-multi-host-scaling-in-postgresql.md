# 4. Coordinate multi-host scaling in PostgreSQL

Date: 2026-07-16

## Status

Accepted

Supersedes [3. Persist shared queue state in SQLite](0003-persist-shared-queue-state-in-sqlite.md)

## Context

ADR 3 made queued demand durable but intentionally retained a single-writer SQLite boundary. One daemon therefore owned Github discovery, message polling, provider execution, and the full capacity limit. It could not combine capacity from multiple provider hosts or fail leadership over without moving the SQLite database and stopping the original writer.

The scaler now needs several hosts to serve shared or disjoint scale sets. Capacity is local to a provider host: two hosts declaring the same scale-set name contribute separate limits, while one host's limits for different scale sets remain independent. The coordinator must preserve a global FIFO, prevent assignments beyond either local limit, manage scale sets that the leader cannot execute locally, and retain demand through node and leader failure.

PostgreSQL credentials may be conventional long-lived values or opaque credentials produced by Vault Agent and similar systems. The scaler cannot know whether a value is time-bound or single-use and must not make lifecycle assumptions about it.

## Decision

Standalone operation will retain the exclusively locked SQLite store and its existing behavior. Clustered operation will use PostgreSQL for node contributions, health and heartbeat state, the global demand queue, assignments, and advisory-lock leader election.

Each host will publish the scale sets in its own configuration. `scale_sets[].max_runners` remains a per-host, per-scale-set limit; there is no configured cluster-wide limit. The leader will sum only live, healthy, configuration-compatible contributions and will assign the oldest demand to a contributor with unused local capacity. Hosts sharing a scale-set name must agree on Github-facing settings such as the runner group. Provider configuration remains local and is never copied into the cluster definition.

Each host will use its operating-system hostname as its cluster identity. Cluster identity will not be user-configurable.

The leader will manage the union of declared scale sets and discovered repositories. Losing a host will remove its effective capacity without deleting its declarations. A host publication that explicitly omits a previous declaration will withdraw it, and the leader will remove a Github scale set only after no host declares that name. Losing leadership will close management sessions without deleting scale sets; the next leader will rebuild them from PostgreSQL state.

PostgreSQL may use its standard environment configuration or a DSN. It may instead use separate `host`, `port`, `name`, `user`, and `password` settings. Every DSN or structured value may be literal, `env://NAME`, or `file:///path`. A DSN cannot be combined with structured settings; omitted structured values retain pgx/PostgreSQL environment defaults. The legacy `dsn_env` and `dsn_file` keys are rejected.

Every clustered connection will assume one required stable PostgreSQL role supplied by `state.postgres.role`. Login identities may rotate, but each must be a member of the stable role. The role will own scaler objects and migration metadata and will apply to ordinary state calls, migrations, migration locks, leader leases, and waiting contenders.

Deployments may provide a separate `state.postgres.direct` endpoint for migrations and session advisory locks while ordinary traffic uses the primary endpoint. Direct structured fields inherit primary structured fields. A distinct endpoint for an opaque primary DSN requires a complete direct DSN. Without a direct endpoint, the primary connection must preserve session affinity.

References will be resolved immediately before opening each connection so new connections receive current values. Files are read and trimmed on every attempt, with native Unix and Windows paths supported. Missing, unreadable, malformed, or empty references fail without exposing resolved values or paths, and optional fallback query syntax is not supported. Structured values are encoded before pgx parses them rather than interpolated into a connection string. Credentials are opaque and will not be persisted, logged, inspected for expiry, or classified by usage semantics.

The daemon will embed reversible tern migrations and automatically migrate only upward during startup. A scaler-specific advisory lock will serialize version inspection, migration, and compatibility validation across simultaneous nodes. All scaler schema setup and teardown SQL will live in migration files rather than Go startup helpers. Each migration will include a complete transactional Down path and preserve one-release rolling compatibility through expand, backfill, and later contract changes.

Rollback will be an explicit command run with the currently deployed binary before installing an older binary. It will acquire the migration and leader locks and reject live node heartbeats. Daemon startup will never automatically execute a Down migration.

After migration and contribution publication, each node will try the leader lock immediately. Non-leaders will wait on the session advisory lock using a dedicated direct connection. PostgreSQL lock release will therefore hand leadership to a waiter without an availability-polling interval.

## Consequences

Spock contributing 10 runners and Serenity contributing 2 runners to the same compatible scale set yields 12 effective runners while preserving limits of 10 and 2 at assignment time. Hosts can also contribute unrelated scale sets, and the leader need not have a provider for every scale set it manages. PostgreSQL transactions serialize FIFO assignment and advisory locking limits active management to one leader.

Clustered deployments now depend on a highly available PostgreSQL service and database connectivity. Temporary node loss reduces capacity and may cause an acquired request whose runner never started to be retried after the node expires; stable request-derived runner names continue to provide provider-side recovery identity. Github acquisition, provider startup, and PostgreSQL still cannot form one atomic transaction, so recovery favors retaining or retrying demand over claiming exactly-once execution.

File-backed credential rotation applies to new connections. The scaler does not force existing connections closed when a file changes, and the leader lease connection remains valid until PostgreSQL or the scaler closes it.

Fresh installations and normal upgrades require no operator migration step. Migration failure prevents readiness and capacity publication. Downgrade remains deliberate because only the newer binary knows how to reverse its migrations, and operators must stop the cluster before rolling the schema back.
