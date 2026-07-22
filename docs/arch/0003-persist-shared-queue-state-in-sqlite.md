# 3. Persist shared queue state in SQLite

Date: 2026-07-12

## Status

Superseded by [4. Coordinate multi-host scaling in PostgreSQL](0004-coordinate-multi-host-scaling-in-postgresql.md)

Supersedes [2. Use GitHub App installations and shared runner capacity](0002-use-github-app-installations-and-shared-runner-capacity.md)

## Context

The shared capacity decision acknowledged Github messages after copying waiting demand into memory. Demand that could not immediately receive capacity was therefore acknowledged but existed only inside the process. A reload, crash, or host replacement could lose that queue and its cross-repository ordering.

The daemon remains a single-writer service and must support Windows, macOS, and Linux. Operators need host migration without the operational cost of a separate database service. Provider operations and Github acknowledgement cannot participate in one database transaction, so recovery must be idempotent rather than exactly once.

## Decision

We will retain Github App discovery and one shared capacity budget per configured scale set. We will persist identifiable waiting demand from job-available messages in a single-writer SQLite database before acknowledging Github messages. Assigned capacity without a positive request identity will be reconciled from Github message statistics after the durable FIFO queue. The configured database will be required, exclusively locked by the daemon, and placed on operator-selected durable storage.

Queue mutations from one Github message will commit atomically. A stable runner name derived from the scale set and Github request ID will let restart recovery recognize provider-visible work. Completed demand and demand outside the current repository and scale-set authority will be deleted rather than retained as history.

## Consequences

FIFO admission and acknowledged waiting demand with Github request identities survive daemon reloads, crashes, reboots, and movement of the durable volume to another host. Redelivered job-available messages are harmless because the request identity is unique within its repository and scale set. Assigned capacity without an identity cannot participate in exact cross-repository FIFO ordering and is admitted only after identifiable demand.

The daemon owns schema migration, locking, file permissions, and database lifecycle behavior. Only one daemon may use a database at a time. Operators must stop the daemon before copying the database or changing its path. Debian removal preserves package-managed state, while Debian purge deletes it.

SQLite cannot make Github, provider startup, and local persistence one atomic operation. Recovery checks stable provider identities and retries missing work, prioritizing no silent loss over an impossible exactly-once guarantee.
