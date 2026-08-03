# 10. Persist unified capacity obligations for FIFO scheduling

Date: 2026-08-03

## Status

Accepted

Supersedes [9. Schedule assigned recovery through a bounded leader executor](0009-schedule-assigned-recovery-through-a-bounded-leader-executor.md)

Amends [3. Persist shared queue state in SQLite](0003-persist-shared-queue-state-in-sqlite.md)

Amends [4. Coordinate multi-host scaling in PostgreSQL](0004-coordinate-multi-host-scaling-in-postgresql.md)

Amends [7. Persist repository control independently from message sessions](0007-persist-repository-control-independently-from-message-sessions.md)

## Context

ADR 3 made positive-request demand durable in standalone SQLite, and ADR 4 established a separate PostgreSQL queue and assignment model for clustered scheduling. ADRs 8 and 9 added recovery for GitHub's aggregate assigned-job count. That recovery remained a second class of work: it was reconstructed from observations, scheduled outside the canonical FIFO queue, and retried through leader-owned in-memory executor state with a fixed worker count.

GitHub provides two different forms of capacity demand. `JobAvailable` carries a positive request identity and queue time. `TotalAssignedJobs` is an aggregate observation that proves capacity is required but cannot recover individual request identities or their original order. Both forms must survive process or leader replacement without inventing identity. They must also compete for the same bounded pool without a failed oldest attempt blocking every later obligation indefinitely.

Provider startup, GitHub acquisition, and either state database cannot share one transaction. Retry eligibility and ownership therefore need to be durable properties of the capacity obligation rather than properties of an in-memory worker or trigger queue. SQLite and PostgreSQL should express the same admission contract even though PostgreSQL additionally assigns work to compatible live nodes.

## Decision

We will represent every unit of requested capacity as a durable obligation in the canonical demand table for its backend. Each row will record its source as either `job_available` or `assigned_count`, its queue time and observation order, and durable retry state including `attempt` and `next_attempt_at`. A `job_available` obligation will carry its positive GitHub request ID. An `assigned_count` obligation will leave the request ID absent, link to the assigned-job observation that created it, and retain the repository-controller epoch that fences stale observations.

We will use one FIFO admission order across both sources: queue time first and durable observation ID second. Scheduling will select the oldest obligation that is due and runnable. An obligation in backoff, lacking a usable session, or otherwise temporarily ineligible will remain durable but will not prevent a later runnable obligation from using available capacity. A failed attempt will update its durable retry fields, so restart or leader replacement preserves both the obligation and when it becomes eligible again.

We will keep request identity optional only in the demand and assignment model. Aggregate obligations and their PostgreSQL assignments will reference both the canonical demand row and the assigned observation. They will never fabricate a GitHub request identity or call request acquisition. When such an assignment reaches provider lifecycle handling, request ID `0` will remain the explicit adapter value meaning that no GitHub request identity exists. Positive-request obligations will retain the existing acquisition boundary.

We will apply the same obligation and selection contract in SQLite and PostgreSQL. PostgreSQL will continue to select a compatible live contributor and persist an assignment before host-local execution. SQLite will continue to execute under its exclusive single-process owner. The architecture will not prescribe a fixed worker count for obligation scheduling; durable eligibility, bounded operation contexts, and database state provide the concurrency and recovery boundary.

PostgreSQL migration 007 will introduce the unified obligation shape and advance the schema compatibility floor. Every cluster node must be stopped for the migration and binary rollout. SQLite schema version 6 will introduce the equivalent standalone shape and migrate automatically while the database is exclusively opened by the stopped-node replacement process.

## Consequences

Both runtime modes retain positive and identity-free capacity obligations, FIFO position, and retry eligibility through restart. Assigned-count recovery no longer depends on a leader-owned trigger queue or a fixed number of recovery workers. PostgreSQL assignments have one durable demand parent regardless of source, while the assigned observation still proves the aggregate obligation's origin and epoch.

FIFO now means oldest runnable work rather than strict head-of-line blocking. A failed or temporarily ineligible oldest row can be overtaken while it is in backoff, but it regains priority among runnable rows when due. This improves pool utilization and makes retry behavior explicit at the cost of allowing observable execution order to differ from raw queue order during failures.

Aggregate statistics still cannot restore individual GitHub job identity or original queue order. Their obligations cannot be acquired by request ID, and lifecycle request ID `0` must never be treated as a demand identity. Operators must perform the PostgreSQL 007 rollout with every node stopped; mixed binaries are unsupported across this compatibility boundary. SQLite upgrades retain the existing exclusive-owner requirement.
