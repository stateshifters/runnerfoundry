# 5. Own runner lifecycle with a bounded waiting lease

Date: 2026-07-27

## Status

Accepted

Amends [4. Coordinate multi-host scaling in PostgreSQL](0004-coordinate-multi-host-scaling-in-postgresql.md)

Amended by [12. Retire the legacy libvirt lifecycle bridge](0012-retire-the-legacy-libvirt-lifecycle-bridge.md)

## Context

GitHub's scale-set statistics are the authority for desired capacity, but a provider instance is not necessarily usable capacity. A runner can be provisioned for an assignment that is canceled before the runner acquires it. GitHub then removes the runner registration while Docker or libvirt continues to report the provider instance. Counting that instance indefinitely can fill `max_runners` and prevent replacement demand from starting.

`JobStarted` and `JobCompleted` identify exact runners. `TotalAssignedJobs` describes aggregate demand. GitHub, the state database, the provider, and message acknowledgement cannot participate in one transaction, so recovery must preserve cleanup intent and make every transition idempotent.

ADR 4 made assignments durable in clustered mode, but cleanup ownership was limited to libvirt. Docker relied on the runner process exiting by itself, and neither provider had a bounded lease between confirmed provider startup and `JobStarted`.

## Decision

RunnerFoundry will persist provider-independent runner lifecycle ownership in standalone and clustered modes. Each owned runner moves through `provisioning`, `waiting_for_job`, `busy`, and `reap_pending`.

The lifecycle record will contain the node and assignment where applicable, repository, scale set, provider, runner name, request identity, GitHub runner identity, timestamps, and cleanup reason. RunnerFoundry will create `provisioning` before provider startup and move it to `waiting_for_job` only after startup succeeds. A startup failure retains a cleanup obligation when provider state may be ambiguous.

`JobStarted` will persist the exact runner as `busy` before its message is acknowledged. `JobCompleted` will atomically make matching lifecycle rows `reap_pending`, complete terminal demand, and preserve exact cleanup ownership. Repeated lifecycle messages and cleanup attempts will be safe.

Every scale set will have `job_start_timeout`, defaulting to `10m`. It accepts any strictly positive Go duration and cannot be disabled. The lease starts when provider startup is confirmed. A waiting runner that does not receive `JobStarted` before the lease expires becomes `reap_pending`. Reloaded values apply to existing waiting runners by comparing the new duration with their persisted `waiting_since`. Busy runners never expire under this lease.

`TotalAssignedJobs` remains the scaling authority. A decrease immediately condemns excess identity-free waiting runners. When apparent capacity blocks demand, RunnerFoundry will verify waiting registrations with GitHub. A missing registration is immediately reapable. Provider instances created before this schema are adopted conservatively: registered instances are protected as busy, while instances already absent from GitHub are reaped.

Cleanup will target only the provider instance and GitHub registration named by the lifecycle record. `reap_pending` continues to consume capacity until both are confirmed absent. Failures retain the record and are retried with bounded provider and GitHub operations. Confirmed cleanup immediately permits retained demand to be scheduled again.

SQLite schema version 4 will add the generic lifecycle table, atomically backfill the released libvirt table, retain the legacy table for one compatibility window, and mirror legacy inserts, updates, and deletes. New libvirt operations will dual-write through the compatibility table. PostgreSQL migration 003 will apply the same expand-and-backfill strategy and retain the old table. Its Down migration will refuse to discard Docker cleanup obligations that the older schema cannot represent.

## Consequences

A canceled or unregistered runner can no longer remain accepted as usable capacity indefinitely once GitHub and the provider respond. Restart, reload, and leader failover retain both the waiting lease and cleanup ownership. Docker and libvirt now have the same cleanup guarantees.

`reap_pending` intentionally reduces available capacity while cleanup is ambiguous. This can delay replacement during a provider or GitHub outage, but it prevents overcommitting a host or deleting an unrelated instance. Absolute recovery still depends on GitHub, the state database, and the owning provider host becoming reachable.

The lifecycle schema and reconciliation loop add state transitions, compatibility writes, and operational logs. A later release may remove the legacy libvirt tables only after the rolling-compatibility window closes.
