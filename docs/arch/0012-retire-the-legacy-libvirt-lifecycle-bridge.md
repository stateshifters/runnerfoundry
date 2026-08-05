# 12. Retire the legacy libvirt lifecycle bridge

Date: 2026-08-05

## Status

Accepted

Amends [5. Own runner lifecycle with a bounded waiting lease](0005-own-runner-lifecycle-with-a-bounded-waiting-lease.md)

## Context

ADR 5 introduced provider-independent runner lifecycle ownership while retaining the released libvirt representation for one compatibility window. PostgreSQL migration 003 and SQLite schema version 4 backfilled the generic lifecycle and mirrored legacy writes so adjacent releases could overlap safely. That documented window has elapsed.

Keeping both representations beyond the window creates two lifecycle authorities. Mirror writes, repair paths, coordinator fallbacks, and daemon branches duplicate transition and recovery policy. A disagreement can make cleanup depend on which representation a caller reads, while every safe change must preserve both implementations.

The contraction must not discard a legacy-only cleanup obligation or silently choose between conflicting records. It must also preserve a deliberate downgrade path for operators who keep the contracting binary available.

## Decision

We will make the generic runner lifecycle the only runtime and persistence authority for Docker and libvirt.

PostgreSQL migration 009 will aggregate-preflight the legacy libvirt table before any mutation. It will refuse the transaction with `legacy_only` and `conflicting` counts when a legacy row has no generic equivalent or disagrees on provider, request identity, or pending-reap state. After a clean preflight, it will remove the legacy trigger, function, and table and advance the compatibility floor to schema generation `9`.

SQLite schema version 8 will apply the same aggregate preflight and contraction under the standalone database's exclusive process ownership. Fresh databases will still execute the historical expand migrations before immediately contracting to the current schema.

The minimum compatible RunnerFoundry version is the first release containing schema generation 9. A binary whose maximum schema generation is 8 or lower must not start against the contracted PostgreSQL schema. RunnerFoundry does not support a mixed-version cluster across this boundary.

PostgreSQL rollback will require every node to remain stopped. Operators will run `migrate down --to 8` with the generation-9 binary before installing an older release. The Down migration will recreate the version-8 table, indexes, and trigger and repopulate every libvirt row from generic lifecycle data without changing generic lifecycle history. Standalone rollback will restore a stopped-service backup taken before SQLite schema version 8 because standalone startup has no Down migration command.

## Consequences

Lifecycle transitions, cleanup recovery, persistence, and coordinator behavior have one provider-independent implementation. Docker and libvirt retain the same observable lifecycle contract through generic APIs, while bridge reconciliation and its failure modes disappear.

Migration 009 scans both lifecycle representations and briefly takes `ACCESS EXCLUSIVE` locks while dropping legacy objects. The rollout therefore requires a full-cluster maintenance window, a verified backup, and sequential restart after migration. Unsafe source data stops the migration before schema mutation and requires operator repair while the cluster remains stopped.

Downgrade reconstructs a compatibility projection, not a second history. Generic timestamps, assignment relationships, and cleanup reasons remain authoritative and unchanged. An older binary can express provider, request identity, runner name, and pending-reap state, but not every richer generic detail in its legacy table; those details remain in the generic lifecycle retained by schema generation 8.
