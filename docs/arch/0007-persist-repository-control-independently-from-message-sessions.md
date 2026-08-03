# 7. Persist repository control independently from message sessions

Date: 2026-08-02

## Status

Accepted

Supersedes [6. Continuously reconcile repository scale-set registration](0006-continuously-reconcile-repository-scale-set-registration.md)

Amended by [8. Unify assigned-job recovery across runtime modes](0008-unify-assigned-job-recovery-across-runtime-modes.md)

## Context

ADR 6 made repository activation atomic while retaining successful GitHub scale-set creations. It deliberately kept repository authority and confirmed registration IDs in process memory. A restart or clustered leader change therefore had to finish fresh discovery and repeat every registration audit before it could reconnect message polling. A transient discovery outage could make otherwise confirmed repository control unavailable precisely when recovery was needed.

GitHub repository discovery, scale-set registration, and scale-set message sessions have different lifetimes. Repository identities and confirmed scale-set registrations are stable facts that can be reused after process failure. Message sessions are leases owned by one process. The current `actions/scaleset` API does not expose an operation that adopts an existing message session, so persisting session identifiers would not make sessions recoverable.

Repository work can also block independently. Bootstrap reconnects, fresh discovery, desired-state calculation, and registration or session repair must not make unrelated repositories wait behind one slow GitHub operation. At the same time, authority removal must be based on a conclusive discovery result rather than a broad cleanup after a partial or failed pass.

## Decision

We will persist GitHub App repository authority and confirmed scale-set registrations in the selected state backend. Standalone mode will store them in the exclusively owned SQLite database. Clustered mode will store them in PostgreSQL through migration 004. The durable catalog will contain discovery generations, the last successful discovery time, installations, repository identities, runner-group identity, and confirmed GitHub scale-set IDs.

We will keep message sessions process-local. Startup and leader acquisition will increment a durable controller epoch, fencing repository-state mutations from an older controller. A controller will bootstrap repository work from the last initialized catalog without waiting for fresh discovery. Both modes will audit cached registrations through the supported scale-set API and create new message sessions; stored registration IDs are not session-adoption authority. Confirmed registrations are persisted while discovery runs independently. Both modes will transactionally publish fresh repository authority, calculate desired state, and reconcile each repository with bounded GitHub operations. A successful result for one installation may remove repositories missing from that installation, while an installation whose repository listing failed retains its previous repository and registration rows. A failed discovery pass will not broadly prune the durable catalog.

We will use four repository workers shared by bootstrap, reconciliation, and repair. At most three workers will accept routine full-pass work, reserving one worker for urgent reconnect or repair work. Work will be coalesced by repository and each GitHub attempt will have a 30-second timeout. Failed repository work will retry after 5, 10, 20, 40, and then at most 60 seconds. The repair schedule will remain process-local because the durable catalog and desired state are sufficient to reconstruct it after restart or leadership change.

We will distinguish stale discovery from leader degradation. Once a durable catalog has been initialized, a discovery failure will retain and serve that catalog, report it as stale, and leave the cluster leader operational. Failure before any successful catalog exists, failure to load clustered desired state, or failure to publish repository state will leave the leader degraded. Individual repository registration or session failures will remain isolated repair work and will not degrade otherwise established leader authority.

## Consequences

A standalone restart or replacement cluster leader can restore polling from durable repository and registration facts while GitHub discovery is unavailable. Slow or failing repositories no longer serialize the whole repository set, and urgent recovery retains a worker even during a large routine reconciliation pass. Controller epochs prevent a canceled or replaced controller from committing late mutations.

The state schemas now own repository-control history in addition to demand and runner lifecycle. SQLite creates the new tables when the store opens. PostgreSQL upgrades automatically through reversible migration 004 before node publication. Operators must preserve and back up these records with the rest of the selected state backend.

Message sessions still disappear with their owning process and must be recreated through the library's supported session-creation API. Confirmed registration IDs remain durable audit facts, but they do not prove that a new session can start. The bounded, non-durable repair queue can lose its current delay on restart; recovery reconstructs the work and may retry sooner.

Serving a stale catalog favors continuity over immediate revocation during an inconclusive discovery outage. A conclusive installation or repository removal still updates durable authority and schedules scoped cleanup. Operators must use status and structured discovery logs to distinguish stale authority from a leader that cannot establish desired state.
