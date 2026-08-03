# 9. Schedule assigned recovery through a bounded leader executor

Date: 2026-08-03

## Status

Accepted

Supersedes [8. Unify assigned-job recovery across runtime modes](0008-unify-assigned-job-recovery-across-runtime-modes.md)

Amends [7. Persist repository control independently from message sessions](0007-persist-repository-control-independently-from-message-sessions.md)

## Context

ADR 8 established one backend-neutral policy for recovering the aggregate assigned-job count reported by GitHub scale-set sessions. It also placed clustered reconciliation in one goroutine per active session, coupled message acknowledgement to that reconciliation, and used a five-second safety audit for every session.

The number of active sessions grows with repositories and scale sets. Per-session goroutines and unconditional safety audits therefore make idle control-plane work grow with session count. A slow PostgreSQL or provider operation also holds the poller's message acknowledgement boundary, allowing recovery failure to redeliver a message whose durable statistics and demand effects already succeeded.

Recovery still needs independent retry and prompt reaction to relevant state changes. It must not let one slow repository serialize unrelated recovery, create unbounded concurrency, or leave shutdown waiting on unbounded work. The leader already owns the GitHub sessions and controller epoch, so it is the authority that can schedule recovery without making the session itself own its execution.

`JobStarted` can report a runner name without a usable request identity for aggregate recovery. The stable runner name therefore needs to be sufficient identity for an identity-free lifecycle, while positive-demand messages retain their existing positive-request identity.

## Decision

We will preserve ADR 8's backend-neutral assigned-job controller policy, durable epoch-fenced PostgreSQL observations and assignments, SQLite lifecycle behavior, positive-ID FIFO priority, and aggregate-count limitations. Process-local `actions/scaleset` v0.4.0 sessions will still be recreated through the supported session-creation API after restart, reload, or leader replacement.

We will replace per-session assigned-recovery goroutines with one leader-owned executor containing four workers. Work will be keyed by repository and scale set, coalesced so that one key has at most one queued or running recovery, and executed from the latest durable observation. Each attempt will use bounded coordination and provider contexts. Relevant statistics and capacity changes will submit recovery triggers; failed or dirtied work can be scheduled again, but a clean session will not receive a periodic safety audit.

We will separate statistics publication from recovery execution. Session bootstrap will publish its initial statistics to leader-owned recovery state and submit recovery before polling starts, without waiting for runner recovery to finish. Each later message will publish its latest statistics, commit its other durable effects, submit a trigger, and complete the normal acknowledgement boundary independently of recovery. The worker will persist the epoch-fenced PostgreSQL observation as part of its bounded attempt. Trigger submission and coalescing outcomes will be visible through structured `assigned_recovery_trigger` events. Recovery attempts will continue to use `assigned_recovery` events.

We will accept a `JobStarted` message with either a positive request ID or a non-empty runner name. A runner-name-only message will select the lifecycle by its exact runner name and will not invent or rewrite a request ID. This keeps identity-free lifecycle request ID `0` while preserving the positive-demand path.

Leader shutdown will stop accepting recovery triggers, cancel the executor, and wait for its four workers to finish their bounded calls before releasing leader-owned state. Session shutdown will no longer wait for a session-specific recovery goroutine.

## Consequences

Message acknowledgement no longer waits for assigned recovery to persist the observation, inspect capacity, or start runners. Executor triggers make recovery independently retryable, while a replacement process or leader receives a fresh initial statistic from its recreated session. A provider or recovery-store failure cannot force message redelivery solely to retry assigned recovery.

Recovery concurrency is constant rather than proportional to the number of sessions. Four unrelated repository and scale-set keys can make progress concurrently, while coalescing prevents repeated observations from creating an unbounded queue. Clean sessions create no periodic recovery traffic. Recovery may begin shortly after acknowledgement rather than before it, and coalescing intentionally discards intermediate aggregate counts in favor of the latest observation.

Runner-name-only `JobStarted` handling lets identity-free runners become busy without fabricated request identity. A message still needs either its positive request identity or its exact runner name before it can affect lifecycle state.

Shutdown has an explicit recovery boundary and remains bounded by the executor's operation contexts. Cancellation can leave durable observations or assignments unfinished; a successor reconstructs work from PostgreSQL and newly recreated session statistics under its newer controller epoch.
