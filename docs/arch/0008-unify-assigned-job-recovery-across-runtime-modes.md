# 8. Unify assigned-job recovery across runtime modes

Date: 2026-08-02

## Status

Superseded by [9. Schedule assigned recovery through a bounded leader executor](0009-schedule-assigned-recovery-through-a-bounded-leader-executor.md)

Amends [7. Persist repository control independently from message sessions](0007-persist-repository-control-independently-from-message-sessions.md)

## Context

GitHub scale-set sessions report an aggregate `TotalAssignedJobs` statistic when a session is created and with subsequent messages. That count includes jobs already assigned to the scale set even when no recoverable `JobAvailable` message remains. Positive request identities and their queue order are therefore a different source of demand from the aggregate assigned count.

Standalone mode already uses the aggregate count to recover missing runners, but the policy was embedded in its coordinator. Clustered mode previously required every schedulable entry to have a positive request ID and ignored the aggregate count. After a leader or process replacement, this could leave already-assigned jobs without runner capacity even though a newly created session reported them.

ADR 7 keeps message sessions process-local because `actions/scaleset` v0.4.0 cannot adopt an existing session through its supported API. Every controller must recreate sessions after restart, reload, or leader replacement. Recovery therefore needs to consume the initial session statistics before polling begins, continue from later statistics, and remain safe when an older controller or session finishes work late.

The two runtime modes have different coordination requirements. SQLite has one process and retains its existing local demand and runner-lifecycle ownership. PostgreSQL coordinates multiple nodes, so an aggregate observation and every runner started for it need durable, epoch-fenced identities that survive leader and executor replacement. The aggregate count cannot reconstruct individual GitHub job identity or queue order.

## Decision

We will reconcile assigned-job statistics through one shared controller policy. Initial statistics from a newly created message session and statistics on every subsequent message will drive the same controller. Session activation will complete initial assigned-job reconciliation before polling begins. Each active session will own its reconciliation thread, serialize overlapping observations as single-flight work, and wake at least every five seconds as a safety path when no new message arrives.

We will keep positive-identity FIFO demand distinct from identity-free assigned recovery. Positive request IDs and queue timestamps will continue to come only from GitHub messages and will receive capacity first. The assigned-job controller will then start only the remaining deficit within the same pool limit. It will never fabricate a request ID. A lower aggregate count will request cleanup only for excess waiting identity-free runners; busy runners will remain protected.

We will adapt the shared policy to each state backend. Standalone mode will continue using SQLite, one-node provider inspection, and its existing runner lifecycle. PostgreSQL migration 005 will persist the latest assigned-job observation, fence it with the repository controller epoch, and allow distributed assignments to refer either to positive demand or to an assigned observation. Clustered identity-free assignments will use the durable assignment ID to derive the provider runner name, while the runner lifecycle will retain request ID `0` to express the absence of GitHub request identity. Assignment and lifecycle state will continue to bound node and pool capacity across leader or process failure.

We will preserve process-local scale-set sessions and recreate them through the supported `actions/scaleset` v0.4.0 session-creation API. A replacement leader will publish a newer controller epoch before its observations or assignments are accepted, so stale sessions cannot overwrite current recovery state.

The controller policy and backend capability boundary are the intended common scaling stack. This decision does not complete the remaining standalone daemon wiring consolidation; SQLite coordination remains single-node and PostgreSQL coordination remains multi-node.

## Consequences

Both runtime modes recover assigned work reported by a new session without waiting for another GitHub message. Later statistics, periodic safety wakes, and serialized reconciliation converge the runner count without admitting duplicate work. Identifiable FIFO demand retains priority and its durable ordering guarantees.

PostgreSQL retains assigned observations and their distributed assignments through leader and executor replacement. Stable assignment-derived runner names suppress duplicate clustered provisioning even though the lifecycle request ID is `0`. Controller epochs fence observations and assignments from an obsolete leader. SQLite retains its simpler exclusive-owner model while applying the same assigned-deficit policy.

Migration 005 advances the PostgreSQL compatibility floor. A coordinated upgrade must stop all nodes before the new schema is introduced, but the runtime remains a multi-node cluster after startup. Rollback removes identity-free assignments only after marking their retained runner lifecycle for cleanup.

Aggregate statistics still cannot restore per-job identity or order. Identity-free recovery cannot acquire a particular request, participate in positive-ID FIFO ordering, or prove which GitHub job a runner will execute. Provider startup, GitHub registration, and database transitions remain separate failure domains, so durable identity and lifecycle reconciliation are still required.
