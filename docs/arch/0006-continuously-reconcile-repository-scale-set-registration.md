# 6. Continuously reconcile repository scale-set registration

Date: 2026-07-27

## Status

Superseded by [7. Persist repository control independently from message sessions](0007-persist-repository-control-independently-from-message-sessions.md)

Amends [2. Use GitHub App installations and shared runner capacity](0002-use-github-app-installations-and-shared-runner-capacity.md)

## Context

Repository discovery defines where RunnerFoundry should serve jobs, but creating a GitHub runner scale set and opening its message session are separate remote operations. Treating them as one rollback unit allowed a later failure to delete registrations that had already succeeded. Conversely, treating a provider process or an old session as proof of usable capacity allowed externally deleted or replaced scale-set registrations to remain unnoticed.

A repository can use several configured scale sets, potentially in the same runner group. Serving only part of that set produces misleading capacity and can route some workflows while silently stranding others. GitHub audits can also fail transiently, so an inconclusive snapshot must not tear down a healthy session generation.

ADR 2 established GitHub App discovery as the repository authority. ADR 4 established leader-owned clustered sessions, and ADR 5 made runner/provider ownership durable. This decision amends those boundaries by separating durable registration convergence from atomic service activation.

## Decision

Every discovery reconciliation will audit the complete desired registration set for each returned repository. Registrations are listed once per distinct runner group. Missing registrations are created independently, and successful creations are retained when another creation or session startup fails.

A repository becomes operational only after every configured registration is confirmed and every message session in one generation starts successfully. Partial session generations are closed and detached from scheduling without deleting registrations, demand, provider instances, or runner lifecycle records. A later reconciliation reuses confirmed registration IDs and creates only missing registrations.

Active sessions retain the exact GitHub scale-set ID from which they were created. Each periodic audit compares those IDs with the current snapshot. A transient audit error marks reconciliation degraded but preserves the active generation. Confirmed absence or ID replacement suspends admission for that repository, repairs the desired registrations, replaces the complete session group, and then resumes scheduling.

Standalone and clustered modes use `github.discovery_interval` for this audit. A cluster leader also audits immediately after election and reload. Failures remain isolated to their repository. Reload confirms the replacement registration set before stopping the old standalone configuration, and optional pruning runs only after the replacement sessions activate.

Runner session replacement does not make provider work terminal. Busy runners and their durable capacity charge survive the replacement generation. Cleanup begins only after a terminal lifecycle event or authoritative provider inspection confirms that the exact owned instance is absent.

## Consequences

One failed registration can no longer erase successful peers, and an externally deleted registration is repaired without manual container or scale-set deletion. Repository activation and repair are atomic from the scheduler's perspective, while registration creation remains monotonic and retryable.

Audits add periodic GitHub API calls: one runner-group lookup and one scale-set list per distinct runner group and repository. During an audit outage, an already operational repository continues to use its last confirmed generation; an incomplete repository remains inactive. Recovery still depends on GitHub responding and on session creation eventually succeeding.

Registration records are not new local durable data. Desired state remains configuration plus App discovery, while exact active IDs live with the session generation. Demand, assignments, runner lifecycle, and provider ownership remain in the existing SQLite or PostgreSQL stores.
