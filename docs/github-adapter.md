# GitHub adapter

The [product contract](PRD.md) defines RunnerFoundry's source-neutral behavior. The [architecture](architecture.md) defines the boundaries every source adapter must preserve. This document owns the GitHub-specific protocol and persistence rules inside those boundaries.

GitHub Actions is the only supported Git source today. The adapter supports repositories installed under a personal account and represents each configured Runner Pool as a repository-scoped GitHub runner scale set. A Runner Pool name is also the `runs-on` value published to GitHub.

For the upstream polling, messaging, demand, and assignment model, see
[GitHub runner scale-set mechanics](references/github-scaleset-mechanics.md).

## Demand and lifecycle translation

Each runner scale-set message can contain exact lifecycle events and an authoritative assigned-job count. The adapter validates and applies the exact events before reconciling the assigned count:

1. Actionable `JobAvailable` events are offered to GitHub's acquire operation. Only acquired requests become jobs.
2. `JobAssigned` events provide lifecycle history only. They do not create jobs or participate in acquisition.
3. Actionable `JobStarted` events durably record runner acceptance.
4. Actionable `JobCompleted` events terminalize the matching job regardless of the event's `Result` value.
5. The message's assigned-job statistic reconciles the total durable demand for that repository and Runner Pool.
6. The adapter acknowledges the message only after every required state change succeeds.

A message without its required statistics fails handling and remains unacknowledged. A durable-handling or acknowledgement failure leaves the acknowledgement cursor unchanged so GitHub can redeliver the message.

`JobAvailable` is the only exact demand input. It is actionable only when it identifies a positive runner request, and duplicate entries for the same request within one message are considered once. Positive, zero, negative, nil, and redelivered `JobAssigned` entries are non-actionable protocol history: they create no job, require no identity validation, and do not block acknowledgement.

Runner-started and runner-completed events may identify work by a positive runner request ID or by runner name. A zero request ID therefore remains actionable when the event carries the RunnerFoundry runner name. Nil events, negative request IDs, and events with neither identity are recorded as ignored evidence. They do not change job state. An acknowledgement failure after durable handling leaves the message available for redelivery, so all translated state changes remain idempotent.

GitHub's initial session statistics are reconciled before ordinary message polling. A session without its initial statistics cannot begin polling. Later messages reconcile their statistics after all actionable lifecycle events in that message are durable.

GitHub's `TotalAssignedJobs` value is the desired active demand for one repository and Runner Pool. Exact queued jobs and all processing jobs count toward that total. A deficit becomes deterministic aggregate queued demand. A contraction removes only the newest aggregate queued jobs; it never removes exact or processing demand merely to match the statistic.

Aggregate entries preserve the authoritative demand count, not an individual GitHub job identity or GitHub's original queue order. The adapter uses the observation time as their FIFO time because the statistic exposes neither per-job identity nor ordering. Once an exact or aggregate entry is durable in `runnerfoundry_queued_jobs`, claiming and execution use the source-neutral job and Runner Pool contracts; the GitHub adapter does not control which member claims it.

## Acceptance, terminalization, and redelivery

`JobStarted` is GitHub's runner-acceptance signal. Once it is durable, a missing or failed provider resource cannot return the job to the queue. The provisioned runner owns normal workflow-result delivery to GitHub.

An actionable `JobCompleted` is GitHub's terminal source signal and is submitted to the core terminalization boundary regardless of its `Result` value. It removes matching queued demand immediately. Unmatched observations are reported without manufacturing state. Matching processing remains only while provider cleanup is incomplete or resource absence is not yet proven.

A delayed `JobStarted` can arrive after an unstarted provisioning attempt has returned to the queue. The durable transition records a source tombstone and removes that queued retry. A matched, durably applied `JobCompleted` also records a tombstone and removes queued demand. Tombstones prevent redelivery of that source identity or a concurrent provisioning retry from recreating the same demand.

Source tombstones are source-neutral lifecycle state keyed by source name and source reference. The GitHub adapter relies on them, but they are not adapter-owned registration or ownership records.

GitHub does not expose a supported operation for reporting an individual provisioning failure. RunnerFoundry therefore cleans up the failed attempt and retries the same unstarted job from the back of its Runner Pool queue. The adapter never manufactures a GitHub workflow result.

## Discovery and active registrations

The source leader discovers every installation visible to the configured GitHub App and every repository visible to each installation, including paginated results. Repository reconciliation runs with a fixed concurrency of four. Runner Pool operations remain ordered within each repository. Failure to enumerate installations or any installation's repositories prevents the pass from committing a replacement snapshot.

A successful reconciliation pass atomically replaces the complete active registration snapshot. Each active registration contains the repository URL, Runner Pool, installation ID, runner-group ID, and scale-set ID. Member bootstrap routes and leader message sessions use only committed active registrations.

When source leadership begins, the committed active snapshot is the reconnect fast path. The new leadership term opens fresh repository session generations from that snapshot while discovery runs independently. An inconclusive discovery failure does not replace the committed registrations or discard usable sessions. Later discovery starts one configured discovery interval after the preceding pass finishes.

A repository session generation becomes active only after sessions for all of that repository's configured Runner Pools open successfully. A failed partial generation is retired, and failed session cleanup remains owned for another cleanup attempt. An unchanged known-good generation remains active until a complete replacement is ready. Repositories open concurrently, while Runner Pool sessions within one repository open in registration order.

## Leadership and message polling

One source leader owns GitHub discovery, capacity publication, and message sessions. Each leadership term has a cancellation fence. Losing leadership cancels polls and source operations before cleanup. Results from an old term cannot handle or acknowledge messages, install sessions, or restore that term. Cleanup failure cannot block a new leader.

Each active session has at most one message request in flight. Immediately before requesting a message, the adapter publishes the shared availability for that Runner Pool. Repository requests run concurrently. After one poll durably handles a message or changes assigned demand, other in-flight polls for the same Runner Pool are cancelled and restarted with fresh availability.

Independent GitHub requests can still observe the same availability snapshot before either message is ingested. RunnerFoundry does not add reservations or another scheduler to hide that external consistency limit. Durable queue processing remains member-local on every active member and does not depend on GitHub leadership.

## Durable registration and ownership state

Active registrations and durable ownership serve different purposes:

- Active registrations are the current routes used for member bootstrap, capacity publication, and message sessions.
- Durable ownership records prove which runner scale sets RunnerFoundry may safely prune. They survive configuration removal and temporary discovery absence.

Both SQLite and PostgreSQL implement these adapter-owned records without adding GitHub types to the source-neutral state contract.

```mermaid
erDiagram
    RUNNERFOUNDRY_GITHUB_REGISTRATIONS {
        text repository_url PK
        text runner_pool PK
        bigint installation_id
        bigint runner_group_id
        bigint scale_set_id
    }
    RUNNERFOUNDRY_GITHUB_SCALE_SET_OWNERSHIP {
        text repository_url PK
        text runner_pool
        bigint installation_id
        bigint runner_group_id
        bigint scale_set_id PK
    }
```

The active-registration key is repository URL plus Runner Pool. The ownership key is repository URL plus scale-set ID. A successful reconciliation commits both collections in one transaction.

## Pruning safety

Pruning is disabled by default. When enabled, it considers stored ownership only for repositories present in the current successful discovery result. Ownership for an undiscovered or temporarily unavailable repository remains intact.

Before deleting an absent configured scale set, the adapter reads GitHub's current scale sets for the stored runner group and checks the scale-set ID, name, and runner-group ID against the ownership record. A matching scale set is deleted and its ownership removed. An already absent scale set loses its ownership record without another delete. A mismatched fingerprint remains owned for later verification and is not deleted. Labels and update settings are not ownership evidence.

GitHub configuration, credentials, and operator procedures remain in the root [README](../README.md#github-adapter-settings).
