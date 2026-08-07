# Architecture

The [product contract](PRD.md) is the authority for RunnerFoundry behavior. This document explains the system boundaries that keep that behavior source-neutral and consistent across deployment modes.

## Components

RunnerFoundry has four core responsibilities:

1. **Ingestion** accepts a source request through an adapter and creates an internal job associated with a Runner Pool.
2. **Queueing** stores jobs by Runner Pool and returns the oldest matching job when that pool requests work.
3. **Execution** provisions one runner to attempt the selected job.
4. **Cleanup** removes the ephemeral runner and its processing record after execution ends.

The provisioned runner reports its workflow result directly to the Git source. The daemon does not observe or persist normal workflow results and has no result-delivery worker.

![Conceptual job ingestion, execution, reporting, and runner-registration flows](images/job-lifecycle.png)

## Source adapter boundary

A source adapter owns everything specific to a Git hosting or CI system:

- authentication and API clients;
- inbound request payloads;
- capacity publication;
- source-specific identifiers and terminology.

The adapter translates those details into the core terms defined by the product contract. Core ingestion, queueing, execution, and persistence contracts use `Job` and `Runner Pool`; they do not expose source-specific types.

Source-specific registration and ownership persistence is an adapter-owned auxiliary port. State backends implement that port at their adapter edges without adding source-specific types or methods to the source-neutral state contract. Active registrations publish current routes; durable ownership records exist only to verify safe source cleanup and do not create jobs or capacity.

GitHub Actions is the only supported source. Its adapter maps GitHub runner scale sets and job requests to Runner Pools and internal jobs. GitHub does not expose a supported operation for reporting a provisioning failure for an individual job, so RunnerFoundry cleans up and retries the same job without manufacturing a result. If a source later provides that operation, reporting remains specific to its adapter and must not introduce a persisted result lifecycle into the core.

The GitHub adapter applies messages in their lifecycle order. `JobAvailable` acquires and enqueues demand, and `JobAssigned` provides idempotent acquisition recovery. `JobStarted` durably records that the runner accepted the job. Every `JobCompleted` message is terminal regardless of its result value. The adapter acknowledges each message only after its required state change is durable.

A delayed `JobStarted` can race with a provisioning retry. If the matching request has already returned to the queue, the adapter records a source tombstone and removes that queued retry so another runner cannot execute it. A `JobCompleted` message also records a tombstone and removes queued demand. Tombstones prevent redelivery or a concurrent retry from recreating demand.

## Queue boundary

The queue contains only jobs waiting to be picked up. FIFO ordering is local to each Runner Pool. A request for work selects the oldest queued job matching the requesting pool; jobs for other pools do not participate in that selection. Equal enqueue timestamps are equally eligible.

Pickup atomically removes the selected job from the queue and creates a processing record. The record includes the claiming member, claim time, exact inventory endpoint, and durable `runner_started` boundary. It contains no workflow result or broader attempt-state machine.

The queue represents source demand only. RunnerFoundry does not manufacture jobs from capacity observations or create a second scheduling model.

## Execution boundary

Execution providers supply ephemeral runner environments. Docker Compose and libvirt are the current providers. Launching uses the provider configured for the Runner Pool, while reconciliation uses one aggregate of every provider inventory represented by the active configuration. Docker endpoint identity includes the member hostname and normalized Compose mechanism; libvirt endpoint identity uses the normalized connection URI. Duplicate endpoints are queried once per maintenance cycle. Each observation includes its Runner Pool, and the cycle snapshot is partitioned for pool reconciliation and occupied-capacity calculation.

Inventory is fail-closed: every represented endpoint must answer before RunnerFoundry treats a runner as absent. An error means runner state is unknown and processing is retained. Failed provisioning performs a fresh inspection of the recorded endpoint before cleanup or absence confirmation. Removing an endpoint never turns invisibility into proof of absence; processing recorded against an unknown endpoint is retained for operator action.

The provisioned runner communicates its normal workflow outcome directly to the Git source. Before runner acceptance, RunnerFoundry owns provisioning failures. After cleanup succeeds or resource absence is confirmed, it atomically moves the same unstarted job to the back of its Runner Pool queue and removes the processing record. A tombstone or concurrent runner-start observation prevents that retry. There is no retry count, delay, backoff, or timeout.

`JobStarted` transfers outcome ownership to the runner and GitHub. Once that boundary is durable, a missing, failed, or terminated provider resource never returns the job to the queue. RunnerFoundry retains processing while it awaits `JobCompleted`, attempting cleanup when a resource may remain. A terminal message removes processing only after cleanup succeeds or absence is confirmed; cleanup failure retains processing for another cleanup attempt.

## Member ownership boundary

A member is identified by its trimmed operating-system hostname. Hostnames must be stable and unique within the cluster, and only one daemon may run for a hostname. Ordinary processing remains member-local, but the leader may recover an unstarted claim after its fixed five-minute lease or clean terminal processing across members.

Members heartbeat to advertise liveness. The 30-second cutoff removes only advertised capacity. It does not authorize processing recovery. After five minutes, an unstarted, non-terminal claim is recoverable only when the leader can inspect its exact recorded endpoint and either destroy the deterministic runner or positively confirm absence. An unknown, unavailable, or incomplete endpoint retains processing. The guarded state transition loses to a concurrent `JobStarted` or terminal signal, and started jobs never return to the queue.

## Capacity boundary

Runner Pool registration and capacity publication are orthogonal to the job lifecycle. Members atomically renew their heartbeat and publish their complete configured-capacity snapshot. Members that have not heartbeated within 30 seconds, or that have gracefully withdrawn, contribute no capacity.

The state store calculates one shared value for each Runner Pool:

`available = max(0, live configured capacity - queued jobs - all processing jobs)`

Moving a job from the queue into processing does not change availability. Cancelling queued work or removing completed processing releases capacity. Capacity queries observe current work but cannot create, claim, reorder, move, or remove it.

GitHub discovery replaces active registrations and durable scale-set ownership in one state transaction. Member routes and leader message sessions use only the committed active registrations. On source leadership acquisition, the committed active snapshot is the reconnect fast path: fresh repository session generations open without waiting for discovery, while discovery starts independently. An inconclusive discovery failure retains that committed authority and its usable sessions. Ownership survives configuration removal and temporary discovery absence, while pruning considers only stored ownership for repositories that are currently discovered. Before deletion, pruning rechecks the numeric runner-group ID, scale-set ID, and name; mismatched fingerprints remain owned for later verification, and labels or update settings are not ownership evidence.

For GitHub, one leader owns repository discovery and message sessions. A leadership term has a cancellable fence. Losing leadership cancels every poll and source operation before cleanup, and late term results cannot handle or acknowledge messages or install sessions. Cleanup failure cannot restore the old term or prevent a new term. Within one term, repositories reconcile concurrently with a fixed limit of four while Runner Pool operations remain serialized inside each repository. A repository session generation becomes visible only after all of its configured Runner Pool sessions open; unchanged and known-good generations remain active until a complete replacement is ready. Each repository-scoped runner scale set representing a Runner Pool receives that pool's shared availability immediately before its message request. Repository requests run concurrently, while each session has at most one request in flight. After a message arrives, other requests for that pool are cancelled and restarted with fresh availability. Independent source requests can still observe the same pre-ingestion snapshot; RunnerFoundry does not add reservations or another scheduler to hide that external consistency limit.

The GitHub representation supports repositories installed under a personal account and is not part of the core queue or execution interfaces. Leadership publishes capacity and ingests messages only. Every active generation also owns one member-local durable-queue processor, independent of source leadership. It starts during activation and waits only for a successful local inventory refresh and reconciliation before claiming from an eligible Runner Pool. A successful local enqueue causes an immediate coalesced wake after persistence, while maintenance wakes discover pre-existing and remotely ingested jobs. Provider-facing maintenance and processing attempts share one gate, and the processor releases it after every attempt so maintenance cannot be starved.

## Reload and shutdown boundary

The daemon-wide `--active-runner-policy=leave|terminate` option controls active resources during reload and graceful shutdown. It defaults to `leave` and is intentionally not part of YAML configuration, so a replacement configuration cannot choose how resources created by the previous generation are handled.

Reload validates and prepares the replacement without claiming work, then deactivates the old generation and activates the replacement. Deactivation cancels and joins both maintenance and queued-job processing before releasing the local gate, so old and replacement generations cannot claim concurrently. Activation commits after the replacement acquires the gate and starts both loops, so `READY` certifies configuration validation, required local and durable-state initialization, provider validation, and local generation ownership. It does not certify Git source discovery, message sessions, inventory, reconciliation, recovery, claims, or provisioning. The first maintenance cycle starts immediately in the background and controls operational health; its failure degrades the active generation without rejecting it. A preparation or local-activation failure closes or rolls back the replacement and reactivates the old generation. The old generation is closed only after activation commits. Close timeouts leave unfinished resource cleanup retryable.

With `leave`, the replacement is applied without explicitly destroying affected runners. Reconciliation then observes the provider-agnostic inventory defined by the replacement configuration. Visible running resources are retained. An unstarted job whose recorded endpoint proves absence can return to the queue after the five-minute recovery lease, while started processing waits for GitHub's terminal message. Processing tied to an operator-removed endpoint is retained for operator action.

With `terminate`, the old provider destroys affected resources after replacement activation commits. An unstarted job returns to the queue only after cleanup succeeds; a started job remains in processing until GitHub terminalizes it. Cleanup failure retains processing, but does not prevent the otherwise-valid replacement configuration from taking effect.

On graceful shutdown, `leave` stops claims and polling, withdraws capacity, and preserves all runners and processing. `terminate` destroys owned resources, requeues successfully cleaned-up unstarted jobs, and leaves started jobs awaiting GitHub completion.

## Deployment boundary

One RunnerFoundry member is a standalone deployment. It may use SQLite or PostgreSQL. SQLite is exclusively owned by that one member.

Two or more RunnerFoundry members form a cluster and share PostgreSQL. PostgreSQL provides the shared state needed by those members; it does not change the product lifecycle.

The state relationships are:

```mermaid
erDiagram
    RUNNERFOUNDRY_MEMBERS ||--o{ RUNNERFOUNDRY_MEMBER_RUNNER_POOLS : advertises
    RUNNERFOUNDRY_MEMBERS ||--o{ RUNNERFOUNDRY_PROCESSING_JOBS : owns

    RUNNERFOUNDRY_MEMBERS {
        text member_id PK
        timestamptz heartbeat_at
    }
    RUNNERFOUNDRY_MEMBER_RUNNER_POOLS {
        text member_id PK,FK
        text runner_pool PK
        integer capacity
    }
    RUNNERFOUNDRY_QUEUED_JOBS {
        uuid job_id PK
        text source_name
        text source_reference
        text runner_pool
        timestamptz enqueued_at
    }
    RUNNERFOUNDRY_PROCESSING_JOBS {
        uuid job_id PK
        text source_name
        text source_reference
        text runner_pool
        text member_id FK
        timestamptz claimed_at
        text inventory_endpoint_id
        boolean runner_started
    }
    RUNNERFOUNDRY_SOURCE_TOMBSTONES {
        text source_name PK
        text source_reference PK
    }
    RUNNERFOUNDRY_GITHUB_REGISTRATIONS {
        text repository_url PK
        text runner_pool PK
        bigint installation_id
        bigint runner_group_id
        bigint scale_set_id
    }
```

```mermaid
flowchart TB
    Adapter["Source adapter"] --> Core["Ingestion, Runner Pool queues, processing ownership"]
    Core --> State["SQLite or PostgreSQL"]
    Core --> Provider["Docker Compose or libvirt"]
    Provider --> Source["Provisioned runner reports workflow result to Git source"]
    Capacity["Runner service capacity"] --> Adapter
```

## Dependency rule

Dependencies point inward from adapters, state backends, and execution providers toward source-neutral core contracts. Adding a Git source, database backend, or execution provider must not add another job lifecycle.
