# Core lifecycle boundary

`internal/core` is RunnerFoundry's source-neutral lifecycle boundary. It gives
source adapters, runtime services, execution contracts and state contracts a
shared domain vocabulary without giving any one integration ownership of the
product lifecycle.

The [product contract](../../docs/PRD.md) is authoritative for lifecycle
behaviour. The [architecture](../../docs/architecture.md) is authoritative for
package ownership and dependency direction. This document explains how the
core declarations realise those contracts; it does not define another
lifecycle.

## What core owns

Core owns:

- source-neutral job, processing, observation, retry, removal and capacity
  values;
- stable source, job, attempt, runner, member, Runner Pool and inventory
  endpoint identities;
- the ports through which source demand and observations enter runtime;
- the ports through which runtime obtains source-owned bootstrap and optional
  provisioning-failure operations;
- opaque bootstrap data passed from a source adapter to an execution provider;
  and
- source-neutral capacity snapshots supplied to source adapters.

Core does not own persistence implementations, source protocols, credentials,
source-specific routing, provider mechanics, lifecycle orchestration or daemon
composition. Persistence interfaces belong to `internal/state`; lifecycle
decisions belong to `internal/runtime`; source translation belongs to
`internal/source/<source>`; and concrete wiring belongs to `internal/daemon`.

The principal control flow is:

```text
source protocol -> source adapter -> core.SourceEventHandler -> runtime -> state/execution
runtime -> core.SourceOperationsResolver -> selected source adapter operations
runtime -> core.CapacitySnapshotProvider -> source adapter -> source protocol
```

Compile-time dependencies point from these concrete and application layers
towards core contracts. Core never imports a concrete source, provider, state
backend or daemon package.

## Identities

`SourceAdapter` is a stable namespace for one source type. It is not a
credential, repository or installation identifier. `SourceReference` combines
that namespace with an adapter-owned identifier, making source demand globally
unique and replay-safe. Routing among credentials, repositories or
installations remains private to the adapter.

`JobID` identifies one durable job across all provisioning attempts.
`AttemptID` identifies one claim of that job. A runner name encodes both so a
source observation can be correlated to the exact attempt without exposing a
source-specific type.

RunnerFoundry supports only the durable `runnerfoundry-v2-...` runner-name
format. `AttemptRunnerName` creates it and `ParseRunnerName` is its authority.
Parsing deliberately distinguishes an external name from a malformed name in
RunnerFoundry's namespace: external names return `owned == false`, while
malformed owned names return `ErrInvalidRunnerName` with `owned == true`.
Generated job and attempt identities must be non-zero.

`DemandScope` is different from job identity. It is an adapter-owned key used
to serialize mutations for one upstream demand queue.

## Lifecycle vocabulary

Runner acceptance is the source-observed no-retry boundary. Core therefore
uses `RunnerAccepted` and `ObserveRunnerAcceptance` for lifecycle state. The
physical database column remains `runner_started`; state backends translate
that storage spelling into `RunnerAccepted` at the persistence boundary.

A terminal observation means that the source has ended its demand. Runtime
still owns provider cleanup and removes processing state only through guarded
state transitions. Retry and removal outcomes preserve the distinction between
an applied transition, an idempotent replay, missing work and a concurrent
state change. Callers must act on the returned outcome rather than treating a
nil error as proof that state changed.

See the [product contract](../../docs/PRD.md#job-lifecycle) for transition order,
cleanup requirements and retry behaviour.

## Bootstrap

Bootstrap data is owned by the source adapter and opaque to runtime and
execution providers. `NewBootstrap` copies its input, and `Bootstrap.Content`
returns a copy, so no caller shares mutable bytes with another boundary. The
zero value is valid empty bootstrap data.

`RunnerBootstrapProvider` creates and cleans up bootstrap for jobs owned by one
source adapter. Cleanup must be idempotent because reconciliation may replay it;
an already absent bootstrap is success.

## Source ports

`SourceEventHandler` is the source-to-runtime port:

- `IngestDemand` durably accepts demand by stable source reference;
- `ObserveRunnerAcceptance` records the no-retry boundary; and
- `ObserveTerminal` records that source demand has ended.

Adapters may redeliver all three operations, so implementations return explicit
idempotent outcomes.

`RunnerBootstrapProvider` is the required runtime-to-source operation.
`ProvisioningFailureReporter` is an optional capability implemented only by a
source that can durably report that outcome. Reporting is idempotent by the
job's stable source identity. A nil error means the source durably accepted the
terminal provisioning failure; a failure leaves processing retained for
reconciliation.

Before runtime calls that optional port, `internal/state` reserves the exact
job attempt. Processing snapshots expose whether failure reporting is pending
or already recorded so another member can resume after a restart. Acceptance,
ordinary retry and ordinary removal cannot cross that reservation; terminal
source observations remain authoritative. State records successful reporting
before guarded finalization, while runtime keeps source I/O outside persistence
transactions.

`SourceOperationsResolver` selects operations strictly by the
`Job.Source.Adapter` namespace. Composition rejects duplicate namespaces. An
unknown namespace is an operational error and never falls back to another
adapter. `SourceOperations` retains the concrete bootstrap provider so runtime
can discover optional capabilities, including `ProvisioningFailureReporter`,
on the same configured source instance.

If a source does not implement failure reporting, runtime follows the product
contract's guarded retry path after cleanup succeeds. This policy is generic;
core contains no source-specific capability list or branch.

## Capacity boundary

`CapacitySnapshotProvider` supplies a `CapacitySnapshot` containing a Runner
Pool and its available slots. It hides state backends, liveness cut-offs and
capacity calculations from adapters. Adapters translate snapshots into their
own publication protocol and must not introduce another capacity model into
core.

## Adding or changing a source adapter

A source adapter is a peer implementation at `internal/source/<source>`. It:

- translates its protocol into `SourceEventHandler` calls;
- implements `RunnerBootstrapProvider`;
- implements `ProvisioningFailureReporter` only when the source genuinely
  supports durable terminal failure reporting;
- publishes neutral capacity snapshots using its own protocol; and
- keeps credentials, installations, repositories, source payloads and private
  routing inside the adapter boundary.

Daemon composition registers the adapter's stable namespace and operations in
the resolver and rejects duplicates before runtime starts. Adding a source must
not add concrete-source knowledge to core or runtime, move source-specific
persistence into neutral state backends, or create a competing lifecycle.
