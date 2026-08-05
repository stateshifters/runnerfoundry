# RunnerFoundry product contract

This document is the normative product contract for RunnerFoundry. Other documentation may explain how to install, configure, operate, or contribute to the project, but it must not define a different job lifecycle.

## Purpose

RunnerFoundry accepts job demand from a supported Git source, queues that demand for the requested Runner Pool, attempts to provide a runner, reports the result to the source, and removes the completed internal job.

GitHub Actions is the only supported Git source today. Support for another source must be implemented behind a source adapter without changing the core contract.

Result reporting is a product outcome. The provisioned runner reports the workflow result directly to the Git source. RunnerFoundry reports a runner-provisioning failure only when the source exposes a supported operation for doing so.

## Terms

- **Git source**: The external system that requests a job and receives its result.
- **Source adapter**: The boundary that translates source-specific requests, results, capacity, and terminology.
- **Job**: One internal unit of demand created from one source request.
- **Runner Pool**: The source-neutral capacity group requested by a job.
- **Runner**: One provisioned execution environment that attempts one job.
- **Result**: Success or failure reported to the source through its adapter. Job failures and runner or execution failures are failures.

Source-specific payloads and names are not core concepts. In particular, a GitHub runner scale set is represented inside RunnerFoundry as a Runner Pool. The GitHub term remains inside the GitHub adapter and GitHub-specific operator configuration.

## Job lifecycle

```mermaid
flowchart LR
    Source["Git source requests a job"] --> Adapter["Source adapter creates a job"]
    Adapter --> Queue["Queue job for its Runner Pool"]
    Queue --> Work["Runner Pool requests work"]
    Work --> Execute["Attempt the oldest matching job"]
    Execute --> Report["Report success or failure through the adapter"]
    Report --> Remove["Remove the internal job"]
```

The lifecycle has these rules:

1. One source request creates one internal job associated with one Runner Pool.
2. Jobs are FIFO within each Runner Pool.
3. When a Runner Pool asks for work, it receives the oldest queued job for that pool.
4. RunnerFoundry attempts to provide a runner and execute the job.
5. Every terminal outcome is success or failure. A runner or execution failure is a failed job outcome, not a separate queue.
6. RunnerFoundry reports the result through the source adapter before removing the internal job.
7. A removed job is final. RunnerFoundry does not requeue it or invent replacement demand.
8. Another attempt begins only when the Git source submits another job request.

## Cleanup

Cleanup applies the same terminal path to a queued job that cannot be served: report failure through the source adapter, then remove the internal job. Cleanup does not create another kind of demand or another job lifecycle.

## Runner Pool capacity

Publishing capacity is separate from processing jobs:

1. The runner service registers or updates Runner Pool capacity.
2. The source adapter publishes that capacity using the source's model.
3. The source can then request jobs for that capacity.
4. Heartbeats, capacity updates, and deregistration maintain availability without changing queued-job ordering or outcomes.

For GitHub, the adapter publishes Runner Pool capacity as GitHub runner scale sets. This mapping is owned entirely by the GitHub adapter.

## Deployment modes

Deployment mode is determined by the number of RunnerFoundry members, not solely by the database:

- **Standalone**: One RunnerFoundry member using SQLite or PostgreSQL.
- **Clustered**: Multiple RunnerFoundry members sharing PostgreSQL.

SQLite is restricted to one member. PostgreSQL supports either one member or multiple members.

## Documentation authority

- This document defines product behavior and terminology.
- [Architecture](architecture.md) explains the boundaries that preserve this contract.
- The root [README](../README.md) provides operator tasks.
- [Contributing](../CONTRIBUTING.md) defines the development workflow.

When another document conflicts with this contract, this contract wins and the other document must be corrected.
