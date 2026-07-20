# PostgreSQL clustering code-quality audit

This audit applies every criterion from [`code_review_symptoms.csv`](https://raw.githubusercontent.com/meza/agent-docs/refs/heads/main/review/code_review_symptoms.csv) to the PostgreSQL clustering recovery change. It was refreshed from the source catalogue on 2026-07-18. A pass identifies the primary evidence; not applicable explains why the criterion does not apply.

| Symptom | Result | Evidence |
| --- | --- | --- |
| SYM-001 | Pass | Command parsing, daemon lifecycle, clustered coordination, assignments, sessions, and PostgreSQL state have responsibility-specific files and names. |
| SYM-002 | Pass | Assignment states, repository scale-set sessions, node health, migration actions, and scheduling outcomes use domain terminology; generic manager/runtime/dependency names are absent. |
| SYM-003 | Pass | `AssignmentState`, `NodeHealth`, `migrationAction`, `scheduleOutcome`, `MigrationStatus`, and PostgreSQL setting types represent correctness-sensitive concepts. |
| SYM-004 | Pass | Assignment transitions live in `internal/cluster/assignments.go`; leadership in `leadership.go`; connection settings in `internal/postgresconfig`; schema SQL in migrations. |
| SYM-005 | Pass | CLI parsing, execution, migration orchestration, provider adapters, persistence, and daemon coordination are separated by abstraction level. |
| SYM-006 | Pass | Assignment transitions, leases, credential refresh, rollback procedure, compatibility, and migration rules are documented in code, README, ADR 4, and migration CONTRIBUTING. |
| SYM-007 | Pass | Comments explain reserved advisory locks, heartbeat/lifetime timing, credential refresh, retained demand, and cleanup trade-offs rather than narrating control flow. |
| SYM-008 | Pass | Production functions pass lint complexity limits; `scc` reports every file at aggregate complexity 100 or less. |
| SYM-009 | Pass | Responsibility files and lint function-length/cognitive-complexity gates keep functions cohesive and bounded. |
| SYM-010 | Pass | Names such as `Publish`, `MigrateDown`, `CompleteDemand`, `startSession`, `shutdown`, and structured action logs expose side effects. |
| SYM-011 | Pass | Typed errors and `scheduleOutcome` replace ambiguous boolean/nil signaling at scheduling and transition boundaries. |
| SYM-012 | Pass | Configuration and physical boundaries are explicit; leadership, worker, session, and assignment mutation is owned by daemon/store instances rather than package caches. |
| SYM-013 | Pass | PostgreSQL owns assignment transitions; the coordination loop owns leadership/reload routing; leader and session types own their cancellation and cleanup. |
| SYM-014 | Pass | Migration constraints and expected-state/owner updates prevent invalid assignment identifiers, metadata, states, and concurrent transitions. |
| SYM-015 | Pass | Config, migration CLI, demand, publication, messages, ports, references, DSN combinations, and repository URLs are rejected at their boundaries. |
| SYM-016 | Pass | Typed conflicts/no-work errors, safe PostgreSQL SQLSTATE errors, wrapped operation errors, stdout/stderr separation, and idempotent completion are tested. |
| SYM-017 | Pass | The assignment state machine, migration-before-publication order, leader/session lifetime, and explicit rollback sequence make temporal requirements visible. |
| SYM-018 | Pass | Tests use channels, injected retry waits, stable request-derived names, deterministic observation ordering, and isolated embedded PostgreSQL. |
| SYM-019 | Pass | Connections, rows, transactions, leases, message sessions, tickers, worker loops, and leader operations have explicit close/cancel/wait paths. |
| SYM-020 | Pass | `make lint` enforces local complexity and `scc` aggregate complexity without a separate gate or new suppression. |
| SYM-021 | Pass | PostgreSQL configuration resolution has one owner; primary/direct inheritance and compatibility predicates are not duplicated across config and store packages. |
| SYM-022 | Pass | Provider dispatch, credential source resolution, primary/direct endpoints, and standalone/clustered state variation are localized at explicit boundaries. |
| SYM-023 | Pass | Node health and scheduling progress use named types; remaining booleans represent intrinsic predicates such as compatibility or provider health. |
| SYM-024 | Pass | Roles, assignment states, health, migration actions, contributions, demand, status, and settings are structured domain values rather than loose primitives. |
| SYM-025 | Pass | `scaler_demand` is authoritative for scale-set/repository/request data; assignments store only execution ownership and transition metadata. |
| SYM-026 | Pass | Consumer-owned daemon interfaces separate migration, node publication, demand scheduling, assignment execution, leadership, GitHub, and provider boundaries. |
| SYM-027 | Pass | `internal/postgresconfig` owns PostgreSQL references/config construction; migration files own DDL; the store owns clustered state transitions. |
| SYM-028 | Pass | Interfaces exist at PostgreSQL, GitHub, provider, notifier, filesystem/config, and process boundaries rather than between arbitrary helpers. |
| SYM-029 | Pass | pgx, Tern, systemd, and GitHub SDKs are adapters around domain-led lifecycle and state-machine design. |
| SYM-030 | Pass | YAML, `PG*`, DSN, literal, `env://`, and `file://` inputs are explicit and validated; hostname identity is intentionally not configurable. |
| SYM-031 | Pass | Transactional demand completion, expected-state updates, advisory locks, single-flight reconciliation, retry-only confirmation, and race tests cover concurrency. |
| SYM-032 | Pass | Indexed FIFO/state queries, local capacity accounting, coalesced reconciliation, independent long-lived loops, and bounded deadlines make scale behavior deliberate. |
| SYM-033 | Pass | Repository/session failures are isolated; unhealthy/expired contributors affect only their capacity; migration failure prevents publication/readiness. |
| SYM-034 | Pass | Reversible sequential migrations, expand/backfill/contract guidance, compatibility floors, and N/N-1 rules support incremental evolution. |
| SYM-035 | Pass | Provider and credential-source variants are explicit; no speculative orchestration or plugin framework was introduced. |
| SYM-036 | Pass | Released migration history is append-only, responsibilities are file-local, and obsolete standalone/manager paths and test factories were deleted. |
| SYM-037 | Pass | Tests assert FIFO, capacity, transition, failover, retry, readiness, output, redaction, and recovery behavior rather than private call order. |
| SYM-038 | Pass | Local limits, aggregate capacity, assignment metadata/state, stable role, migration reversibility, leadership exclusivity, and retained demand are tested. |
| SYM-039 | Pass | Success, validation, partial failure, connection failure, stale transition, cancellation, rollback, recovery, and cleanup paths all have tests. |
| SYM-040 | Pass | Integration-tagged tests use real PostgreSQL for migrations, transactions, roles, credential rotation, advisory locks, FIFO, and failover. |
| SYM-041 | Pass | Tests avoid sleeps and ordered result queues; retry/cancellation behavior uses channels or injected waits. |
| SYM-042 | Pass | Behavior-focused test names, explicit setup, one primary action, and contextual failure messages make scenarios readable. |
| SYM-043 | Pass | Shared helpers cover credentials and embedded PostgreSQL; domain-condition fakes replace scripted call-order queues. |
| SYM-044 | Pass | Unit tests replace only physical PostgreSQL, GitHub, provider, notifier, process, or filesystem boundaries; owned domain logic remains real. |
| SYM-045 | Pass | Tests cover acquisition/database partial failure, ambiguous provider start, restart repair, leader loss, dynamic credentials, and empty database startup. |
| SYM-046 | Pass | Lint, format, vet, unit, PostgreSQL integration, coverage, race, and smoke verification are automated in Make and CI. |
| SYM-047 | Pass | Go naming, error wrapping, structured action/phase logging, test organization, and documentation terminology follow repository conventions. |
| SYM-048 | Pass | README, ADR 4, root CONTRIBUTING, migration CONTRIBUTING, comments, and this audit preserve operator and contributor intent. |
| SYM-049 | Pass | Existing structured logging reports action, phase, safe identifiers, status, version, direction, and duration where relevant. |
| SYM-050 | Pass | Errors retain operation and safe SQLSTATE classification while redacting connection configuration and server detail. |
| SYM-051 | Pass | Redaction tests forbid credentials, DSNs, roles, secret contents, and reference paths; logs omit SQL and JIT configuration. |
| SYM-052 | Pass | `READY=1` means migrated/published local readiness; systemd status separately reports standby, reconciling, operational, and degraded leadership. |
| SYM-053 | Pass | Migration, publication, leadership, reconciliation, acquisition, assignment, repair, start, completion, reload, and rollback paths emit diagnostics. |
| SYM-054 | Pass | Stable-role membership, session role override, trust boundaries, credential references, and application/non-application privilege responsibilities are explicit. |
| SYM-055 | Pass | The documented stable role is NOLOGIN and non-administrative, scoped to the scaler database/schema; transient logins receive membership only. |
| SYM-056 | Pass | No production credential is embedded; conventional environment/DSN and per-setting file references are supported. |
| SYM-057 | Pass | The database stores demand/coordination metadata, not credentials, DSNs, provider configuration, JIT secrets, or secret paths. |
| SYM-058 | Pass | Structured logs and migration status make leadership, migrations, assignment transitions, and rollback visible without exposing secrets. |
| SYM-059 | Pass | Invalid role/config/reference combinations fail closed; automatic startup only migrates upward; rollback requires an explicit stopped cluster. |
| SYM-060 | Pass | CLI status is stable plain text on stdout, help uses the supplied command output, and errors use stderr without color-only meaning. |
| SYM-061 | Pass | User-facing terms use neutral role, leader, contributor, node, assignment, and session language. |
| SYM-062 | Pass | Automated tests and the required verification commands back capacity, FIFO, migration, role, credential, failover, recovery, and compatibility claims. |
| SYM-063 | Pass | ADR 4 records atomicity, stable-role ownership, readiness, direct endpoint, migration, compatibility, and recovery assumptions/trade-offs. |
| SYM-064 | Pass | CLI, daemon, PostgreSQL, GitHub, provider, migration, and documentation boundaries express the same lifecycle and ownership model. |
| SYM-065 | Pass | Demand owns GitHub-facing request data; assignments own execution state; host config owns local provider limits; PostgreSQL owns cluster coordination. |
| SYM-066 | Pass | Confirmation-only retry, retained demand, stable runner names, state repair, contributor expiry, isolated sessions, and independent workers cover partial failure. |
| SYM-067 | Pass | Startup migration, `acquiring`/`starting` repair, failover, reload, SQLite reopen, explicit rollback, and retry behavior are documented and tested. |
| SYM-068 | Pass | Tern migrations are transactional, reversible, serialized, tested Up/Down, and keep all domain DDL outside Go startup code. |
| SYM-069 | Pass | Compatibility metadata, known migration range, startup compatibility validation, N/N-1 rules, and current-binary rollback address version skew. |
| SYM-070 | Pass | CI runs lint/complexity, format, vet, unit, PostgreSQL integration, coverage, race, and smoke using pinned Go/tool/database versions. |
| SYM-071 | Pass | Responsibility-oriented files, typed transitions, migration guide, explicit CLI, and constrained configuration make correct use easier to find. |
| SYM-072 | Pass | Changes to credentials, schema, leadership, scheduling, execution, or sessions each have a clear owner and focused regression tests. |
| SYM-073 | Pass | ADR 4 records the accepted schema-owner privilege and GitHub/PostgreSQL atomicity costs; irreversible migrations and speculative frameworks are rejected. |
| SYM-074 | Pass | Standalone and clustered modes share demand/recovery vocabulary while documenting their intentionally different coordination guarantees. |
| SYM-075 | Pass | Pure rules use unit tests; PostgreSQL semantics use integration tests; smoke covers only build and critical lifecycle surfaces. |
| SYM-076 | Pass | Unit tests use in-memory/domain fakes; physical files, processes, sockets, and clocks appear only where their semantics are the subject. |
| SYM-077 | Pass | Integration tests exercise real embedded PostgreSQL schema, transactions, role switching, credentials, concurrency, locks, and teardown. |
| SYM-078 | Not applicable | The scaler is a headless daemon with no browser/UI journey or deployed end-to-end harness in this change; critical system journeys are covered by lifecycle smoke and PostgreSQL integration tests. |
| SYM-079 | Pass | `make smoke` builds the binary and runs shallow daemon lifecycle, migration CLI, capacity, FIFO, failover, and reversible-migration checks. |
| SYM-080 | Pass | Tests use temporary SQLite/files/PostgreSQL runtimes, unique ports, fresh schemas, explicit setup, and cleanup. |
| SYM-081 | Pass | GitHub and providers are faked in ordinary tests; no test calls live third-party services; embedded PostgreSQL is pinned and isolated. |
| SYM-082 | Pass | Assertions include observed status, assignments, requests, nodes, capacities, output, SQLSTATE, versions, and backend IDs where useful. |
| SYM-083 | Pass | Refactored command, reload, leadership, scheduler, provider, session, and store tests each name and assert one primary behavior. |
| SYM-084 | Pass | PostgreSQL connectors, GitHub clients, provider commands, process runners, notifier, and retry timing are controlled at physical boundaries in unit tests. |
