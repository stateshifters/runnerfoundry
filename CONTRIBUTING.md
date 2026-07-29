# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
discussions, or any other method with the owners of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

The [architecture overview](docs/architecture.md) describes the application's current system boundaries, runtime modes, and
interactions. Read it before changing behavior that crosses those boundaries.

## Quality bar (what good looks like)

This project optimizes for long-term maintainability and predictable behavior. Prefer small, boring, readable changes over clever
ones.

## Ways of working (contract)

This is how we contribute quality code.

### Test-first workflow (mandatory)

The expected behavior of production code MUST be expressed in proper automated tests first.

Automated software tests MUST exercise production code through a stable interface and verify durable observable behavior or a
genuine code invariant. They MUST NOT prove that work was performed. Do not commit tests that scan source files for words or
symbols, assert incidental file layout or implementation structure, or otherwise encode the mechanics of a requested edit.

Checks that confirm an edit was completed are work verification, not software tests. Run searches, diff inspection, linters, and
other appropriate commands as one-time verification evidence without adding them to the test suite.

- Write the test first.
  - If the behavior is user-facing, the test MUST be a snapshot test that captures the rendered user-visible output.
- Verify the new test fails (prove the gap).
- Then, and only then, implement the code change that makes the test pass.

For a behavior change, completion means the tests prove the production behavior. A valid failing test fails because production
code does not yet provide the required behavior, not merely because the requested edit has not been performed. Refactors,
documentation changes, internal removals, and mechanical cleanups do not require new tests solely to prove that the work occurred.

## Shared infrastructure risk

This CLI relies on shared infrastructure and credentials. That means a single bad actor can degrade or remove service for
every user, and recovering may require a coordinated release. Treat any change that could amplify abuse, increase request
volume, or relax safeguards as a safety boundary.

Do not add user-facing knobs or configuration that let users change behavior in ways that could increase external load or
reduce protections without explicit approval. If you believe an exception is needed, open an issue and get sign-off before
changing behavior.

### Code philosophy

- Simplicity first: implement the smallest change that satisfies the requirement.
- Apply KISS. Seek smallest changes satisfying criteria
- Treat YAGNI as an active constraint. Require justification for abstractions.
- Maintainability: prefer clear control flow and explicit dependencies over deep abstraction.
- Testability: structure code so important behavior can be validated by fast, deterministic tests.
- Consistency: follow existing patterns in this repo. Do not introduce a new pattern unless it replaces an old one and is agreed
  ahead of time.
- Prioritize correctness and testability over cleverness
- The project is cross-platform (first-class support for Windows, macOS, Linux) for both users and developers.

### Naming and file conventions

We prefer descriptive names that stay clear outside Go-specific idioms.

- Receiver names should be descriptive and derived from the type (for example, `client *Client`). Avoid single-letter receivers.
- Avoid single-letter identifiers except `t`, `err`, `cfg`, `cmd`, and `ctx` in narrow scopes.
- Go source filenames must be lowercase. Use `snake_case` for multiword names.


## Verification

PostgreSQL schema changes must also follow the authoritative [migration contribution guide](internal/cluster/migrations/CONTRIBUTING.md).

### Test suite boundaries

Unit tests MUST NOT perform filesystem, database, network, process, or other external I/O. Prefer injected dependencies,
fakes, and in-memory stores so the default test suite remains fast, deterministic, and isolated.

The `integration` build tag is the only build tag permitted for tests. It MAY be used only when real boundary behavior is
the subject of the test and replacing that boundary with an in-memory substitute would invalidate the behavior being
verified. Qualifying behavior includes database-engine semantics, durable close-and-reopen recovery, and the production
composition entry point. Slowness, complexity, cross-package composition, or inconvenient setup alone do not justify the
tag. Production Go files MUST NOT use build tags.

### PostgreSQL integration tests

Fast unit tests use in-memory fakes and do not provision PostgreSQL. Tests that require real migrations, roles, transactions, or advisory-lock behavior use the `integration` build tag and the pinned embedded PostgreSQL binary. Containers and a Dev Container are not required.

`make lint` also runs the pinned `scc` aggregate-complexity check. Every non-generated Go file, including tests, must remain at or below 100; the command reports each violating path and value. This is a backstop, not a substitute for cohesive functions, explicit boundaries, and behavior-focused tests.

The embedded binary is cached under the operating system's user cache directory at `runnerfoundry/embedded-postgres`, using Go's `os.UserCacheDir`. Set `RUNNERFOUNDRY_POSTGRES_CACHE` to use another absolute directory; CI uses `/cache/embedded-postgres`. A cold integration run populates this cache, while later runs reuse the pinned binary.

The cluster integration package starts one PostgreSQL server for the package and creates a separate database for each test. This keeps real PostgreSQL process, role, transaction, migration, and advisory-lock behavior while isolating test data without repeatedly restarting the server.

Run the fast suite during normal development:

```sh
make test
```

Run integration behavior explicitly before review:

```sh
make test-integration
```

`make coverage` and `make test-race` include the integration tag. `make smoke` builds the executable, exercises the clustered daemon lifecycle and migration command surfaces, then uses real PostgreSQL to verify multi-host capacity, leader failover, demand continuity, and reversible migration behavior.

### Complete verification suite

V1 code must maintain 100% Go test coverage.

```sh
make lint
make fmt-check
make vet
make test
make test-integration
make coverage
make test-race
make smoke
```

- The coverage total must report `100.0%`.
- Outside tests, build tags are limited to `ruleguard/rules-team.go`.
