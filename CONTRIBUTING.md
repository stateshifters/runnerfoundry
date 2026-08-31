# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
discussions, or any other method with the owners of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Contributor terms

A contribution is any code, documentation, design, or other material intentionally submitted for inclusion in RunnerFoundry.
Material conspicuously marked "Not a Contribution" is excluded.

By submitting a contribution, you confirm that you created it or otherwise have authority to grant these rights. You grant
Stateshifter Labs Limited, the RunnerFoundry copyright holder identified in [COPYRIGHT](COPYRIGHT), and its successors and
assigns, a perpetual, worldwide, non-exclusive, royalty-free, irrevocable, transferable, and sublicensable license to use,
reproduce, modify, prepare derivative works of, publicly display, publicly perform, distribute, sublicense, and relicense the
contribution, in whole or in part, under `AGPL-3.0-or-later` or other terms. You also grant a perpetual, worldwide, non-exclusive,
royalty-free, irrevocable, transferable, and sublicensable license under patent claims you control that are necessarily infringed
by the contribution to make, have made, use, offer to sell, sell, import, and otherwise transfer the contribution, alone or in
combination with RunnerFoundry as submitted.

You retain copyright ownership of your contribution. If it is accepted, it remains available as part of the public
`AGPL-3.0-or-later` edition. Submission does not create any entitlement to compensation, royalties, revenue sharing, ownership
of RunnerFoundry, employment, partnership, or governance authority. You are not required to maintain or support an accepted
contribution.

These terms are accepted by intentionally submitting a contribution. No separate signature or contributor license agreement
check is required. If an employer or another legal entity owns the contribution, you confirm that you are authorized to grant
these rights on its behalf. Do not submit a contribution if you do not agree to these terms or do not have the required
authority.

The [product contract](docs/PRD.md) is the sole authority for product behavior and terminology. Read it before changing job
ingestion, queueing, execution, result reporting, capacity publication, persistence, or deployment behavior. The
[architecture overview](docs/architecture.md) explains the boundaries that preserve that contract.

## Product contract authority

`docs/PRD.md` and `docs/architecture.md` are binding product requirements. They are not suggestions, proposals,
implementation notes, or descriptions that may be rewritten to match the current code. Any edit to either document requires
explicit user approval for that exact change, including edits presented as clarification, cleanup, reconciliation, or
refactoring. A general request to change code does not grant permission to change the product contract.

When code conflicts with these documents, the code is wrong. Change the implementation to satisfy the contract; do not weaken,
reinterpret, or remove the contract to accommodate the implementation. If the requested work cannot comply, stop and obtain
explicit user direction before changing either document or proceeding with a conflicting implementation.

The operator [README](README.md), examples, and other task documentation must conform to the product contract. Correct those
documents when they drift, but do not use them to introduce, override, or reinterpret product behavior.

Documentation and core code use the source-neutral terms **Git source**, **source adapter**, **job**, **Runner Pool**,
**runner**, and **result**. Source-specific terminology belongs only in its adapter, exact external API names, and explicitly
source-specific operator instructions. GitHub runner scale-set terminology must not enter core contracts.

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

Choose work verification by deliverable. Documentation changes use content, link, and documentation-specific checks.
Configuration changes use parser, schema, or tool-native static validation. Mechanical edits and removals use targeted searches,
diff inspection, and applicable static checks. Analysis and plans use evidence reconciliation and internal-consistency review.
Documentation, configuration, mechanical edits, removals, analysis, and plans MUST NOT use TDD or production software tests.

- Write the test first.
  - If the behavior is user-facing, the test MUST be a snapshot test that captures the rendered user-visible output.
- Verify the new test fails (prove the gap).
- Then, and only then, implement the code change that makes the test pass.

For a behavior change, completion means the tests prove the production behavior. A valid failing test fails because production
code does not yet provide the required behavior, not merely because the requested edit has not been performed. Refactors,
documentation changes, internal removals, and mechanical cleanups do not require new tests solely to prove that the work occurred.

#### Coverage follows purpose

V1 code MUST maintain 100% Go statement coverage using hermetic tests, as reported by `make coverage`. Always call this result
"statement coverage". It records only whether each instrumented statement was executed at least once.

Every maintained production path MUST serve a concrete product behavior, domain invariant, boundary guarantee, or failure
policy, and that purpose MUST be protected by an automated test through a stable interface.

Coverage-only tests are invalid. Uncovered code without a defensible purpose is a design problem, not a test gap. Passing the
statement-coverage gate does not establish test adequacy or meaningful assertions; those remain separate acceptance conditions.
Do not describe a change as fully or comprehensively tested solely because the statement-coverage gate reports 100%.

#### E2E tests

Ordinary tests prohibit external I/O. Tests tagged `e2e` are the sole exception: they may use isolated in-memory or
temporary-file SQLite databases and locally embedded PostgreSQL. E2E tests may start PostgreSQL subprocesses, use loopback
database connections, and create database files under test-owned temporary directories. They MUST NOT contact Git hosts,
execution providers, or other external services; inject scripted implementations at those boundaries.

The E2E suite is intentionally separate from the everyday verification gates. Prepare the pinned PostgreSQL 17.5 and 18.3
binaries before running it:

```sh
make prepare-e2e
make test-e2e
```

`prepare-e2e` is the only step that downloads PostgreSQL binaries. `test-e2e` requires the prepared binaries and fails rather
than downloading them. Prepared PostgreSQL files are stored under the ignored `build/e2e/postgres` directory by default. The
following variables support focused diagnosis:

```sh
make test-e2e E2E_BACKEND=sqlite
make prepare-e2e E2E_POSTGRES_VERSION=18
make test-e2e E2E_BACKEND=postgres E2E_POSTGRES_VERSION=18
```

Set `E2E_POSTGRES_ROOT` to use a different binary directory. PostgreSQL 17 uses its amd64 binary on Apple Silicon and therefore
requires Rosetta; PostgreSQL 18 is the native Apple Silicon default for focused local runs.

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
Any inspection or application of migrations through an installed RunnerFoundry executable described there is an operator procedure,
not contributor or CI verification.

### Test isolation

All ordinary tests MUST be hermetic. They MUST NOT perform real filesystem, network, database, subprocess, application, or
other external I/O. Inject those boundaries and use fakes in ordinary tests. The build-tagged E2E suite described above is the
only exception and its allowed I/O remains limited to isolated local databases, embedded PostgreSQL processes, and database
files in test-owned temporary directories. Production Go files MUST NOT use build tags.

### Available verification gates

RunnerFoundry behavior is proved by automated tests that exercise production code through the stable seams defined above. Contributor and CI verification MUST NOT execute a built RunnerFoundry executable, RunnerFoundry installed from a distribution package, a RunnerFoundry container, or a deployed RunnerFoundry instance to prove application behavior, startup, embedded build metadata, or toolchain output. The compiler, linker, packager, and deployment system are not RunnerFoundry software-test subjects.

Building or packaging may produce a requested artifact, but executing that artifact is not a verification gate. The gates listed below are exhaustive unless the product and contribution contracts are explicitly changed. This restriction does not prevent operators from running RunnerFoundry for its intended purpose.

```sh
make lint
make fmt-check
make vet
make test
make coverage
make race
```

`make test-e2e` is a separate, rare product-contract gate. It is not invoked by `make test`, `make coverage`, or `make race`.

- `make lint` runs the pinned `scc` aggregate-complexity check. Every non-generated Go file, including tests, must remain at
  or below 100; the command reports each violating path and value.
- Outside tests, build tags are limited to `ruleguard/rules-team.go`.
