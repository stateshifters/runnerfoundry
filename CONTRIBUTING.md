# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
discussions, or any other method with the owners of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Contributor terms

A contribution is any code, documentation, design, or other material intentionally submitted for inclusion in RunnerFoundry.
Material conspicuously marked "Not a Contribution" is excluded.

By submitting a contribution, you confirm that you created it or otherwise have authority to grant these rights. You grant the
RunnerFoundry copyright holder identified in [COPYRIGHT](COPYRIGHT), and its successors and assigns, a perpetual, worldwide,
non-exclusive, royalty-free, irrevocable, transferable, and sublicensable license to use, reproduce, modify, prepare derivative
works of, publicly display, publicly perform, distribute, sublicense, and relicense the contribution, in whole or in part, under
`AGPL-3.0-or-later` or other terms. You also grant a perpetual, worldwide, non-exclusive, royalty-free, irrevocable,
transferable, and sublicensable license under patent claims you control that are necessarily infringed by the contribution to
make, have made, use, offer to sell, sell, import, and otherwise transfer the contribution, alone or in combination with
RunnerFoundry as submitted.

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

- Write the test first.
  - If the behavior is user-facing, the test MUST be a snapshot test that captures the rendered user-visible output.
- Verify the new test fails (prove the gap).
- Then, and only then, implement the code change that makes the test pass.

For a behavior change, completion means the tests prove the production behavior. A valid failing test fails because production
code does not yet provide the required behavior, not merely because the requested edit has not been performed. Refactors,
documentation changes, internal removals, and mechanical cleanups do not require new tests solely to prove that the work occurred.

## Shared infrastructure risk

This CLI relies on shared infrastructure and credentials. A single bad actor can therefore degrade or remove service for
every user, and restoring service may require coordinated work. Treat any change that could amplify abuse, increase request
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

### Test isolation

All tests MUST be hermetic. Tests MUST NOT perform real filesystem, network, database, subprocess, application, or other
external I/O. Inject those boundaries and use fakes in tests. Build tags do not provide exceptions. Production Go files
MUST NOT use build tags.

### Available verification gates

V1 code MUST maintain 100% Go test coverage using hermetic tests.

```sh
make lint
make fmt-check
make vet
make test
make coverage
make race
```

- `make lint` runs the pinned `scc` aggregate-complexity check. Every non-generated Go file, including tests, must remain at
  or below 100; the command reports each violating path and value.
- Outside tests, build tags are limited to `ruleguard/rules-team.go`.
