# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
discussions, or any other method with the owners of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Quality bar (what good looks like)

This project optimizes for long-term maintainability and predictable behavior. Prefer small, boring, readable changes over clever
ones.

## Ways of working (contract)

This is how we contribute quality code.

### Test-first workflow (mandatory)

The expectation of the system MUST be expressed in proper automated tests first.

- Write the test first.
  - If the behavior is user-facing, the test MUST be a snapshot test that captures the rendered user-visible output.
- Verify the new test fails (prove the gap).
- Then, and only then, implement the code change that makes the test pass.

Completion means the tests prove it. If you cannot write a test for the expectation, stop and resolve that before proceeding.

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

V1 code must maintain 100% Go test coverage.

```sh
make lint
make fmt-check
make vet
make test
make coverage
make test-race
make smoke
```

- The coverage total must report `100.0%`.
- The code must not use build tags with the only exception being the `ruleguard/rules-team.go` file.
