# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
discussions, or any other method with the owners of this repository before making a change.

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

## Set up a contributor workspace

Clone the repository and work from its root. Install the Go version declared in [go.mod](go.mod), Git, and GNU Make.
Make uses Bash on Unix systems and `pwsh.exe` (PowerShell 7) on Windows. Ensure those commands are on your path.
Go downloads pinned module and tool dependencies when a command first needs them, so initial setup requires network access.
The race gate also requires a race-supported Go platform and a working C compiler; see [Go's race detector requirements](https://go.dev/doc/articles/race_detector#Requirements).

Start with `make test` for the ordinary hermetic suites. Use the [verification gates](#available-verification-gates) appropriate to the change.
`make fmt-check` checks Go formatting; `make vet` checks suspicious Go constructs; `make lint` checks lint rules and complexity;
`make coverage` checks statement coverage; and `make race` checks for data races.
These gates do not require a running Git source or execution provider. E2E preparation is separate and described under [E2E tests](#e2e-tests).

### Build and package

The maintained build target produces a Linux AMD64 executable:

```sh
make build
```

The output is `build/linux/amd64/runnerfoundry`. Building an artifact does not verify application behaviour.

To cross-compile the Apple Silicon executable from a contributor host:

```sh
make build-macos
```

The output is `build/darwin/arm64/runnerfoundry` and requires macOS 13 or later. No Intel macOS executable is packaged.

To produce a Debian package, source-location notice, third-party license notices, and SBOM metadata:

```sh
make package-deb VERSION=<version>
```

Replace `<version>` with the intended package version. Packages are written under `dist/`.
The packaging target uses pinned Go tools and downloads dependencies as needed.
Release automation selects the release version and invokes `make prepare`, which clears `dist/` and produces both the Debian
package and `dist/runnerfoundry_<version>_darwin_arm64.pkg`. The two packages include the same licence, source-location,
third-party notice and SBOM metadata. Individual packaging targets retain other files in `dist/`.
See [installation](docs/installation.md) for the operator workflow; do not execute a built or packaged daemon as contributor verification.

### macOS release signing on Linux

The release workflow downloads Quill 0.7.1, macospkg 0.7.3 and rcodesign 0.29.0 Linux AMD64 binaries.
It verifies their pinned SHA-256 checksums.
These tools run independently of the project's Go toolchain. A macOS host is not needed to build, sign, notarize or staple the installer.
The Linux release environment also needs OpenSSL, curl, tar, sha256sum, base64 and the standard GNU file utilities.

Release steps run only for `main` outside pull-request events. The GitHub Actions repository must supply these secrets:

| Secret | Value |
| --- | --- |
| `MACOS_APPLICATION_P12_BASE64` | Base64 Developer ID Application certificate and private key in a password-protected PKCS12 bundle |
| `MACOS_APPLICATION_P12_PASSWORD` | Password for that bundle |
| `MACOS_INSTALLER_P12_BASE64` | Base64 Developer ID Installer certificate and private key in a password-protected PKCS12 bundle |
| `MACOS_INSTALLER_P12_PASSWORD` | Password for that bundle |
| `MACOS_NOTARY_P8_BASE64` | Base64 App Store Connect team API private key |
| `MACOS_NOTARY_KEY_ID` | API key ID |
| `MACOS_NOTARY_ISSUER_ID` | API issuer ID |
| `MACOS_TEAM_ID` | Apple Developer team ID used to verify the signed installer |

The Release step creates private certificate and notarization-key files under `RUNNER_TEMP` and removes them on exit.
Application signing requires a verified chain to an Apple root; preparation fails if Quill cannot complete that chain.
Passwords remain in environment variables.
Do not enable shell tracing, print these values or put credential files in build artifacts or caches.

`make package-macos VERSION=<version>` runs in that Linux release environment after the tools and credentials are available.
Its tool-facing environment is `QUILL_SIGN_P12`, `QUILL_SIGN_PASSWORD`, `MACOS_INSTALLER_P12_PATH`, `MACOSPKG_P12_PASSWORD`,
`APPLE_KEY_ID`, `APPLE_ISSUER_ID`, `APPLE_PRIVATE_KEY_PATH` and `MACOS_TEAM_ID`; the workflow maps the repository secrets to them.
`QUILL`, `MACOSPKG` and `RCODESIGN` can override the tool command paths.
For a direct target invocation, `QUILL_SIGN_P12` must contain the private key and either only the leaf certificate or a full chain
including the Apple root. Quill 0.7.1 mishandles partial CA chains. The workflow accepts the stored application bundle and prepares
a suitable temporary bundle before invoking the target. Direct callers must supply a bundle already in one of these accepted forms.

The temporary JSON API key file used for notarization has private permissions and is removed automatically on success or failure.

Quill signs the executable. macospkg assembles and signs an ARM64-only distribution installer for macOS 13 or later.
rcodesign submits the signed installer to Apple, waits up to 30 minutes for acceptance, and staples the ticket.
See [rcodesign notarization and stapling](https://gregoryszorc.com/docs/apple-codesign/0.29.0/apple_codesign_rcodesign_notarizing.html) for submission status and log commands.
A final package check requires a Developer ID signature, the configured team, a stapled ticket and Apple's online notarization record.
Any failure stops release preparation before publication.
A notarization timeout does not establish rejection; check the submission in Apple's service before retrying.

The installer places the command in `/usr/local/bin`, the example configuration and metadata in `/usr/local/share/runnerfoundry`,
and a disabled system service in `/Library/LaunchDaemons`. It creates the `_runnerfoundry` service account and a starter
configuration only when none exists. Operators prepare credentials and provider access before enabling and starting the service.
See [macOS installation](docs/installation-macos.md) for the operator workflow.

Optional VM-image examples have their own [libvirt image preparation requirements](docs/providers/libvirt.md#prepare-guest-images)
and example-specific [Ubuntu](examples/libvirt-ubuntu-runner/README.md) and [Windows](examples/libvirt-windows-runner/README.md) build commands.
When changing those guest launchers, their hermetic checks run separately from image preparation, from the repository root:

```sh
bash examples/libvirt-ubuntu-runner/runnerfoundry-jit-launcher.test.sh
pwsh -NoProfile -File examples/libvirt-windows-runner/runnerfoundry-jit-launcher.Tests.ps1
```

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
