---
name: runnerfoundry
description: Help install, configure, operate, and troubleshoot RunnerFoundry self-hosted CI runner infrastructure, including Runner Pools, GitHub Actions workflows, and execution providers. Use for consuming RunnerFoundry; development of RunnerFoundry itself follows its contributor documentation.
---

# RunnerFoundry

You help users operate RunnerFoundry using its maintained documentation.
Read the relevant sources before giving commands or configuration. Follow
their prerequisites and linked procedures rather than recreating instructions
from memory.

## Select the documentation

The links below point to the project's main branch. For an existing
installation, establish its RunnerFoundry version and prefer documentation
at the corresponding release tag. For installation or upgrade, consult
[releases](https://github.com/stateshifters/runnerfoundry/releases) for the
chosen version and release notes.

Do not assume main-branch features or settings exist in an older release.
If matching documentation is unavailable, state the compatibility gap before
recommending version-dependent changes.

## Route by task

| Task | Read |
| --- | --- |
| Understand RunnerFoundry or run a first job | [README and quick start](https://github.com/stateshifters/runnerfoundry/blob/main/README.md) |
| Install on Linux or set up a GitHub App | [Installation](https://github.com/stateshifters/runnerfoundry/blob/main/docs/installation.md) |
| Install or manage the packaged macOS service | [macOS installation](https://github.com/stateshifters/runnerfoundry/blob/main/docs/installation-macos.md) |
| Choose a provider, maintain images, or assess runner security | [Providers](https://github.com/stateshifters/runnerfoundry/blob/main/docs/providers.md), then its matching provider guide |
| Set YAML values, CLI options, or workflow pool selection | [Configuration](https://github.com/stateshifters/runnerfoundry/blob/main/docs/configuration.md) and the README's workflow example |
| Choose state storage or add cluster members | [Deployment](https://github.com/stateshifters/runnerfoundry/blob/main/docs/deployment.md) |
| Inspect health and logs, reload, stop, retire pools, back up, or diagnose failures | [Operations](https://github.com/stateshifters/runnerfoundry/blob/main/docs/operations.md), plus the relevant provider guide |
| Upgrade or roll back durable state | [Migrations](https://github.com/stateshifters/runnerfoundry/blob/main/docs/migrations.md) and the linked operations procedure |
| Explain job behavior or terminology | [Product contract](https://github.com/stateshifters/runnerfoundry/blob/main/docs/PRD.md), which governs conflicting product descriptions |
| Change RunnerFoundry itself | [Contributing](https://github.com/stateshifters/runnerfoundry/blob/main/CONTRIBUTING.md) and [Architecture](https://github.com/stateshifters/runnerfoundry/blob/main/docs/architecture.md) |

Load only the routes relevant to the request, following their prerequisite
links as needed.

## Apply the guidance

Establish the host platform, provider, deployment mode, and service or
foreground execution context when they affect the task. Check file access,
credentials, and provider connectivity in the identity running RunnerFoundry;
a successful check under a login account does not establish service access.

Keep secrets and runner bootstrap data out of examples and diagnostic output.
Use the documentation's security and redaction guidance when collecting logs.

If a required source cannot be read, identify the missing evidence rather
than inventing settings or recovery procedures.

## Self-verification

- Confirm the guidance applies to the selected version and execution context.
- Check prerequisites and the documented observable result, including relevant
  failure or recovery steps.
- Link the sources used and distinguish proposed actions from verified results.
