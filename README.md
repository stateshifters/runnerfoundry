# RunnerFoundry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/runnerfoundry-lockup-horizontal-cyan-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/runnerfoundry-lockup-horizontal-cyan-light.png">
  <img src="docs/images/runnerfoundry-lockup-horizontal-cyan-light.png" alt="RunnerFoundry" width="380" height="96">
</picture>

Your infrastructure. Dynamic, shared runner capacity for your personal repositories.

RunnerFoundry is a shared control plane for self-hosted ephemeral CI runners.
Pool capacity across repositories and hosts on infrastructure you manage. Define a Runner Pool once,
and let RunnerFoundry create disposable runners as work arrives and clean them up afterwards.

## Who RunnerFoundry is for

Use RunnerFoundry when you want several repositories to share self-hosted capacity and can maintain the machines and runner images.
Start with one host and SQLite. Several hosts can share PostgreSQL and contribute different pools or capacity to the same pool.
If hosted runners meet your needs, you do not need to operate your own runner infrastructure.

Self-hosted runners execute repository code on your machines. Start with a trusted repository and read the
[security guidance](docs/providers.md#security-and-diagnostic-logs) before accepting untrusted work.

## How it works

A Runner Pool is a named group of capacity. Jobs request a pool, and RunnerFoundry creates runners as work arrives.
Each runner executes one job and reports its result directly to the Git source.
RunnerFoundry cleans up the runner resource afterwards.

Provisioning failures are reported when the Git source supports it. Otherwise, RunnerFoundry retries after cleanup
until a runner accepts the job or the Git source ends the demand.
An execution failure after acceptance is a workflow result, not a provisioning retry.

## Current integrations

GitHub Actions is the first supported Git source and is currently the only available source adapter.
The control plane separates Git source integrations from the shared runner lifecycle so additional sources can be added.
Docker Compose and libvirt virtual machines are the supported execution providers.

With the GitHub integration, your workflow selects a Runner Pool with `runs-on: linux-docker`.
The quick start below uses that integration to share capacity across personal-account repositories.

## Quick start

This path uses an Ubuntu 24.04 AMD64 host, a Debian package, SQLite, and Docker Compose.
For Apple Silicon Macs, follow [macOS installation](docs/installation-macos.md).

### 1. Prepare your host and GitHub App

Follow [installation and GitHub App setup](docs/installation.md). Finish with:

- Docker Engine and Compose accessible to the `runnerfoundry` service account
- RunnerFoundry installed and `runnerfoundry --version` working
- your numeric GitHub App ID and a readable private key at `/etc/runnerfoundry/github-app.pem`

### 2. Configure a runner and its pool

Create `/etc/runnerfoundry/compose.yaml`:

```yaml
services:
  runner:
    image: ghcr.io/actions/actions-runner:latest
    user: runner
    working_dir: /home/runner
    command: ["/home/runner/run.sh"]
```

Replace `/etc/runnerfoundry/config.yaml` with the following, substituting your App ID for `123456`:

```yaml
github:
  apps:
    - app_id: 123456
      private_key_file: /etc/runnerfoundry/github-app.pem
state:
  path: /var/lib/runnerfoundry/state.db
scale_sets:
  - name: linux-docker
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

This allows two concurrent runners. Ensure both files are readable by the service account:

```sh
sudo chown root:runnerfoundry /etc/runnerfoundry/config.yaml /etc/runnerfoundry/compose.yaml
sudo chmod 0640 /etc/runnerfoundry/config.yaml /etc/runnerfoundry/compose.yaml
sudo -u runnerfoundry docker compose -f /etc/runnerfoundry/compose.yaml config --services
```

The last command must list `runner`. Correct file or Docker access errors before starting the service.
The image is a minimal evaluation environment; it does not supply every tool your workflows may need.
Before maintaining this setup, [pin and update your runner image](docs/providers.md#update-runner-images).
Automatic runner software updates are disabled by default, so keeping that image current is your responsibility.

### 3. Start RunnerFoundry

```sh
sudo systemctl enable --now runnerfoundry
sudo systemctl status runnerfoundry
sudo journalctl -u runnerfoundry -f
```

Look for discovery and runner-pool registration progress. A running process alone does not prove GitHub is ready to send work.
If startup or registration fails, use [troubleshooting](docs/operations.md#troubleshooting).

### 4. Run your first job

In a repository where you installed the App, create `.github/workflows/runnerfoundry-test.yml` on its default branch:

```yaml
name: RunnerFoundry test
on:
  workflow_dispatch:
jobs:
  verify:
    runs-on: linux-docker
    steps:
      - run: uname -a
```

Open the repository's Actions tab, select this workflow, and choose Run workflow.
Success means the job completes in GitHub, its logs show Linux system information, and the one-job container is removed afterwards.
With no other workflow jobs running, check the host after cleanup:

```sh
sudo -u runnerfoundry docker ps --all
```

The container created for your test job should no longer be listed. Unrelated containers can remain.
If the test container remains, inspect [provider and cleanup failures](docs/operations.md#troubleshooting) in the logs.
If the job stays queued, follow [queued-job diagnosis](docs/operations.md#jobs-stay-queued).

Install the same App on another personal repository to let it use `runs-on: linux-docker` too.
New repositories are discovered on the [configured interval](docs/configuration.md#github-configuration).

## Standalone deployment

One member can use SQLite or PostgreSQL. See [deployment choices](docs/deployment.md#standalone-deployment).

## Clustered deployment

Several members share PostgreSQL. See [add cluster members](docs/deployment.md#clustered-deployment).

## Runner providers

See [Docker and libvirt contracts, examples, and image maintenance](docs/providers.md).

## Configuration reference

See [all YAML settings and command-line options](docs/configuration.md).
The YAML key `scale_sets` configures Runner Pools; use the exact key shown in examples.

## Operations

See [health, logs, reloads, backups, upgrades, and troubleshooting](docs/operations.md).
Schema-specific upgrade and rollback constraints are in [migrations](docs/migrations.md).

## Contribute and understand the implementation

For contributor and maintainer tasks:

- [Contributing](CONTRIBUTING.md): tools, builds, checks, and submission requirements
- [Product contract](docs/PRD.md): lifecycle and terminology
- [Architecture](docs/architecture.md): responsibilities and dependencies
- [GitHub adapter](docs/github-adapter.md): integration contracts
- [GitHub runner scale-set mechanics](docs/references/github-scaleset-mechanics.md): upstream protocol

## Support

Use [GitHub Issues](https://github.com/stateshifters/runnerfoundry/issues) for bugs and operator questions.
Follow the [support checklist](docs/operations.md#request-support), including `runnerfoundry --version` and redacted logs.

## License

RunnerFoundry is licensed under [AGPL-3.0-or-later](LICENSE). Personal, internal, and commercial use is welcome, including managed services.
See [licensing](LICENSING.md) for obligations and commercial enquiries, and [trademarks](TRADEMARKS.md) for name and branding use.
