# RunnerFoundry

RunnerFoundry turns job demand from a Git source into ephemeral self-hosted runners. It queues each job for a Runner Pool, gives the oldest matching job to that pool, and owns the ephemeral runner until provider cleanup completes. The provisioned runner reports the workflow result directly to the source.

GitHub Actions is the only supported Git source. RunnerFoundry is designed for personal GitHub accounts that need to share self-hosted capacity across repositories without organization-level runners. Docker Compose and libvirt are the supported execution providers.

The normative behavior and terminology live in the [product contract](docs/PRD.md). The [architecture](docs/architecture.md) explains the system boundaries without defining another lifecycle.

[Quick start](#quick-start) | [Installation](#installation) | [Configuration](#configuration) | [PostgreSQL](#use-postgresql) | [Providers](#prepare-runner-capacity) | [Operations](#operations) | [Contributing](CONTRIBUTING.md) | [MIT license](LICENSE)

## How it works

1. GitHub requests a job for capacity published by the GitHub adapter.
2. The adapter creates an internal job associated with a Runner Pool.
3. The job remains in that Runner Pool's FIFO queue until a member picks it up.
4. Pickup atomically moves the oldest queued job into member-owned processing.
5. RunnerFoundry provisions one ephemeral runner. A failed provisioning attempt is cleaned up and moves the same job to the queue tail.
6. GitHub's `JobStarted` message durably ends provisioning retries for that job.
7. The runner reports the workflow result directly to GitHub.
8. Every `JobCompleted` message terminalizes the job, after which RunnerFoundry finishes provider cleanup and removes processing.

Runner registration and capacity publication are separate from this job flow. Before `JobStarted`, RunnerFoundry retries provisioning indefinitely from the queue tail after successful cleanup, without a retry count, delay, backoff, or timeout. After `JobStarted`, missing or failed runner resources never cause an internal retry. The daemon does not observe or persist normal workflow results and has no result-delivery worker.

Every repository gets its own GitHub runner scale set, but scale sets with the same Runner Pool name share one availability value. RunnerFoundry subtracts queued jobs and every processing job from the capacity contributed by live members. Moving a job from queued to processing does not change availability.

`available = max(0, live configured capacity - queued jobs - all processing jobs)`

GitHub does not expose a supported operation for reporting a provisioning failure for an individual job. RunnerFoundry therefore cleans up and retries the same job without inventing a GitHub result. If a future source supports such reporting, it remains inside that source adapter and does not create a persisted result lifecycle.

## Deployment choices

- **Standalone with SQLite:** one RunnerFoundry member and one SQLite database.
- **Standalone with PostgreSQL:** one RunnerFoundry member and one PostgreSQL database.
- **Clustered with PostgreSQL:** multiple RunnerFoundry members sharing PostgreSQL.

SQLite supports one member only. While RunnerFoundry is running, it holds the SQLite database in exclusive locking mode: a second daemon and external SQLite readers cannot access the live database until RunnerFoundry stops. PostgreSQL supports both standalone and clustered operation.

## Quick start

This example creates one standalone Docker-backed Runner Pool on a Debian or Ubuntu AMD64 host.

### Prerequisites

You need:

- Docker Engine and Docker Compose;
- permission to create and install a GitHub App;
- outbound HTTPS access to GitHub Actions; and
- a RunnerFoundry Debian package from [GitHub Releases](https://github.com/meza/RunnerFoundry/releases).

Self-hosted runners execute repository workflow code on your infrastructure. Review GitHub's [secure-use guidance](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners) before serving untrusted workflows.

### Install RunnerFoundry

```sh
sudo apt install ./runnerfoundry.deb
```

The package installs the executable and systemd unit and creates the `runnerfoundry` service account. It does not start the service before valid configuration and credentials exist.

### Configure the GitHub adapter

Create a GitHub App, grant repository **Administration: Read and write** permission, and install it on the repositories RunnerFoundry should serve. Record its numeric App ID and install its private key:

```sh
sudo install -m 0640 -o root -g runnerfoundry \
  github-app.pem /etc/runnerfoundry/github-app.pem
```

The GitHub adapter represents Runner Pools as repository-scoped GitHub runner scale sets. `scale_sets[].name` is therefore both the Runner Pool name inside RunnerFoundry and the `runs-on` value exposed by GitHub. Install the App on each personal-account repository that should share that Runner Pool.

### Prepare Docker capacity

Save this file as `/etc/runnerfoundry/compose.yaml`:

```yaml
services:
  runner:
    image: ghcr.io/actions/actions-runner:latest
    user: runner
    working_dir: /home/runner
    command: ["/home/runner/run.sh"]
```

Pin a reviewed image tag or digest for maintained environments and add the tools, volumes, networks, and security controls required by your workflows.

### Configure RunnerFoundry

Save this file as `/etc/runnerfoundry/config.yaml`, replacing the App ID:

```yaml
github:
  app:
    app_id: 123456
    private_key_file: /etc/runnerfoundry/github-app.pem
  discovery_interval: 5m

log:
  format: text
  level: info

state:
  path: /var/lib/runnerfoundry/state.db

prune: false

scale_sets:
  - name: linux-docker
    runner_group: default
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

Protect the configuration and confirm that the service account can read its inputs:

```sh
sudo chown root:runnerfoundry /etc/runnerfoundry/config.yaml
sudo chmod 0640 /etc/runnerfoundry/config.yaml
sudo -u runnerfoundry test -r /etc/runnerfoundry/config.yaml
sudo -u runnerfoundry test -r /etc/runnerfoundry/github-app.pem
```

### Start the service

```sh
sudo systemctl enable --now runnerfoundry
sudo systemctl status runnerfoundry
sudo journalctl -u runnerfoundry -f
```

Run a workflow using the configured Runner Pool name:

```yaml
jobs:
  verify:
    runs-on: linux-docker
    steps:
      - uses: actions/checkout@v7
      - run: uname -a
```

The expected result is one ephemeral runner for that job and a result reported to GitHub when the attempt finishes.

## Installation

### Debian package

The AMD64 package installs:

- `/usr/bin/runnerfoundry`;
- `/lib/systemd/system/runnerfoundry.service`;
- `/etc/runnerfoundry/config.yaml`;
- `/var/lib/runnerfoundry` for packaged SQLite state; and
- notices and an SBOM under `/usr/share/doc/runnerfoundry`.

Package upgrades and `apt remove` preserve packaged state. `apt purge runnerfoundry` removes the package-managed SQLite database and its sidecar files. Custom state paths remain operator-owned.

### Source-built executable

Install the executable and create a dedicated account with access to the selected execution provider:

```sh
sudo install -m 0755 runnerfoundry /usr/local/bin/runnerfoundry
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin runnerfoundry
sudo install -d -m 0750 -o root -g runnerfoundry /etc/runnerfoundry
sudo install -d -m 0750 -o runnerfoundry -g runnerfoundry /var/lib/runnerfoundry
```

Use the packaged systemd unit as a starting point and supply `--config <path>`.

## Configuration

RunnerFoundry reads one strict YAML document at startup and on explicit reload. Unknown fields and invalid combinations fail validation.

### GitHub adapter settings

These settings belong to the GitHub adapter. GitHub runner scale-set terminology does not define core queue or execution contracts.

| Setting | Requirement | Purpose |
| --- | --- | --- |
| `github.app.app_id` | Required positive integer | Numeric GitHub App ID. |
| `github.app.private_key_file` | Required | Path to the App private key. |
| `github.discovery_interval` | Optional; default `5m`, minimum `1m` | GitHub repository and capacity discovery interval. |
| `scale_sets[].name` | Required and unique | Runner Pool name and GitHub `runs-on` value. |
| `scale_sets[].runner_group` | Optional; default `default` | GitHub runner group used for the published capacity. |
| `scale_sets[].max_runners` | Required positive integer | Capacity contributed by this member. |
| `prune` | Optional; default `false` | Remove GitHub runner scale sets not present in this configuration. Enable only when RunnerFoundry owns them. |

Each `scale_sets` entry configures exactly one execution provider: `docker` or `libvirt`.

### State settings

Configure exactly one state backend:

| Setting | Requirement | Purpose |
| --- | --- | --- |
| `state.path` | Required for SQLite | SQLite database path. RunnerFoundry exclusively owns the live database until it stops. |
| `state.postgres` | Required for PostgreSQL | PostgreSQL connection and stable-role settings. |

One member using `state.postgres` is standalone. Multiple members sharing it form a cluster. Changing backends or PostgreSQL settings requires a restart.

Each member uses its trimmed operating-system hostname as its stable identity. Hostnames must be unique within a cluster, and only one RunnerFoundry daemon may run for a hostname. A member that has not heartbeated for 30 seconds contributes no advertised capacity. That cutoff does not requeue its work. The leader can recover an unstarted claim only after five minutes and only with cleanup or positive absence proof from the claim's exact inventory endpoint. Started jobs never return to the queue.

### Active runner policy

`--active-runner-policy` controls active resources during configuration reload and graceful shutdown. It is a daemon option, not a YAML setting, and accepts `leave` or `terminate`. The default is `leave`.

```sh
runnerfoundry --config /etc/runnerfoundry/config.yaml \
  --active-runner-policy=terminate
```

`leave` preserves active runners and processing. `terminate` destroys affected resources and returns only unstarted jobs to the queue after cleanup succeeds. Started jobs remain in processing until GitHub sends `JobCompleted`.

### Logging settings

| Setting | Default | Accepted values |
| --- | --- | --- |
| `log.format` | `text` | `text`, `json` |
| `log.level` | `info` | `debug`, `info`, `warn`, `error` |
| `log.file.path` | unset | Absolute path for file-only daemon logs |
| `log.file.max_size_mb` | `100` | Positive MiB limit |
| `log.file.max_backups` | `5` | Positive retained-file count |

When `log.file` is omitted, logs go to standard output. Do not configure multiple members to write to the same file.

## Use PostgreSQL

RunnerFoundry requires PostgreSQL 17 or later. Create a stable non-login role and a separate login identity. RunnerFoundry assumes the stable role on each connection and does not manage PostgreSQL roles or grants.

```sql
CREATE ROLE runnerfoundry
  NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

CREATE DATABASE runnerfoundry OWNER runnerfoundry;
```

Configure the backend:

```yaml
state:
  postgres:
    role: runnerfoundry
    host: postgresql.example.net
    port: "5432"
    name: runnerfoundry
    user: file:///run/secrets/runnerfoundry-postgres-user
    password: file:///run/secrets/runnerfoundry-postgres-password
    direct:
      host: postgresql-direct.example.net
      port: "5432"
```

Connection values may be literals, `env://VARIABLE` references, or absolute `file:///path` references. `dsn` may replace structured primary fields. Use `direct` for a session-affine endpoint when the primary endpoint cannot preserve sessions.

Every member in a cluster uses the same GitHub and PostgreSQL configuration. Members may contribute different local providers and capacity under the same Runner Pool name.

Inspect or apply migrations with the deployed executable:

```sh
runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
runnerfoundry migrate up --config /etc/runnerfoundry/config.yaml
```

For a rollback, stop every member before running:

```sh
runnerfoundry migrate down --to <version> \
  --config /etc/runnerfoundry/config.yaml
```

Operate PostgreSQL availability, backups, transport security, and database restoration separately from RunnerFoundry.

## Prepare runner capacity

### Prepare Docker Compose capacity

The operator owns the Compose service, image, command, tools, volumes, networks, and security policy. Configure a Docker-backed Runner Pool with:

```yaml
scale_sets:
  - name: linux-docker
    runner_group: default
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

RunnerFoundry validates the service with `docker compose config --services` and supplies JIT configuration through `ACTIONS_RUNNER_INPUT_JITCONFIG`.

See the [minimal Docker example](examples/docker-runner/README.md).

### Prepare libvirt capacity

The operator owns the guest image, domain XML, firmware, storage, networking, workflow tools, guest cleanup, and shutdown. Configure a libvirt-backed Runner Pool with:

```yaml
scale_sets:
  - name: linux-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/domain.xml
```

RunnerFoundry passes the JIT configuration in an SMBIOS type 11 value named `runnerfoundry.jit`. Keep that value out of logs and persistent guest state.

#### Prepare the guest image

The guest must:

1. read the `runnerfoundry.jit` value without logging it;
2. expose that value only to the runner process as `ACTIONS_RUNNER_INPUT_JITCONFIG`, inherited through the child environment and absent from command-line arguments;
3. start the unregistered GitHub Actions runner once;
4. remove transient runner and work data; and
5. shut down after the attempt.

Optional examples are available for [Ubuntu](examples/libvirt-ubuntu-runner/README.md) and [Windows Server](examples/libvirt-windows-runner/README.md).

## Operations

### Inspect the service

```sh
sudo systemctl status runnerfoundry
sudo journalctl -u runnerfoundry -f
```

`systemctl status` reports source-neutral runtime health as `operational` or `degraded`; cluster leadership is shown separately as `unknown`, `standby`, or `leader`. `READY=1` continues to mean that the active generation completed its first maintenance cycle. A rejected reload that restores or preserves the active generation does not make the service degraded.

Text and JSON logs use the same structured operational fields. The `operation` field identifies maintenance, reload, leadership, capacity publication, claims, runner starts or retries, and successful cleanup. Events include only non-secret identifiers such as `member`, `runner_pool`, and `job_id`, plus an `outcome` and `error_category`; raw error details are not logged. Repeated failures are logged immediately, whenever their fingerprint changes, and every five minutes while unchanged. The recovery event includes the recurrence count and failure duration.

### Reload configuration

```sh
sudo systemctl reload runnerfoundry
```

Reload validates and prepares the replacement configuration before applying it. The replacement acquires the local work gate and completes one successful maintenance cycle before it becomes active and the service reports `READY`. If activation fails, RunnerFoundry rolls back prepared logging and reactivates the previous generation. State-backend changes require a restart.

Runner reconciliation is provider-agnostic. Docker inventory endpoint identity includes the member hostname and Compose mechanism; libvirt endpoint identity uses the normalized connection URI. Each endpoint returns all observable RunnerFoundry resources with their Runner Pool, is queried once per maintenance cycle, and is then partitioned by pool. Duplicate configured endpoints are queried once. Inventory errors retain processing because absence was not established. Failed provisioning triggers a fresh inspection of the exact endpoint recorded with the claim.

GitHub discovery replaces its complete persisted registration snapshot in one transaction. When `prune` is enabled, RunnerFoundry deletes only scale-set IDs from its previous committed snapshot after the current ID, name, and runner group still match. Similar labels or update settings do not establish ownership.

For a removed Runner Pool or one changed between Docker and libvirt, `leave` applies the replacement without destroying its active runners. Processing recorded against a removed or unavailable endpoint is retained; invisibility is not absence proof. With `terminate`, RunnerFoundry uses the old provider after replacement activation commits to destroy affected resources, requeues cleaned-up unstarted jobs, and leaves started jobs awaiting `JobCompleted`. Cleanup failures retain processing without rejecting the otherwise-valid replacement.

On graceful shutdown, `leave` withdraws capacity and preserves all active runners and processing. `terminate` destroys owned runners, requeues unstarted jobs only after successful cleanup, and preserves started processing until GitHub terminalizes it.

### Upgrade

Review the release notes, stop the affected member or cluster, install the package on every affected host, and start the members again. This unreleased schema has no upgrade path: after a schema change, recreate the selected SQLite database or PostgreSQL schema before starting RunnerFoundry. Do not attempt to migrate or reuse state created by an earlier development revision.

### Troubleshooting

- **Service is degraded:** Filter the service log by `outcome=failed`, then use `operation` and `error_category` to identify the failing boundary. An `outcome=recovered` event clears that operation's active failure; `systemctl status` returns to `operational` after every active operation has recovered.
- **Configuration rejected:** Read the first validation error in the service log. Check unknown fields, App credentials, state-backend selection, positive capacity, and exactly one provider per Runner Pool.
- **GitHub capacity missing:** Verify App permissions, installation scope, the configured GitHub runner group, and the private-key file.
- **GitHub capacity is zero:** Confirm at least one member is heartbeating, then check whether queued jobs or any processing jobs already consume the Runner Pool's configured capacity. A state-read failure deliberately advertises zero rather than stale capacity.
- **Jobs remain queued:** Confirm that workflow `runs-on` matches `scale_sets[].name`, provider validation succeeds, and the Runner Pool has capacity.
- **Docker unavailable:** Run `docker compose -f <file> config --services` as the service account.
- **libvirt unavailable:** Run `virsh --connect <uri> list --all` as the service account and validate the domain XML and referenced storage.
- **PostgreSQL unavailable:** Verify login membership in the stable role, referenced-secret permissions, and session affinity for the direct endpoint.

## Security

Self-hosted runners execute workflow code on operator-controlled infrastructure. Ephemeral execution does not make untrusted code safe. Isolate runner networks and credentials according to repository trust and avoid privileged host access unless a workflow explicitly requires it.

Never commit or publish GitHub App keys, PostgreSQL credentials, JIT configuration, or sensitive logs.

## Documentation

- [Product contract](docs/PRD.md): normative behavior and terminology.
- [Architecture](docs/architecture.md): component and adapter boundaries.
- [Contributing](CONTRIBUTING.md): development and verification workflow.
- [Releases](https://github.com/meza/RunnerFoundry/releases): packages and release notes.

## Support

Use [GitHub Issues](https://github.com/meza/RunnerFoundry/issues) for reproducible bugs and operator questions. Include the RunnerFoundry version, host operating system, deployment mode, provider, redacted configuration, expected result, observed result, and relevant non-secret logs.

## License

RunnerFoundry is available under the [MIT License](LICENSE).
