# RunnerFoundry

RunnerFoundry turns job demand from a Git source into ephemeral self-hosted runners. It queues each job for a Runner Pool, gives the oldest matching job to that pool, and owns the ephemeral runner until provider cleanup completes. The provisioned runner reports the workflow result directly to the source.

GitHub Actions is the only supported Git source. RunnerFoundry is designed for personal GitHub accounts that need to share self-hosted capacity across repositories without organization-level runners. Docker Compose and libvirt are the supported execution providers.

RunnerFoundry integrates directly with GitHub Actions through the standalone [`github.com/actions/scaleset`](https://github.com/actions/scaleset) Go library. It does not run or integrate with Actions Runner Controller (ARC); ARC's Kubernetes controllers and behavior are not part of RunnerFoundry. See [GitHub runner scale-set mechanics](docs/references/github-scaleset-mechanics.md) for the upstream protocol RunnerFoundry consumes.

RunnerFoundry publishes supported pre-1.0 releases. PostgreSQL state advances through forward migrations. SQLite state may require recreation when release notes identify a schema change; recreation discards the existing durable state.

The normative behavior and terminology live in the [product contract](docs/PRD.md). The [architecture](docs/architecture.md) explains the source-neutral system boundaries, and the [GitHub adapter](docs/github-adapter.md) owns GitHub-specific protocol behavior.

[Quick start](#quick-start) | [Installation](#installation) | [Configuration](#configuration) | [PostgreSQL](#use-postgresql) | [Providers](#prepare-runner-capacity) | [Operations](#operations) | [Contributing](CONTRIBUTING.md) | [Licensing](LICENSING.md)

## How it works

1. GitHub requests jobs for capacity published by the GitHub adapter.
2. The adapter translates GitHub's exact requests and authoritative assigned-job count into internal jobs associated with a Runner Pool.
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
- a RunnerFoundry Debian package from [GitHub Releases](https://github.com/stateshifters/runnerfoundry/releases).

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

Build the Linux AMD64 executable with Go 1.26.5 and GNU Make:

```sh
make build
./build/linux/amd64/runnerfoundry --version
```

`make build` sets `GOOS=linux`, `GOARCH=amd64`, and `CGO_ENABLED=0`. A successful version command prints `runnerfoundry <version>`. Install that executable and create a dedicated account with access to the selected execution provider:

```sh
sudo install -m 0755 build/linux/amd64/runnerfoundry /usr/local/bin/runnerfoundry
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
| `prune` | Optional; default `false` | Remove owned GitHub runner scale sets only after no live cluster member declares their Runner Pool. |

Each `scale_sets` entry configures exactly one execution provider: `docker` or `libvirt`.

In a PostgreSQL cluster, each member publishes its configured Runner Pools and runner-group names to shared state. GitHub reconciliation uses the union declared by members that have heartbeated within the liveness cutoff; it does not treat the leader's local `scale_sets` as cluster-wide configuration. Members may therefore contribute different pools. A Runner Pool must use one runner-group name across the cluster. A disagreement preserves existing registrations and degrades reconciliation for that pool without affecting other pools.

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

`leave` preserves active runners and processing and never waits for those runners to finish. On process shutdown, RunnerFoundry allows up to 30 seconds for process-local cleanup before exiting successfully. Cleanup that does not finish within that window is left to operating-system resource release and startup reconciliation; published capacity expires through the normal member-liveness cutoff when it cannot be withdrawn first.

`terminate` destroys affected resources and returns only unstarted jobs to the queue after cleanup succeeds. Started jobs remain in processing until GitHub sends `JobCompleted`. This explicit policy is not subject to the 30-second `leave` shutdown window and can therefore take substantially longer.

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

CREATE ROLE runnerfoundry_login
  LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

GRANT runnerfoundry TO runnerfoundry_login;

CREATE DATABASE runnerfoundry OWNER runnerfoundry;
```

Configure authentication for `runnerfoundry_login` through the PostgreSQL mechanism used by the deployment. The primary and direct endpoints may use different login identities, but every identity must be a member of `runnerfoundry` so RunnerFoundry can execute `SET ROLE runnerfoundry`. Store login names and passwords in protected environment or file references rather than the YAML document.

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

Session pooling on the primary endpoint is recommended for the best job-pickup performance. In PostgreSQL deployments, each member attempts to reserve one primary pooled connection for `LISTEN`, while queue inserts publish best-effort `NOTIFY` wakes so members can compete for newly queued work immediately. While the listener is idle, RunnerFoundry periodically validates that same pooled session so infrastructure inactivity timeouts do not unnecessarily recycle it. Deployments where `LISTEN` or the session keepalive is unavailable remain supported; members discover the same durable work during the next maintenance cycle. Do not configure or use `state.postgres.direct` merely to enable queue notifications. The direct endpoint is reserved for operations that cannot work through the primary pool.

Every member in a cluster uses the same GitHub App and PostgreSQL database. Members may declare different Runner Pools and contribute different local providers and capacity. Members that declare the same Runner Pool must use the same GitHub runner-group name.

### Migrate PostgreSQL

Every daemon startup acquires the migration lock and applies all known forward migrations before opening the runtime connection pool. Use the migration commands to inspect or apply the same migrations explicitly during a controlled maintenance window:

```sh
runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
runnerfoundry migrate up --config /etc/runnerfoundry/config.yaml
```

For an upgrade, back up PostgreSQL, stop every member, install the new executable or package on every host, and run `migrate status` followed by `migrate up` from one host before restarting the members. Starting a member before the explicit `migrate up` applies the same forward migrations automatically.

For a rollback, back up PostgreSQL, stop every member, and wait more than 30 seconds for the last published heartbeat to expire. Inspect the current and known versions before selecting the rollback target:

```sh
runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
runnerfoundry migrate down --to <version> \
  --config /etc/runnerfoundry/config.yaml
```

After the rollback, install the older executable whose target schema matches `<version>`, run its `migrate status`, and only then restart the members. Restarting the newer executable would apply its forward migrations again. Rollback refuses to run while a leader lease is active or a member heartbeat is still live.

Operate PostgreSQL availability, backups, transport security, and database restoration separately from RunnerFoundry.

## Prepare runner capacity

Runner starts are bounded by five minutes. Provider validation, inventory refresh, inspection, and destruction are each bounded by 30 seconds. An earlier caller deadline is preserved. A start deadline is one failed provisioning attempt; after cleanup, the job follows the lifecycle's unlimited retry rule. The per-call deadlines do not add a retry count, backoff, or overall lifecycle timeout.

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

RunnerFoundry validates the service with `docker compose config --services` and supplies JIT configuration through `ACTIONS_RUNNER_INPUT_JITCONFIG`. Runner containers are launched with `--rm`, so Docker removes normally exited runners while RunnerFoundry force-removes resources that require exceptional cleanup.

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

`READY=1` means RunnerFoundry accepted its configuration, opened its required local and durable-state resources, validated its providers, and activated the runtime generation. It does not wait for Git source discovery, message sessions, inventory, reconciliation, recovery, or provisioning. While the first maintenance cycle is pending, `systemctl status` reports `ready` with unknown leadership. Maintenance then reports source-neutral runtime health as `operational` or `degraded`; cluster leadership is shown separately as `unknown`, `standby`, or `leader`. A rejected reload that restores or preserves the active generation does not make the service degraded.

Text and JSON logs use the same structured fields and domain terminology. The `msg` field describes the specific activity or failure and names its principal subject, such as `Reconciling GitHub repositories: 23/150`, `Recovered 12 assigned GitHub jobs for https://github.com/example/project in Runner Pool linux`, or `Failed to configure GitHub runner scale set for Runner Pool linux in https://github.com/example/project`. Queryable context is carried separately in `type`, `component`, `operation`, `stage`, and `outcome`. Resource-specific records also include every available identity needed to correlate the work.

At `info`, operators see generation and leadership transitions, the immediate maintenance cycle, bounded GitHub reconciliation and session checkpoints, changed aggregate Runner Pool capacity, durable job ingestion, claims, runner starts, and provisioning retries. Repository and session progress is emitted for the first item, each 5% boundary, and the final item, so normal output remains bounded as an installation grows. Failures are emitted at `error` by the subsystem that owns the failed operation. Recovery records include `failure_duration`.

Set the existing `log.level` to `debug` to add every scheduled maintenance cycle and serialized phase; the exact repository URL, Runner Pool, session, endpoint, source request, and external operation; per-repository capacity and poll outcomes; and the producing `source.file`, `source.line`, and `source.function`. Debug output does not include message payloads, request headers, provider commands, environment variables, domain XML, Compose content, or JIT input. GitHub boundary errors may include upstream diagnostics. Provider diagnostics are logged only after provider-specific protection. PostgreSQL and SQLite records identify the backend and state operation without including driver-supplied connection details.

Progress records use `current`, `total`, and `duration_ms` when applicable. Discovery completion includes `repository_count` and `installation_count`. Assigned-demand reconciliation includes `desired_jobs`, `available_jobs`, `acquired_jobs`, `running_jobs`, `registered_runners`, `busy_runners`, `idle_runners`, `active_jobs`, `queued_jobs`, `processing_jobs`, `jobs_added`, and `jobs_removed`. Other stable context includes `member`, `repository`, `installation_id`, `runner_pool`, `runner_group_id`, `scale_set_id`, `message_id`, `github_event_type`, `github_job_id`, `workflow_run_id`, `github_runner_id`, `source_request_id`, `runner_name`, `identity_reason`, `job_id`, `provider`, `endpoint`, `state_backend`, `state_operation`, `capacity`, and `count`. Protect three value classes when collecting or sharing logs: GitHub App private keys, PostgreSQL credentials or DSNs, and JIT configuration.

Example OpenSearch query-string filters (adjust the service field to match the collector):

```text
service.name:runnerfoundry AND type:progress_event AND outcome:started AND stage:*
service.name:runnerfoundry AND type:operational_event AND outcome:failed AND component:*
service.name:runnerfoundry AND repository:"https://github.com/example/project"
service.name:runnerfoundry AND operation:assigned_demand AND runner_pool:linux
```

### Reload configuration

```sh
sudo systemctl reload runnerfoundry
```

Reload validates and prepares the replacement configuration before applying it. The replacement becomes active and the service reports `READY` after it acquires the local work gate. Its first maintenance cycle starts immediately in the background. An external dependency or maintenance failure degrades the replacement but does not reject it or reactivate the previous generation. A configuration, preparation, provider-validation, durable-state, or local-activation failure rejects the replacement, rolls back prepared resources, and preserves or reactivates the previous generation. State-backend changes require a restart.

Runner reconciliation is provider-agnostic. Docker inventory endpoint identity is the member's Docker host; the configured Compose mechanism remains specific to provisioning. Libvirt endpoint identity uses the normalized connection URI. Each endpoint returns all observable RunnerFoundry resources with their Runner Pool, is queried once per maintenance cycle, and is then partitioned by pool. Duplicate configured endpoints are queried once. Inventory errors retain processing because absence was not established. Failed provisioning triggers a fresh inspection of the exact endpoint recorded with the claim.

GitHub discovery persists each successful registration independently. Each upsert atomically stores the active route and its ownership evidence; unchanged registrations cause no table-wide rewrite. When `prune` is enabled, RunnerFoundry deletes only scale-set IDs whose current ID, name, and runner group still match durable ownership. Similar labels or update settings do not establish ownership.

The source leader restores every committed registration immediately while discovery runs independently. Restoration does not filter registrations through the new leader's local Runner Pools, so a leadership change does not alter registration IDs or counts. Local configuration controls only the bootstrap routes and provider capacity available on that member. Successful repository and Runner Pool routes activate even when another route fails.

GitHub message sessions remain process-local because the pinned `github.com/actions/scaleset` library creates sessions but does not expose session adoption. Graceful leadership loss cancels polling and best-effort closes those sessions. After an ungraceful handoff, an individual restored route may receive `409 Conflict` until GitHub expires the former session lease; that route retries without blocking discovery, queue processing, leadership, or sibling routes. Initial session statistics restore assigned demand when its replacement session opens.

GitHub installation IDs are opaque, and records with different installation IDs may coexist while routes converge. Discovery through an accessible installation replaces the active record for each route it confirms. Inability to discover access is inconclusive: RunnerFoundry retains the exact record for retry and does not infer an installation remap. A definitive repository or session not-found through an old installation is terminal and deletes only the exact persisted fingerprint under a guard, so it cannot erase a newer replacement. Pruning remains separate cleanup and is the explicit gate for cluster-wide Runner Pool removal: with pruning disabled, absent declarations do not remove registrations; with pruning enabled, a pool is eligible only after no live member declares it. A pruning failure retains ownership and does not prevent newly reconciled routes from activating. Leadership loss is a hard fence: polling stops immediately, late results cannot be installed or acknowledged, and cleanup failure cannot resume the old term or block a new leader.

Durable queue processing is member-local and does not depend on source leadership. Every active member starts its queue processor with the runtime generation. Claims begin only after that member has refreshed inventory and reconciled the affected Runner Pool successfully. SQLite wakes the local pool after an enqueue is durable. PostgreSQL broadcasts a best-effort wake after commit so every listening member can enter the claim race; maintenance still discovers jobs when a notification is unavailable or missed.

For a removed Runner Pool or one changed between Docker and libvirt, `leave` applies the replacement without destroying its active runners. Processing recorded against a removed or unavailable endpoint is retained; invisibility is not absence proof. With `terminate`, RunnerFoundry uses the old provider after replacement activation commits to destroy affected resources, requeues cleaned-up unstarted jobs, and leaves started jobs awaiting `JobCompleted`. Cleanup failures retain processing without rejecting the otherwise-valid replacement.

On graceful shutdown, `leave` withdraws capacity and preserves all active runners and processing. `terminate` destroys owned runners, requeues unstarted jobs only after successful cleanup, and preserves started processing until GitHub terminalizes it.

### Upgrade

Review the release notes and back up the selected state backend before installing a new pre-1.0 release.

For PostgreSQL, follow the [migration procedure](#migrate-postgresql). Forward migrations preserve supported PostgreSQL state; do not recreate the PostgreSQL schema for an ordinary upgrade.

For SQLite, stop the member before installing the package. Reuse the existing database unless the release notes explicitly require recreation for that schema change. Recreation is destructive: it discards queued jobs, processing ownership, source tombstones, member capacity, and GitHub adapter registration and ownership state. Preserve a backup and accept or resolve that state loss before starting with a new database.

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
- [Architecture](docs/architecture.md): source-neutral component and adapter boundaries.
- [GitHub adapter](docs/github-adapter.md): GitHub protocol, discovery, session, and persistence behavior.
- [Contributing](CONTRIBUTING.md): development, verification, and contributor licensing terms.
- [Licensing](LICENSING.md): public, commercial, and source-availability terms.
- [Trademark policy](TRADEMARKS.md): permitted uses of the RunnerFoundry name and branding.
- [Releases](https://github.com/stateshifters/runnerfoundry/releases): packages and release notes.

## Support

Use [GitHub Issues](https://github.com/stateshifters/runnerfoundry/issues) for reproducible bugs and operator questions. Include the RunnerFoundry version, host operating system, deployment mode, provider, redacted configuration, expected result, observed result, and relevant non-secret logs.

## License

RunnerFoundry is free and open-source software licensed under
[AGPL-3.0-or-later](LICENSE). Personal, internal, and commercial use, including managed services, is
welcome. If you improve RunnerFoundry, contributing those improvements upstream helps maintain one strong implementation.
Contributions, attribution, and sponsorship are appreciated but not required.

See [Licensing](LICENSING.md) for source-availability obligations, commercial licensing, custom work, voluntary attribution,
and sponsorship.
