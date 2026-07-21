# Github Scaler

`github-scaler` is a Go daemon for autoscaling ephemeral Github Actions runner scale sets.

It is using GitHub's new [Scaleset library](https://github.com/actions/scaleset)

The current version supports Docker Compose and libvirt providers. A standalone daemon stores waiting demand in SQLite. Multiple daemons can instead coordinate through PostgreSQL, with one elected leader managing Github scale sets and every host contributing its own provider capacity. The daemon does not store Github credentials, PostgreSQL credentials, or JIT secrets in either database.

:::notice
Libvirt support is for unix only. Windows hosts can only use Docker Compose.
:::

## Configuration

The daemon requires `--config <path>`.

```yaml
github:
  app:
    app_id: 123456
    private_key_file: /etc/github-scaler/github-app.pem
  discovery_interval: 5m

log:
  format: text
  level: info

state:
  path: /var/lib/github-scaler/state.db

prune: true

scale_sets:
  - name: linux-docker
    runner_group: default
    max_runners: 2
    docker:
      compose_file: ./compose.yaml
      service: runner

  - name: linux-vm
    runner_group: default
    max_runners: 1
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: ./domain-template.xml
```

For clustered operation, replace `state.path` with `state.postgres` on every host:

```yaml
state:
  postgres:
    role: github_scaler
    host: postgresql.internal
    port: "5432"
    name: github_scaler
    user: file:///run/secrets/github-scaler-postgres-user
    password: file:///run/secrets/github-scaler-postgres-password
    direct:
      host: postgresql-direct.internal
      port: "5432"
```

The daemon uses the operating-system hostname as the host's cluster identity. It is not configurable. `state.postgres.role` is required. Static or rotating login users must be members of this stable role; every database session assumes it before running migrations, state calls, or leadership coordination. Database objects and migration metadata remain owned by the role instead of a transient login identity.

PostgreSQL can be configured with `host`, `port`, `name`, `user`, and `password`. Each value, including `role`, may be a literal, an `env://VARIABLE` reference, or a `file:///path` reference. `file://` accepts native Unix and Windows absolute paths. For example, Vault Agent can manage the username and password as separate files, as shown above.

Alternatively, set `dsn` to a conventional PostgreSQL connection string or an `env://` or `file://` reference containing one. A DSN cannot be combined with structured fields. Omitted structured fields inherit pgx/PostgreSQL defaults, including `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and related environment variables. `state.postgres` may contain only the required role when all connection values come from standard PostgreSQL environment settings. The removed `dsn_env` and `dsn_file` keys are rejected.

`direct` is optional and uses the same DSN or structured fields and reference syntax. Structured direct fields inherit omitted primary fields, then standard PostgreSQL defaults. A direct DSN cannot be mixed with direct structured fields, and a primary DSN requires a complete `direct.dsn` when the direct endpoint differs. Migrations and advisory-lock sessions use this session-affine endpoint; ordinary state calls use the primary endpoint. Without `direct`, the primary endpoint must be PostgreSQL or a session-mode pooler. Transaction-mode pooling is suitable only for ordinary primary traffic when a direct endpoint is configured.

References are resolved whenever a new database connection is opened, and file contents are trimmed but never cached. Missing, unreadable, malformed, and empty references fail; fallback query values are not supported. Existing connections are not recycled when a file changes, so an active leader lease remains open until PostgreSQL or the scaler closes it. Credential values are opaque: the scaler does not inspect or assume an expiry, lease duration, or usage count. Connection errors and logs do not contain resolved values, generated connection strings, reference paths, or pgx connection details.

### PostgreSQL lifecycle

The stable role and its login memberships must exist before the scaler starts. Provision the role with `NOLOGIN`, `NOSUPERUSER`, `NOCREATEDB`, `NOCREATEROLE`, and `NOREPLICATION`, and limit its ownership and privileges to the scaler database or schema and its objects. Grant rotating login identities membership in that role; do not grant scaler objects directly to those identities. The application does not create databases, roles, memberships, grants, or role attributes.

Daemon startup automatically migrates an empty or outdated database upward before publishing capacity or reporting ready. Concurrent nodes serialize this work with a PostgreSQL advisory lock. Schema setup and teardown come exclusively from the embedded reversible migrations.

Inspect or apply migrations explicitly with:

```sh
github-scaler migrate status --config /etc/github-scaler/config.yaml
github-scaler migrate up --config /etc/github-scaler/config.yaml
```

`migrate status` is read-only. On an empty database it reports version zero without creating migration metadata. Migration commands load only `state.postgres`, so unrelated daemon configuration does not prevent database inspection or recovery.

Downgrade with the currently deployed binary, because an older binary does not contain migrations introduced by a newer release. Stop every scaler node, wait at least 30 seconds for its heartbeat to expire, then run:

```sh
github-scaler migrate down --to <version> --config /etc/github-scaler/config.yaml
```

Rollback refuses to run while a leader lease or live node heartbeat remains. Deploy the older binary only after the command succeeds.

Create a Github App with repository administration access for self-hosted runners, install it on the accounts the daemon should serve, and grant it either selected-repository or all-repository access. `github-scaler` discovers every installation owned by the App and every repository exposed by each installation. Github installation settings are the repository opt-in boundary.

Set `github.app.app_id` to the numeric App ID. Store the App private key outside the YAML and set `github.app.private_key_file` to its path. Plaintext private keys and legacy PAT fields are rejected. The discovery interval defaults to `5m` and cannot be shorter than `1m`.

Each scale-set definition is created in every discovered repository. In standalone mode, `max_runners` is one shared limit across all installations and repositories for that definition. In clustered mode, it is a per-host, per-scale-set contribution. The leader sums compatible contributions from live, healthy hosts. For example, `max_runners: 10` on Spock and `max_runners: 2` on Serenity produce an effective capacity of 12 for the shared scale-set name, while neither host can receive more than its own limit. Limits for different scale sets remain independent.

Cluster hosts may declare shared or disjoint scale sets. The leader manages their union, including scale sets absent from its own local configuration. Live, healthy hosts sharing a name must agree on Github-facing settings such as `runner_group`; conflicting live declarations contribute zero capacity and are logged as errors. Provider settings and `max_runners` remain host-local. A missed heartbeat or unhealthy provider removes that host from both effective capacity and compatibility checks, so its stale settings cannot disable compatible live contributors. Its declaration remains in the managed union until the host explicitly withdraws it. The scale set is removed only after the final declaration is explicitly withdrawn.

Jobs are admitted by Github queue time through one FIFO per scale set across repositories and hosts. The oldest observed job's repository receives the next available runner, while Github chooses the exact job assigned within that repository. PostgreSQL keeps clustered demand and assignments available through leader failover; SQLite keeps standalone FIFO ordering across restart and reload.

Exactly one of `state.path` and `state.postgres` is required. `state.path` should be absolute in production, and only one daemon may open that SQLite database. Changing the state backend or any `state.postgres` configuration requires a restart; replacing the contents of an existing referenced file does not change configuration and affects subsequent connections. Put SQLite on durable storage if a replacement standalone host must resume queued work. Back up or migrate SQLite only while the service is stopped.

`max_runners` must be greater than zero; omit a scale set instead of setting zero capacity.

Github runner scale sets are routed by their scale-set name. Use the configured `scale_sets[].name` value in workflow `runs-on`; do not configure runner matching labels through Docker Compose environment variables such as `LABELS`.

Set `prune: true` only when `github-scaler` owns every runner scale set in the discovered repositories and runner groups. When enabled, discovery deletes Github runner scale sets that are not present in the current config. Leave it unset or `false` if another controller or human-managed runner scale set shares the same repositories.

`log.format` accepts `text` or `json`. `log.level` accepts `debug`, `info`, `warn`, or `error` and defaults to `info`. Lifecycle milestones are logged at `info`. Set the level to `debug` to include individual repository, scale-set, polling, reconciliation, cleanup, reload, assignment, acquisition, repair, start, completion, migration, rollback, and leadership actions. Failures remain visible at `warn` or `error` as appropriate. Logs may include non-secret node, scale-set, repository, request, assignment, operation, and PostgreSQL SQLSTATE values. PostgreSQL migration command errors may also include the server's non-secret message, detail, hint, table name, and constraint name. They never include PostgreSQL connection settings, role names, secret-reference paths, credentials, JIT secrets, generated connection strings, SQL bodies, or raw pgx connection errors.

## Install as a Daemon

Install the Debian package on Ubuntu or Debian hosts:

```sh
sudo apt install ./github-scaler_1.0.0_amd64.deb
```

The package installs:

- `/usr/bin/github-scaler`
- `/lib/systemd/system/github-scaler.service`
- `/etc/github-scaler/config.yaml`
- `/var/lib/github-scaler` for durable queue state

Package installation creates the `github-scaler` service user and its state directory, then reloads systemd units. It does not enable or start the daemon because the config and credentials must be set first. Package upgrades, reinstalls, and `apt remove` preserve `/var/lib/github-scaler/state.db`. `apt purge github-scaler` removes the package-managed database and its sidecars. A custom `state.path` remains operator-managed and is not deleted by purge.

If the configured providers require host group access, add the service user to those groups before starting the daemon:

```sh
sudo usermod -aG docker github-scaler
sudo usermod -aG libvirt github-scaler
```

Edit `/etc/github-scaler/config.yaml`, place the App private key at the configured path, then enable the service:

```sh
sudo systemctl enable --now github-scaler
sudo systemctl status github-scaler
```

The daemon reads `/etc/github-scaler/config.yaml` at startup. It does not watch or continuously reread the file.

Systemd readiness covers local preflight only. The daemon sends `READY=1` after configuration, App credentials, and local settings validate and, in clustered mode, after database migration and contribution publication succeed. It does not claim that Github reconciliation has succeeded. The daemon reports its operational phase separately through systemd `STATUS=` values: `standby`, `leader reconciling`, `leader operational`, or `leader degraded`. Github discovery, scale-set registration, and message-session creation run immediately after readiness. A Github outage or repository-specific registration failure leaves the process ready, reports the leader as degraded, logs the failure, and retries on the configured discovery interval.

After editing the config or replacing the private-key file, reload the service:

```sh
sudo systemctl reload github-scaler
```

Reload validates the replacement configuration and credentials first. In standalone mode, the daemon then drains active runners, closes the old GitHub message sessions, unregisters the old runner scale sets, and reconciles the replacement configuration. In clustered mode, the host atomically replaces its published contributions; running work continues, and an active leader immediately reconciles the changed union without waiting for the next discovery interval or deleting scale sets that another host still declares. Existing runner containers or VMs keep running until their active jobs exit. Watch progress with:

```sh
journalctl -u github-scaler -f
```

Inspect logs with:

```sh
journalctl -u github-scaler -f
```

### Manual Install

For local builds, install the Linux binary somewhere on `PATH` for system services:

```sh
sudo install -m 0755 github-scaler /usr/local/bin/github-scaler
```

Create a dedicated service user and give it access to the provider sockets it needs. For Docker, that usually means the `docker` group. For libvirt, use the group required by your host policy, commonly `libvirt`.

```sh
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin github-scaler
sudo usermod -aG docker github-scaler
sudo usermod -aG libvirt github-scaler
```

Create the config directory and store the App private key outside the YAML:

```sh
sudo install -d -m 0750 -o root -g github-scaler /etc/github-scaler
sudo install -d -m 0750 -o github-scaler -g github-scaler /var/lib/github-scaler
sudo install -m 0640 -o root -g github-scaler config.yaml /etc/github-scaler/config.yaml
sudo install -m 0640 -o root -g github-scaler github-app.pem /etc/github-scaler/github-app.pem
```

Use absolute paths inside `/etc/github-scaler/config.yaml`, especially for the SQLite state database, Docker Compose files, and libvirt XML templates. The daemon has no required working directory.

Create `/etc/systemd/system/github-scaler.service`:

```ini
[Unit]
Description=GitHub runner scale-set scaler
After=network-online.target docker.service libvirtd.service
Wants=network-online.target

[Service]
Type=notify-reload
NotifyAccess=main
User=github-scaler
Group=github-scaler
ExecStart=/usr/local/bin/github-scaler --config /etc/github-scaler/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5s
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

If a host only uses Docker or only uses libvirt, remove the unused service from `After=` and omit the unused group membership.

Enable and start the daemon:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now github-scaler
sudo systemctl status github-scaler
```

Inspect logs with:

```sh
journalctl -u github-scaler -f
```

## Docker Provider

Docker runner configuration is Compose-owned. Put images, env files, volumes, labels, networks, command, user, workdir, and other Docker-native settings in the Compose service.

For each runner, `github-scaler` runs:

```text
docker compose -f <compose_file> run --detach --rm --name <runner> --label github-scaler.scale-set=<name> --label github-scaler.runner=<runner> --env ACTIONS_RUNNER_INPUT_JITCONFIG=<jit_config> <service>
```

The scaler injects the JIT config as `ACTIONS_RUNNER_INPUT_JITCONFIG`, which is the environment variable consumed by the Github Actions runner. The runner process should exit after its assigned job; `--rm` lets Docker remove the container.

Restart recovery uses Docker labels to discover owned containers. Running owned containers count as active capacity. Exited and removing containers are left to Docker's `--rm` lifecycle. Dead containers are removed during reconciliation, and explicit repository teardown force-removes running containers.

## Libvirt Provider

Libvirt runner configuration comes from a domain XML template. The template must describe an ephemeral VM image that starts a Github runner from the delivered JIT config and powers off after the runner exits.

The V1 implementation parses and renders XML with `libvirt.org/go/libvirtxml`, then applies it through `virsh` against `connection_uri`. The rendered domain gets:

- a unique domain name matching the runner name
- namespaced `github-scaler` ownership metadata for restart recovery
- `<os><smbios mode="sysinfo"/></os>`
- `<sysinfo type="smbios"><oemStrings><entry>github-scaler.jit=<encoded JIT></entry></oemStrings></sysinfo>`

The rendered runner XML stays in memory and is sent to `virsh create /dev/stdin`, so scaler-created VM runners are transient libvirt domains. The JIT config is visible to the guest through SMBIOS OEM strings and is not stored in libvirt metadata.

Restart recovery lists libvirt domains, filters to scaler-owned metadata, and treats running owned domains as active capacity. Shutoff transient domains disappear automatically. Forced cleanup uses `virsh destroy <domain>` for active owned domains only and never requests storage removal.

## Scale Behavior

For each scale-set definition, the daemon:

- discovers every App installation and accessible repository on the configured interval
- creates the Github runner scale set in each discovered repository if needed
- polls Github scale-set messages with `maxCapacity = max_runners`
- queues identifiable job-available demand by Github queue time across all installations
- enforces `max_runners` across every repository using that definition
- reconciles identity-free assigned capacity from Github statistics after durable FIFO demand
- generates JIT configs while shared capacity is available
- acknowledges Github messages only after local handling succeeds

In clustered mode, PostgreSQL elects one leader and stores the global FIFO, host contributions, and runner assignments. Only the leader creates and polls the union of Github scale sets. Each assignment follows `reserved -> acquiring -> ready -> starting -> running -> complete`. The leader records Github acquisition intent before contacting Github and confirms the acquisition before the selected host may claim it. Each live host claims assignments addressed to its operating-system hostname and starts them with that host's local provider configuration. Every non-complete assignment consumes the selected host's local capacity. Expired-host assignments complete without deleting their demand, making that demand eligible for reassignment. A replacement leader resumes scheduling from the same durable demand.

The daemon commits identifiable Github demand to the configured state database before acknowledging its message. In standalone mode, job-assigned and job-completed messages without positive request identities are not stored as queue entries; their assigned capacity comes from Github message statistics. Runner recovery combines the durable queue with provider-visible Docker labels/names and libvirt metadata. A duplicate job-available delivery updates the existing request rather than adding another queue entry.

### Recovery Guarantees

Positive-identity job demand is durable across restart and reload. For each available job, the daemon first stores the request in the configured state database. The queue entry remains until Github reports `JobStarted` for that request or terminal `JobCompleted` state. Starting or observing a runner is not proof that Github assigned the job successfully.

#### Standalone recovery

At standalone startup, the daemon reopens `state.path`, rebuilds repository workers from Github discovery, and schedules the persisted queue. A provider-visible named runner suppresses duplicate provisioning without completing its matching queue entry. If that runner disappears before `JobStarted`, the daemon provisions it again without reacquiring the request. Missing repositories retain their demand until discovery can register their workers. FIFO order and the standalone `max_runners` limit continue to apply.

Standalone reload drains active runners, closes old scale-set sessions, reuses the SQLite store for configured scale sets, registers replacement workers, and schedules retained demand. Demand for repositories or scale sets removed by the replacement configuration is intentionally deleted. Identity-free assigned work is not stored as queue demand; Github message-session statistics restore that assigned capacity after restart or reload.

#### Clustered recovery

The leader and host executors resume from PostgreSQL demand and assignment state. A database failure after Github confirms acquisition retries only the `acquiring -> ready` database transition in the same process; it does not reacquire the request. After a process failure leaves an assignment in `acquiring`, a replacement leader may retry acquisition, but it requires Github's response to contain the requested ID before making the assignment ready. An unconfirmed response starts no identity-bound runner and leaves the demand retained.

Before starting an assignment in `starting`, the host checks for its stable request-derived runner name. An existing runner is observed rather than started again. After an ambiguous provider error, the host checks again before retrying. A process restart repairs `starting` assignments before claiming new work, and failures remain in `starting` rather than returning to `ready`. Github `JobStarted` and terminal `JobCompleted` messages complete demand and its active assignment together.

Clustered reload replaces the host's contributions and immediately reconciles them when that host is leader. Missing repositories retain demand until discovery can recreate their sessions. Expired hosts lose effective capacity, and their active assignments complete while their demand remains available to another compatible contributor.

Github's scale-set client does not formally specify whether acquiring the same request again after a successful acquisition is idempotent. The requested-ID check is therefore the recovery boundary after an `acquiring` process failure. Public release remains blocked until the upstream reacquisition contract is confirmed or recovery no longer depends on it.

When installation access is removed, the daemon immediately drops queued demand, destroys active local runners, closes the message sessions, and attempts to unregister the affected scale sets. This can interrupt active jobs. On normal shutdown, active jobs are left running while message sessions close and scale sets unregister.

## Migrating from PAT Authentication

PAT configuration is not supported. Replace `github.token_env` or `github.token_file` with `github.app.app_id` and `github.app.private_key_file`, remove every `scale_sets[].repositories` list, and install the App on the repositories the daemon should manage. Capacity changes from per-repository to one shared `max_runners` limit per scale-set definition.

## Development & Contributing

Please follow the [contributing guidelines](CONTRIBUTING.md) when contributing to this project. We welcome contributions from the community, whether it's bug fixes, new features, or improvements to existing code.
