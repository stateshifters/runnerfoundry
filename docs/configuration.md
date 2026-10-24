# Configuration reference

Use this reference to choose values for an installed RunnerFoundry member. For a first setup, follow the
[quick start](../README.md#quick-start); for shared state, follow [deployment](deployment.md).

RunnerFoundry reads YAML from `--config`. The packaged service supplies `/etc/runnerfoundry/config.yaml`.
Unknown fields and invalid values reject startup or a replacement configuration. YAML keys are case-sensitive.
Use absolute paths so file locations do not depend on the daemon's working directory.

## Apply configuration changes

Edit the file, then [reload and check the result](operations.md#reload-configuration).
A rejected replacement leaves the existing configuration in service when preparation fails.
Every change to `state`, including paths, hosts, credentials, role, and direct endpoint settings, requires a restart.
Restart after changing PostgreSQL credential-file contents too, to apply credentials consistently across connections.
Changing a backend or database path does not transfer existing state; see [migration constraints](migrations.md).

Changes to runner images require the [image update procedure](providers.md#update-runner-images).
Changing a configuration file does not replace running images by itself.

## Top-level settings

These are the supported top-level keys:

| Key | Type | Default and requirement |
| --- | --- | --- |
| `github` | mapping | Required; configure at least one App |
| `state` | mapping | Required; choose exactly one backend |
| `scale_sets` | list | Required; at least one Runner Pool |
| `log` | mapping | Optional; text at info level on standard output |
| `autoUpdateRunners` | boolean | `false`; maintain runner software in your images |
| `prune` | boolean | `false`; remote scale sets remain after pool removal |

### `autoUpdateRunners`

With `false`, automatic GitHub Actions runner software updates are disabled for every managed scale set.
You must keep Docker and VM runner images current. GitHub stops queueing jobs to outdated runners;
see [update requirements and image maintenance](providers.md#update-runner-images).

Set `true` to allow GitHub runner software updates. This applies to all configured Apps, repositories, and pools.
Reload or restart to apply it. The setting is applied during repository reconciliation; an API failure is logged and that repository's reconciliation fails.
It does not restart runners or rebuild your images.

## GitHub configuration

```yaml
github:
  apps:
    - app_id: 123456
      private_key_file: /etc/runnerfoundry/github-app.pem
  discovery_interval: 5m
```

| Key | Type | Default and validation | Effect |
| --- | --- | --- | --- |
| `github.apps` | list | Required, non-empty | Apps used for repository discovery and authentication, in precedence order |
| `github.apps[].app_id` | integer | Required, positive, unique | Numeric App ID, not a Client ID or installation ID |
| `github.apps[].private_key_file` | string | Required; readable file with non-empty key material | Private key for that App |
| `github.discovery_interval` | duration string | `5m`; at least `1m` | Repository rediscovery interval for each App |

Duration strings use units such as `1m`, `30m`, or `1h30m`.
All private keys are prepared before startup or reload is accepted. Key contents must be valid for GitHub App authentication.
GitHub App authentication is the only supported authentication method; supply keys through files, not inline YAML or PAT settings.
The `env://` and `file://` value forms described below apply to PostgreSQL settings, not arbitrary YAML strings.

Every Runner Pool is published to every repository discovered through the Apps. When Apps overlap, the first successfully authoritative App owns that repository's routes.
Lower-priority Apps still serve non-overlapping repositories. Keep App order identical across cluster members.
If one App later fails authentication or discovery, health can degrade while trustworthy routes from other Apps continue working.

Removing an App stops its active polling and bootstrap routes but does not delete its remote scale sets.
Re-add the App to resume management, or follow [retirement guidance](operations.md#retire-a-pool-or-app).

## Logging configuration

```yaml
log:
  format: text
  level: info
```

| Key | Type | Default | Validation and effect |
| --- | --- | --- | --- |
| `log.format` | string | `text` | `text` or `json` |
| `log.level` | string | `info` | `debug`, `info`, `warn`, or `error` |
| `log.file` | mapping | absent | When present, sends daemon logs to a rotating file instead of standard output |
| `log.file.path` | string | none | Required with `log.file`; absolute path writable by the service account |
| `log.file.max_size_mb` | integer | `100` | Positive; maximum file size in MiB before rotation |
| `log.file.max_backups` | integer | `5` | Positive; number of rotated files retained |

For example:

```yaml
log:
  format: json
  level: info
  file:
    path: /var/log/runnerfoundry/runnerfoundry.log
    max_size_mb: 100
    max_backups: 5
```

Do not share one log file between members. With file logging, use that file for daemon diagnostics rather than expecting those messages in the journal.
Reload applies logging changes after preparing the destination. See [find logs](operations.md#find-logs).

## State configuration

Choose exactly one of `state.path` and `state.postgres`. Both together, or neither, are rejected.

### SQLite

```yaml
state:
  path: /var/lib/runnerfoundry/state.db
```

`state.path` is a required non-empty path when PostgreSQL is absent. The service account needs access to its parent directory.
Only one member may use the database. RunnerFoundry exclusively owns the live database;
see [backups and upgrades](operations.md#back-up-and-upgrade-sqlite).

### PostgreSQL

Prepare the database and role using [deployment](deployment.md#use-postgresql-on-one-host), then supply either structured fields or a DSN:

```yaml
state:
  postgres:
    role: runnerfoundry
    host: postgresql.example.net
    port: "5432"
    name: runnerfoundry
    user: file:///etc/runnerfoundry/secrets/postgres-user
    password: file:///etc/runnerfoundry/secrets/postgres-password
```

| Key | Type | Requirement and effect |
| --- | --- | --- |
| `state.postgres.role` | string | Required; stable PostgreSQL role assumed after connecting; login must be permitted to assume it |
| `state.postgres.dsn` | string | Optional connection string; mutually exclusive with primary structured fields |
| `state.postgres.host` | string | Optional structured host |
| `state.postgres.port` | string | Optional structured TCP port; resolved number must be from 1 to 65535 |
| `state.postgres.name` | string | Optional structured database name |
| `state.postgres.user` | string | Optional structured login identity |
| `state.postgres.password` | string | Optional structured login password |
| `state.postgres.direct` | mapping | Optional session-affine endpoint, described below |

`role` remains separate and required with a DSN; do not embed it as a DSN role parameter.
A DSN cannot be combined with primary `host`, `port`, `name`, `user`, or `password`.
RunnerFoundry leaves omitted connection values to the PostgreSQL client configuration.
For predictable deployment, explicitly provide your environment's required connection values rather than relying on ambient defaults.
For DSN syntax, TLS parameters, and client defaults, see the [pgx connection reference](https://pkg.go.dev/github.com/jackc/pgx/v5@v5.10.0/pgconn#ParseConfig).

For example, with a protected connection-string file:

```yaml
state:
  postgres:
    role: runnerfoundry
    dsn: file:///etc/runnerfoundry/secrets/postgres-dsn
```

### PostgreSQL value sources

Every non-empty PostgreSQL string setting, including `role` and every `direct` field, accepts these forms:

| Form | Example | Conditions |
| --- | --- | --- |
| Literal | `host: postgresql.example.net` | Used as written |
| Environment | `user: env://RUNNERFOUNDRY_POSTGRES_USER` | Variable must exist and contain a non-empty value in the daemon's environment |
| File | `password: file:///etc/runnerfoundry/secrets/postgres-password` | Absolute readable path; contents must be non-empty after surrounding whitespace is removed |

Malformed references, missing values, unreadable files, and invalid resolved ports fail connection preparation.
An interactive shell's environment is not automatically the systemd service's environment. The packaged service defines no environment file;
use protected value files for the deployment examples or configure your own systemd environment explicitly.
Protect passwords and DSNs; do not post them in logs or support reports.

### Direct PostgreSQL connection

Use `state.postgres.direct` when the primary endpoint does not preserve database sessions, such as a transaction-pooling proxy.
The direct endpoint must preserve sessions for coordination and migration operations. Both endpoints must reach the same RunnerFoundry database.
If omitted, the primary endpoint must meet both needs.

| Key | Type | Effect |
| --- | --- | --- |
| `state.postgres.direct.dsn` | string | Alternative DSN; cannot be mixed with direct structured fields |
| `state.postgres.direct.host` | string | Overrides primary structured host |
| `state.postgres.direct.port` | string | Overrides primary structured port; resolved value must be from 1 to 65535 |
| `state.postgres.direct.name` | string | Overrides primary structured database name |
| `state.postgres.direct.user` | string | Overrides primary structured login |
| `state.postgres.direct.password` | string | Overrides primary structured password |

With a structured primary connection, omitted direct structured values inherit primary values.
For example, adding `direct: {host: postgresql-direct.example.net}` changes only the host.
An explicit direct DSN supplies its own connection configuration instead of inheriting structured fields.
With a primary DSN, a differing direct connection must also be a DSN; structured overrides are rejected.
The effective role always comes from `state.postgres.role`.

## Runner Pool configuration

`scale_sets` must contain at least one pool. Each member declares its own capacity and exactly one provider per pool.

```yaml
scale_sets:
  - name: linux-docker
    runner_group: default
    max_runners: 2
    stop_if_not_started_within: 5m
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

| Key | Type | Default and validation | Effect |
| --- | --- | --- | --- |
| `scale_sets[].name` | string | Required, non-empty, unique within the member | Pool name and workflow `runs-on` value |
| `scale_sets[].runner_group` | string | `default` | GitHub runner-group name; same-pool cluster declarations must agree |
| `scale_sets[].max_runners` | integer | Required, positive | Capacity contributed by this member; live same-pool member capacities add together |
| `scale_sets[].stop_if_not_started_within` | duration string | `5m`; positive, such as `10m` | Time from claim until runner acceptance before recovery becomes eligible |
| `scale_sets[].docker` | mapping | Choose exactly one of `docker`, `libvirt`, and `orchard` | Docker Compose provider |
| `scale_sets[].libvirt` | mapping | Choose exactly one of `docker`, `libvirt`, and `orchard` | libvirt provider |
| `scale_sets[].orchard` | mapping | Choose exactly one of `docker`, `libvirt`, and `orchard` | Orchard macOS provider |

The start timeout does not limit accepted workflow execution. On expiry, recovery needs an identifiable provider endpoint.
RunnerFoundry cleans up the unaccepted attempt and requeues the same GitHub demand at the queue tail.
Cleanup failure retains processing for another cleanup attempt. It is not a total job timeout or retry limit.
Reload applies a changed timeout.

### Docker settings

| Key | Type | Requirement |
| --- | --- | --- |
| `scale_sets[].docker.compose_file` | string | Required, non-empty Compose file path |
| `scale_sets[].docker.service` | string | Required, non-empty service name present in that Compose model |

RunnerFoundry checks the service before activating the pool. See the [container contract](providers/docker.md#prepare-and-configure-a-service).

### libvirt settings

| Key | Type | Requirement |
| --- | --- | --- |
| `scale_sets[].libvirt.connection_uri` | string | Required, non-empty libvirt URI, such as `qemu:///system` |
| `scale_sets[].libvirt.domain_xml_template` | string | Required, non-empty domain XML template path |

The template must be readable and renderable, and the connection accessible before activation.
See the [guest and domain contract](providers/libvirt.md#libvirt-guest-contract).

### Orchard settings

```yaml
scale_sets:
  - name: macos-orchard
    max_runners: 1
    orchard:
      controller_url: https://orchard.example.net:8443
      service_account: runnerfoundry
      token_file: /etc/runnerfoundry/orchard-token
      trusted_certificate_file: /etc/runnerfoundry/orchard-ca.pem
      vm_spec_file: /etc/runnerfoundry/macos-vm.json
```

| Key | Type | Requirement and effect |
| --- | --- | --- |
| `scale_sets[].orchard.controller_url` | string | Required HTTP or HTTPS controller URL; credentials, query strings and fragments are rejected |
| `scale_sets[].orchard.service_account` | string | Required non-empty Orchard service-account name; grant `compute:read` and `compute:write` |
| `scale_sets[].orchard.token_file` | string | Required readable file containing a non-empty service-account token |
| `scale_sets[].orchard.trusted_certificate_file` | string | Optional readable file containing exactly one PEM certificate to trust for the controller; normal system trust applies when omitted |
| `scale_sets[].orchard.vm_spec_file` | string | Required readable JSON file containing a native Orchard VM specification |

All Orchard file paths must be absolute. YAML does not expand `~` or environment variables.
RunnerFoundry reads the token, certificate and VM specification at startup or reload. Reload after replacing their contents.
Pool activation checks controller authentication, inventory reads and the local VM specification.
It does not test permission to create or delete VMs, connect to the guest agent, or establish guest readiness.
The service account must have `compute:read` and `compute:write` before processing work.
Use the [Orchard setup and guest checks](providers/orchard.md) before sending work.

The VM file uses the flat Orchard 0.58.0 `v1.VM` JSON shape, not a wrapper containing `spec`.
`image` is required. `runtime`, `os`, `arch` and `powerState` may be omitted or set to `tart`, `darwin`, `arm64` and `running`, respectively.
Do not provide `name` or server-maintained fields such as `status`, `worker`, `generation` or `conditions`.
RunnerFoundry assigns each attempt's name; other native editable settings, including resources, scheduling labels, host directories,
endpoints and restart policy, retain their Orchard meaning. Unknown JSON fields are rejected.
Use `restart_policy: Never` for the supplied one-job guest. See the [example VM file](../examples/orchard-macos-runner/vm.json).

The normalized controller URL identifies the provider endpoint across RunnerFoundry members.
An image or credential change applies to new attempts after reload without changing that endpoint.
Changing the URL does not move VMs. Drain and clean up attempts before changing it.
If the former endpoint is no longer configured, processing ownership remains retained for those attempts.
Restore a pool configuration with the former controller URL and working credentials, then reload or restart;
merely restoring credential or VM files does not restore management of that endpoint.
See [endpoint recovery](providers/orchard.md#recover-after-changing-the-controller).

## Pruning

`prune` is an optional boolean, default `false`. With `false`, removing a pool does not delete its remote GitHub scale sets.
With `true`, deletion requires all of the following:

- no live member declares that pool
- the repository is present in successful discovery
- RunnerFoundry has stored ownership evidence for the scale set
- GitHub's current ID, name, and runner group match that evidence

Already absent resources lose their ownership record without another deletion. Unavailable repositories, ownership mismatches, and deletion failures retain evidence for later checks.
Removing an App does not delete its remote scale sets, even with pruning enabled.
Use the [retirement procedure](operations.md#retire-a-pool-or-app) rather than assuming configuration removal deletes resources.

## Command-line reference

### Run the daemon

```sh
runnerfoundry --config /etc/runnerfoundry/config.yaml
```

`--config` is required. The service supplies it. `--active-runner-policy=leave` is the default;
`--active-runner-policy=terminate` destroys affected active runner resources on shutdown or incompatible reload.
An incompatible reload removes a pool or changes its provider kind. Accepted jobs do not become provisioning retries.
This is a daemon option, not YAML. See [stop and restart](operations.md#stop-and-restart) before changing it.

### Print the version

```sh
runnerfoundry --version
```

Prints `runnerfoundry <version>` and exits without starting the daemon. Use it alone, without other arguments.

### PostgreSQL migrations

```sh
sudo -u runnerfoundry runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
sudo -u runnerfoundry runnerfoundry migrate up --config /etc/runnerfoundry/config.yaml
sudo -u runnerfoundry runnerfoundry migrate down --to <version> --config /etc/runnerfoundry/config.yaml
```

`status` inspects the database and reports `current=<n> target=<n> known=<min>..<max>`.
`up` applies known forward migrations. `down` requires a known schema target via `--to`; that flag is rejected for other actions.
Successful changes report `Migration up completed at version <n>.` or the corresponding `down` message.
Failures are written to standard error and exit non-zero.

These commands require PostgreSQL configuration and its credentials; they do not start runner processing or require a complete daemon configuration.
Do not run Down against active members. Follow [migration and rollback procedures](migrations.md).
