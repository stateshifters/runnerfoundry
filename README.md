# Github Scaler

`github-scaler` is a Go daemon for autoscaling ephemeral Github Actions runner scale sets.

It is using GitHub's new [Scaleset library](https://github.com/actions/scaleset)

The current version supports Docker Compose and libvirt providers. The daemon polls Github for scale-set messages, stores waiting demand in SQLite, generates JIT configs, and starts ephemeral runners in the configured provider. It does not store Github credentials or JIT secrets in the database.

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

Create a Github App with repository administration access for self-hosted runners, install it on the accounts the daemon should serve, and grant it either selected-repository or all-repository access. `github-scaler` discovers every installation owned by the App and every repository exposed by each installation. Github installation settings are the repository opt-in boundary.

Set `github.app.app_id` to the numeric App ID. Store the App private key outside the YAML and set `github.app.private_key_file` to its path. Plaintext private keys and legacy PAT fields are rejected. The discovery interval defaults to `5m` and cannot be shorter than `1m`.

Each scale-set definition is created in every discovered repository. `max_runners` is one shared limit across all installations and repositories for that definition, not a per-repository limit. Jobs are admitted by Github queue time. The oldest observed job's repository receives the next available runner, while Github chooses the exact job assigned within that repository. FIFO ordering survives daemon restarts and reloads through the configured SQLite database.

`state.path` is required and should be an absolute path in production. Only one daemon may open a state database. Changing the path requires a restart; reload rejects the replacement configuration. Put the database on durable storage if a replacement host must resume queued work. Back up or migrate the database only while the service is stopped.

`max_runners` must be greater than zero; omit a scale set instead of setting zero capacity.

Github runner scale sets are routed by their scale-set name. Use the configured `scale_sets[].name` value in workflow `runs-on`; do not configure runner matching labels through Docker Compose environment variables such as `LABELS`.

Set `prune: true` only when `github-scaler` owns every runner scale set in the discovered repositories and runner groups. When enabled, discovery deletes Github runner scale sets that are not present in the current config. Leave it unset or `false` if another controller or human-managed runner scale set shares the same repositories.

`log.format` accepts `text` or `json`. `log.level` accepts `debug`, `info`, `warn`, or `error` and defaults to `info`. Lifecycle milestones are logged at `info`. Set the level to `debug` to include individual repository, scale-set, polling, reconciliation, cleanup, and reload actions. Failures remain visible at `warn` or `error` as appropriate.

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

Systemd readiness covers local preflight only. The daemon sends `READY=1` after the configuration, App credentials, and local settings validate. Github discovery, scale-set registration, and message-session creation run immediately afterward through the reconciliation loop. A Github outage or repository-specific registration failure does not block service startup; the daemon logs the failure and retries on the configured discovery interval.

After editing the config or replacing the private-key file, reload the service:

```sh
sudo systemctl reload github-scaler
```

Reload validates the replacement configuration and credentials before stopping the active scale sets. Systemd considers the reload accepted after this local preflight succeeds. The daemon then drains active runners, closes the old GitHub message sessions, unregisters the old runner scale sets, and reconciles the replacement configuration. Existing runner containers or VMs keep running until their active jobs exit. Watch progress with:

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

Restart recovery uses Docker labels to discover owned containers. Running owned containers count as active capacity. Stopped owned leftovers are removed during reconciliation.

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

The daemon commits identifiable Github demand to SQLite before acknowledging its message. Job-assigned and job-completed messages without positive request identities are not stored as queue entries; their assigned capacity comes from Github message statistics. Runner recovery combines the durable queue with provider-visible Docker labels/names and libvirt metadata. A duplicate job-available delivery updates the existing request rather than adding another queue entry.

When installation access is removed, the daemon immediately drops queued demand, destroys active local runners, closes the message sessions, and attempts to unregister the affected scale sets. This can interrupt active jobs. On normal shutdown, active jobs are left running while message sessions close and scale sets unregister.

## Migrating from PAT Authentication

PAT configuration is not supported. Replace `github.token_env` or `github.token_file` with `github.app.app_id` and `github.app.private_key_file`, remove every `scale_sets[].repositories` list, and install the App on the repositories the daemon should manage. Capacity changes from per-repository to one shared `max_runners` limit per scale-set definition.

## Development & Contributing

Please follow the [contributing guidelines](CONTRIBUTING.md) when contributing to this project. We welcome contributions from the community, whether it's bug fixes, new features, or improvements to existing code.
