# Github Scaler

`github-scaler` is a Linux-only Go daemon for autoscaling ephemeral Github Actions runner scale sets.

V1 polls Github scale-set demand, scales to `statistics.TotalAssignedJobs` capped by each set's `max_runners`, starts Docker Compose services or libvirt domains with Github JIT runner config, rebuilds provider state after daemon restarts, and cleans up completed runner instances.

## Configuration

The daemon requires `--config <path>`.

```yaml
github:
  url: https://github.com/OWNER/REPO
  token_env: GITHUB_SCALER_PAT

log:
  format: text

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

Github authentication is PAT-only in V1. Use exactly one of:

- `github.token_env`
- `github.token_file`

Plaintext `github.token` values in YAML are rejected. Do not commit tokens, JIT configs, queue tokens, or generated runtime config containing secrets.

`log.format` accepts `text` or `json`.

## Install as a Daemon

Install the Debian package on Ubuntu or Debian hosts:

```sh
sudo apt install ./github-scaler_1.0.0_amd64.deb
```

The package installs:

- `/usr/bin/github-scaler`
- `/lib/systemd/system/github-scaler.service`
- `/etc/github-scaler/config.yaml`
- `/etc/github-scaler/github-scaler.env`

Package installation creates the `github-scaler` service user and reloads systemd units. It does not enable or start the daemon because the config and token must be set first.

If the configured providers require host group access, add the service user to those groups before starting the daemon:

```sh
sudo usermod -aG docker github-scaler
sudo usermod -aG libvirt github-scaler
```

Edit `/etc/github-scaler/config.yaml` and `/etc/github-scaler/github-scaler.env`, then enable the service:

```sh
sudo systemctl enable --now github-scaler
sudo systemctl status github-scaler
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

Create the config directory and store secrets outside the YAML:

```sh
sudo install -d -m 0750 -o root -g github-scaler /etc/github-scaler
sudo install -m 0640 -o root -g github-scaler config.yaml /etc/github-scaler/config.yaml
sudo install -m 0640 -o root -g github-scaler github-scaler.env /etc/github-scaler/github-scaler.env
```

The environment file should define the token referenced by `github.token_env`:

```sh
GITHUB_SCALER_PAT=ghp_example
```

Use absolute paths inside `/etc/github-scaler/config.yaml`, especially for Docker Compose files and libvirt XML templates. The daemon has no required working directory and stores no local state.

Create `/etc/systemd/system/github-scaler.service`:

```ini
[Unit]
Description=GitHub runner scale-set scaler
After=network-online.target docker.service libvirtd.service
Wants=network-online.target

[Service]
Type=simple
User=github-scaler
Group=github-scaler
EnvironmentFile=/etc/github-scaler/github-scaler.env
ExecStart=/usr/local/bin/github-scaler --config /etc/github-scaler/config.yaml
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
- `github-scaler.scale-set` metadata
- `github-scaler.runner` metadata
- `github-scaler.jit-env` metadata set to `ACTIONS_RUNNER_INPUT_JITCONFIG`

The JIT config itself is not embedded directly in the domain XML. The VM template is responsible for delivering it by its documented convention, such as cloud-init, fw_cfg, a mounted config drive, or an image-specific bootstrap script.

Restart recovery lists libvirt domains, filters to scaler-owned metadata, treats running owned domains as active capacity, and undefines shutoff owned domains during reconciliation.

## Scale Behavior

For each scale set, the daemon:

- creates the Github runner scale set if needed
- polls Github scale-set messages with `maxCapacity = max_runners`
- uses `statistics.TotalAssignedJobs` as desired capacity
- caps desired capacity at `max_runners`
- generates one JIT config for each missing runner
- acknowledges Github messages only after local handling succeeds

The daemon stores no local database. All restart recovery comes from Github statistics plus provider-visible Docker labels/names and libvirt metadata.

## Development & Contributing

Please follow the [contributing guidelines](CONTRIBUTING.md) when contributing to this project. We welcome contributions from the community, whether it's bug fixes, new features, or improvements to existing code.
