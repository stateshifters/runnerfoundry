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
    provider: docker
    docker:
      compose_file: ./compose.yaml
      compose_project: github-scaler
      service: runner
      env_jit_config_name: RUNNER_JIT_CONFIG

  - name: linux-vm
    runner_group: default
    max_runners: 1
    provider: libvirt
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: ./domain-template.xml
```

Github authentication is PAT-only in V1. Use exactly one of:

- `github.token_env`
- `github.token_file`

Plaintext `github.token` values in YAML are rejected. Do not commit tokens, JIT configs, queue tokens, or generated runtime config containing secrets.

`log.format` accepts `text` or `json`.

## Docker Provider

Docker runner configuration is Compose-owned. Put images, env files, volumes, labels, networks, command, user, workdir, and other Docker-native settings in the Compose service.

For each runner, `github-scaler` runs:

```text
docker compose -f <compose_file> -p <compose_project> run --detach --rm --name <runner> --label github-scaler.scale-set=<name> --label github-scaler.runner=<runner> --env <jit_env>=<jit_config> <service>
```

`env_jit_config_name` defaults to `RUNNER_JIT_CONFIG`. The service image must consume that value and start the Github runner with the JIT config. The official Github runner image expects `ACTIONS_RUNNER_INPUT_JITCONFIG`; see [examples/docker-runner](examples/docker-runner). The runner process should exit after its assigned job; `--rm` lets Docker remove the container.

Restart recovery uses Docker labels to discover owned containers. Running owned containers count as active capacity. Stopped owned leftovers are removed during reconciliation.

## Libvirt Provider

Libvirt runner configuration comes from a domain XML template. The template must describe an ephemeral VM image that starts a Github runner from the delivered JIT config and powers off after the runner exits.

The V1 implementation parses and renders XML with `libvirt.org/go/libvirtxml`, then applies it through `virsh` against `connection_uri`. The rendered domain gets:

- a unique domain name matching the runner name
- `github-scaler.scale-set` metadata
- `github-scaler.runner` metadata
- `github-scaler.jit-env` metadata set to `RUNNER_JIT_CONFIG`

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
