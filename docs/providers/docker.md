# Docker Compose runners

Use Docker Compose for Linux jobs that fit a container. RunnerFoundry creates a new container from the configured service for each attempt.

## Install and prepare the host

Install RunnerFoundry using the [Linux package](../installation.md#install-the-package) or [macOS installer](../installation-macos.md),
then [create the GitHub App](../installation.md#create-the-github-app).
On Linux, install Docker Engine and the Compose plugin using [Docker's host installation instructions](https://docs.docker.com/engine/install/).
On macOS, install and start [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) or another Docker environment providing the CLI and Compose plugin.
The daemon needs `docker` on its `PATH`, access to the Docker endpoint, and registry credentials for the chosen image.
Its environment also supplies Compose interpolation values and Docker context settings.

On a Linux package host, check access as the daemon account:

```sh
sudo -u runnerfoundry docker info
sudo -u runnerfoundry docker compose version
```

On macOS, run the equivalent checks as the user who will run RunnerFoundry:

```sh
docker info
docker compose version
```

Use the same account and Docker context for preparation, daemon operation, and recovery.
The Linux package joins Docker's group only if that group already exists when RunnerFoundry is installed.

## Prepare and configure a service

The host needs Docker Engine, the Compose plugin, and daemon-account access to Docker.
Your Compose service must contain an unregistered GitHub Actions runner that consumes `ACTIONS_RUNNER_INPUT_JITCONFIG`, executes one job, and exits.
RunnerFoundry supplies this environment value; do not put a JIT value in the Compose file or configure a permanent runner registration.

On a Linux package host, save a Compose model such as this at `/etc/runnerfoundry/compose.yaml`.
On macOS, save it as `compose.yaml` beside `config.yaml` in the [private working directory](../installation-macos.md#prepare-configuration-and-provider-access):

```yaml
services:
  runner:
    image: ghcr.io/actions/actions-runner:latest
    user: runner
    working_dir: /home/runner
    command: ["/home/runner/run.sh"]
```

Add its pool under `scale_sets`, keeping the configuration's `github` and `state` settings:

```yaml
scale_sets:
  - name: linux-docker
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

For macOS, set `compose_file: ./compose.yaml` instead and keep that working directory when starting the daemon.
Check the model from that directory as your user:

```sh
docker compose -f ./compose.yaml config --services
```

It must list `runner` for this service.

RunnerFoundry validates that `runner` exists in this Compose model before activating the pool.
It starts detached one-job containers and cleans up failed or completed resources. If cleanup fails, capacity may remain occupied while cleanup is retried.


The [Compose example](../../examples/docker-runner/README.md) contains the same minimal service and exact steps to use it.
Add the tools required by your workflows and choose an image tag or digest for repeatable use.
Only expose the volumes, networks, devices, and privileges those jobs need.
See [Docker settings](../configuration.md#docker-settings) for the configuration reference.

Compose validation confirms the named service exists; it does not establish runner registration, image availability, or tool suitability.
For the Linux package, [start the daemon](../../README.md#3-start-runnerfoundry) for a new installation,
or [reload the running service](../operations.md#reload-configuration).
On macOS, [start the installed command](../installation-macos.md#start-runnerfoundry) from your working directory.

## How containers run and clean up

RunnerFoundry uses [`docker compose run`](https://docs.docker.com/reference/cli/docker/compose/run/) in detached mode with automatic container removal.
The service's command, entrypoint, volumes, and network settings supply the execution environment.
Service ports are not published by this invocation. Linked dependency services can start with the runner.
RunnerFoundry removes the exact managed runner container, including after a failed attempt.
Dependency containers, volumes, networks, and external storage remain operator-owned; a disposable runner does not erase writes to mounted storage.

The runner reports its job result to GitHub. A container starting or exiting alone does not prove that it accepted or completed a workflow job.
Follow the [first-job workflow](../../README.md#4-run-your-first-job), using your pool name as `runs-on`.
Confirm the job succeeds and its container is removed afterwards.

## Update images

1. Choose a current runner image and required tools, and record its tag or digest.
2. Pull or build it on every member serving the pool.
3. Update the Compose image reference. Reload if RunnerFoundry YAML changed.
4. Run a small job and check the runner version, tools, and container removal.

Compose's [`pull_policy`](https://docs.docker.com/reference/compose-file/services/#pull_policy) controls registry access at launch.
For an image-only service, the default `missing` policy uses cached images except that `latest` is always pulled.
`always` pulls on every launch; `never` requires a cached image. Use a digest when you need fixed image contents.
Existing containers keep their image. Keep accepted jobs running while updating capacity.
The [shared update guidance](../providers.md#update-runner-images) explains runner update requirements and upstream release sources.

## Troubleshoot Docker access

If `docker info` fails as the daemon account, inspect the Docker endpoint, socket ownership, and host security policy.
On a Linux package host where Docker's group was created after RunnerFoundry installation:

```sh
sudo usermod -aG docker runnerfoundry
sudo systemctl restart runnerfoundry
sudo -u runnerfoundry docker info
```

The final command must succeed. Docker group membership grants [root-level host privileges](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).
Check [active runner shutdown policy](../operations.md#choose-active-runner-shutdown-behaviour) before restarting during work.

## Troubleshoot queued jobs and failed attempts

Check that `runs-on` matches the pool and that the configured service is visible to the daemon account:

For the Linux package:

```sh
sudo -u runnerfoundry docker compose -f /etc/runnerfoundry/compose.yaml config --services
```

For macOS, run `docker compose -f ./compose.yaml config --services` as your user from the working directory.

The output must include `runner` for the configuration above. Correct missing files, interpolation values, service names, registry access,
and image startup errors shown in daemon logs. Check the image contains a compatible unregistered runner and has guest network access to GitHub.

On a Linux package host, inspect containers without dumping their environment:

```sh
sudo -u runnerfoundry docker ps --all
```

On macOS, run `docker ps --all` as your user in the same Docker context.

Use `docker logs <container-id>` while the affected container still exists. Automatic removal means its local logs may disappear on exit.
Arrange [external diagnostic collection](../providers.md#security-and-diagnostic-logs) before relying on disposable runners for production work.
Never collect raw bootstrap values or unredacted environment dumps.

If cleanup fails, restore access to the same Docker endpoint and let RunnerFoundry retry its owned cleanup.
Do not use a blanket prune command to resolve one managed container's failure.
For GitHub discovery, capacity, and retry symptoms, follow [shared queued-job diagnosis](../operations.md#jobs-stay-queued)
and [provisioning recovery](../operations.md#provisioning-attempts-keep-retrying).
