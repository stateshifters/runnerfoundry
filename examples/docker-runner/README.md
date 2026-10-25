# Run the Docker Compose example

Complete [Docker provider setup](../../docs/providers/docker.md#install-and-prepare-the-host).
From the repository root, install this example's service:

```sh
sudo install -D -m 0644 examples/docker-runner/compose.yaml /etc/runnerfoundry/compose.yaml
```

Add this pool to your existing configuration, keeping its `github` and `state` settings:

```yaml
scale_sets:
  - name: linux-docker
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

Confirm the service is visible:

```sh
sudo -u runnerfoundry docker compose -f /etc/runnerfoundry/compose.yaml config --services
```

The output must include `runner`. [Start RunnerFoundry](../../README.md#3-start-runnerfoundry) for a new installation,
or [reload the running service](../../docs/operations.md#reload-configuration).
Run the [first-job workflow](../../README.md#4-run-your-first-job)
with `runs-on: linux-docker`; confirm the job succeeds and its container is removed.
