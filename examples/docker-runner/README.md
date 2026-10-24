# Optional Docker Compose example

RunnerFoundry does not build container images or require this Compose file. The operator may supply any Compose service whose runner consumes `ACTIONS_RUNNER_INPUT_JITCONFIG`, executes one job, and exits.

This optional example uses GitHub's runner image as the smallest evaluation setup. From the repository root, copy it to the path referenced by the Docker-backed Runner Pool:

```sh
cd examples/docker-runner
sudo install -D -m 0644 compose.yaml /etc/runnerfoundry/compose.yaml
```

Configure the Runner Pool through the GitHub adapter's `scale_sets` setting:

```yaml
scale_sets:
  - name: linux-docker
    runner_group: default
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

The mutable `latest` image keeps this example short. Pin a reviewed tag or digest before using it as a maintained runner environment, and add the tools, volumes, networks, and security controls required by the workflows it will execute. The [Docker provider guide](../../docs/providers.md#docker-compose) explains the runtime contract and ownership boundary.

Follow [image maintenance](../../docs/providers.md#update-runner-images) to find current runner releases, pin an image, and refresh cached capacity.
