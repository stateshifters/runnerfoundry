# Choose a runner provider

Each Runner Pool uses one execution provider per member. RunnerFoundry creates and cleans up runner instances;
you supply images, workflow tools, networking, storage, and host access.

| Provider | Use it for | Installation, operation and troubleshooting |
| --- | --- | --- |
| Docker Compose | Linux jobs that fit a container | [Docker guide](providers/docker.md) |
| libvirt | Full Linux or Windows VMs, devices, firmware, or VM isolation | [libvirt guide](providers/libvirt.md) |
| Orchard | macOS jobs on Apple Silicon workers | [Orchard guide](providers/orchard.md) |

The [quick start](../README.md#quick-start) is the shortest Docker evaluation path.
Use the provider guides for setup, guest contracts, image maintenance, and recovery.
For the daemon and state, follow [installation](installation.md), [configuration](configuration.md), and [operations](operations.md).

## Update runner images

RunnerFoundry defaults to `autoUpdateRunners: false`. You must keep the runner inside every image current.
GitHub requires updates within 30 days of a new release and can require critical security updates sooner;
outdated runners stop receiving jobs. See [GitHub's update requirements](https://docs.github.com/en/actions/reference/runners/self-hosted-runners#runner-software-updates-on-self-hosted-runners).
The [configuration setting](configuration.md#autoupdaterunners) can allow runner software updates, but it does not rebuild images or install your workflow tools.

### Find and verify versions

Use official sources and pin the exact artifacts you use:

| Component | Upstream source | What to record |
| --- | --- | --- |
| GitHub Actions runner | [Runner releases](https://github.com/actions/runner/releases) | Stable version and SHA-256 for the correct OS and architecture |
| Container image | [Official runner image](https://github.com/actions/runner/pkgs/container/actions-runner) | Available tag or digest, runner version, and your added tools |
| Windows VirtIO media | [Upstream download guidance](https://github.com/virtio-win/virtio-win-pkg-scripts#downloads) | Resolved stable archive URL and SHA-256 of the downloaded ISO |

VirtIO's `latest` channel can contain prerelease-quality builds; resolve the stable link before choosing a version.
A checksum detects that subsequent downloads differ from the pinned artifact; a locally calculated checksum is not an upstream signature.
Follow upstream authenticity guidance as well as checking hashes.

Follow the provider's update procedure:

- [Docker images](providers/docker.md#update-images)
- [libvirt images](providers/libvirt.md#update-images)
- [Orchard images](providers/orchard.md#update-images)

## Security and diagnostic logs

An ephemeral runner is not a security boundary by itself. Isolate untrusted code from trusted hosts and credentials.
Review image contents, mounted volumes, network access, Docker privileges, libvirt and Orchard permissions, and host policy.
Protect GitHub App keys and PostgreSQL credentials. Never log or persist JIT bootstrap data.
See [GitHub's self-hosted runner security guidance](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners).

RunnerFoundry daemon logs describe provisioning and operations. They do not replace the runner application's diagnostic logs.
Before production use, arrange forwarding and preservation of ephemeral runner diagnostics outside the disposable container or VM,
as [GitHub recommends](https://docs.github.com/en/actions/reference/runners/self-hosted-runners#ephemeral-runners-for-autoscaling).
You choose that log storage and collection mechanism. Check that failed startup logs remain available after a runner disappears,
and keep secrets out of the forwarded data. See [operational diagnosis](operations.md#find-logs).
