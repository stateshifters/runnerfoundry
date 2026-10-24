# Choose and maintain runner environments

Use Docker Compose for Linux jobs that fit a container. Use libvirt when a workflow needs Windows, a full operating system,
VM-specific devices or firmware, or a VM isolation boundary. Each pool uses one provider per member.

RunnerFoundry creates and cleans up runner instances. You supply the images, workflow tools, networking, storage, and access policy.
The [quick start](../README.md#quick-start) is the shortest Docker evaluation path.

## Docker Compose

The host needs Docker Engine, the Compose plugin, and service-account access to Docker.
Your Compose service must contain an unregistered GitHub Actions runner that consumes `ACTIONS_RUNNER_INPUT_JITCONFIG`, executes one job, and exits.
RunnerFoundry supplies this environment value; do not put a JIT value in the Compose file or configure a permanent runner registration.

For example:

```yaml
services:
  runner:
    image: ghcr.io/actions/actions-runner:latest
    user: runner
    working_dir: /home/runner
    command: ["/home/runner/run.sh"]
```

Configure its pool:

```yaml
scale_sets:
  - name: linux-docker
    max_runners: 2
    docker:
      compose_file: /etc/runnerfoundry/compose.yaml
      service: runner
```

RunnerFoundry validates that `runner` exists in this Compose model before activating the pool.
It starts detached one-job containers and cleans up failed or completed resources. If cleanup fails, capacity may remain occupied while cleanup is retried.

The base image is a minimal evaluation environment. Add the tools required by your workflows, and pin a reviewed image tag or digest for repeatable use.
Define only the volumes, networks, and privileges those jobs need. See the [optional Compose example](../examples/docker-runner/README.md).

## libvirt

The host needs `virsh`, a reachable libvirt connection, and permission for the service account to use it.
The sample images additionally need Linux AMD64 with QEMU/KVM, suitable firmware, and the `default` libvirt network.
Their READMEs describe image-building tools; Packer is optional tooling, not a RunnerFoundry dependency.

Configure a pool with a domain XML template:

```yaml
scale_sets:
  - name: linux-vm
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/domain.xml
```

The template must be readable by RunnerFoundry and valid for the target host's libvirt/QEMU features.
RunnerFoundry starts transient domains. Your storage design must also discard guest writes if jobs need a clean disk on every attempt;
a transient domain alone does not make its backing storage disposable.

### Libvirt guest contract

Supply an image with the GitHub Actions runner installed but not registered. At startup, the guest must:

1. Read the SMBIOS type 11 value prefixed `runnerfoundry.jit=` without logging or persisting the value.
2. Pass the value to the runner through the child process's `ACTIONS_RUNNER_INPUT_JITCONFIG` environment variable.
3. Run the runner once for one job.
4. Remove transient runner and work state, then shut down after the attempt.

The domain template must permit SMBIOS system information. RunnerFoundry supplies the domain name, its `urn:runnerfoundry` ownership metadata,
and the JIT SMBIOS value; do not rely on custom values in those reserved fields surviving rendering.
Keep bootstrap secrets out of image files, command-line arguments, console output, and persistent storage.

You own the backing image, firmware, network, and disk lifecycle. QEMU needs permission to traverse image directories and read the files,
including under host ACL, SELinux, or AppArmor policy. Service-account readability alone does not establish QEMU access.
Do not overwrite or remove a backing image while domains are using it.

Start from one of these optional examples:

- [Ubuntu image](../examples/libvirt-ubuntu-runner/README.md)
- [Windows Server image](../examples/libvirt-windows-runner/README.md)

They demonstrate compatible guests and transient disks, not hardened production images or a promise that every workflow tool is installed.
Use a workflow shell available in the image; the Windows example includes Windows PowerShell, not PowerShell 7.

## Update runner images

RunnerFoundry defaults to `autoUpdateRunners: false`. You must keep the runner inside every image current.
GitHub requires updates within 30 days of a new release and can require critical security updates sooner;
outdated runners stop receiving jobs. See [GitHub's update requirements](https://docs.github.com/en/actions/reference/runners/self-hosted-runners#runner-software-updates-on-self-hosted-runners).
The [configuration setting](configuration.md#autoupdaterunners) can allow runner software updates, but it does not rebuild images or install your workflow tools.

### Find and verify versions

Use official sources and pin the exact artifacts you review:

| Component | Upstream source | What to record |
| --- | --- | --- |
| GitHub Actions runner | [Runner releases](https://github.com/actions/runner/releases) | Stable version and SHA-256 for the correct OS and architecture |
| Container image | [Official runner image](https://github.com/actions/runner/pkgs/container/actions-runner) | Available tag or digest, runner version, and your added tools |
| Windows VirtIO media | [Upstream download guidance](https://github.com/virtio-win/virtio-win-pkg-scripts#downloads) | Resolved stable archive URL and SHA-256 of the downloaded ISO |

VirtIO's `latest` channel can contain prerelease-quality builds; resolve the stable link before choosing a version.
A checksum detects that subsequent downloads differ from the reviewed artifact; a locally calculated checksum is not an upstream signature.
Follow upstream authenticity guidance as well as checking hashes.

### Refresh Docker capacity

1. Choose a current runner image, add your workflow tools, and record its tag or digest.
2. Pull or build that image on every member serving the pool. A cached `latest` image does not become fresh merely because another job starts.
3. Update the Compose image reference on those members. If you changed RunnerFoundry YAML, reload it and check the result.
4. Run a small workflow and check its runner version and required tools in the job logs. Confirm that its container is removed afterwards.

Existing containers keep their current image. Plan changes around active work; do not destroy an accepted job just to refresh an image.

### Refresh VM capacity

1. Update the example's runner version and OS-specific SHA-256 together, using its README to find the pin locations.
2. For Windows media changes, keep the example README and its maintained build workflow on the same version and checksum.
3. Build a new image using the example's build instructions. Verify downloads before extracting or installing them.
4. Install the new image at a new path and prepare a domain template referencing it. Keep the prior image available for running domains and rollback.
5. Update `domain_xml_template` if using a new template path, then reload and check the result.
6. Run a small job, check its runner version and workflow tools, and confirm shutdown and resource cleanup. Remove old backing images only after no domains use them.

The examples remain examples: their pinned versions document a reproducible starting point, not a substitute for checking upstream releases when you build.

## Security and diagnostic logs

An ephemeral runner is not a security boundary by itself. Isolate untrusted code from trusted hosts and credentials.
Review image contents, mounted volumes, network access, Docker privileges, libvirt permissions, and host policy.
Protect GitHub App keys and PostgreSQL credentials. Never log or persist JIT bootstrap data.
See [GitHub's self-hosted runner security guidance](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners).

RunnerFoundry daemon logs describe provisioning and operations. They do not replace the runner application's diagnostic logs.
Before production use, arrange forwarding and preservation of ephemeral runner diagnostics outside the disposable container or VM,
as [GitHub recommends](https://docs.github.com/en/actions/reference/runners/self-hosted-runners#ephemeral-runners-for-autoscaling).
You choose that log storage and collection mechanism. Check that failed startup logs remain available after a runner disappears,
and keep secrets out of the forwarded data. See [operational diagnosis](operations.md#find-logs).
