# Optional Ubuntu libvirt runner image

This example builds an Ubuntu 26.04 LTS qcow2 image for a libvirt-backed Runner Pool. Packer is optional: an image built with other tooling is compatible when it satisfies the [libvirt guest contract](../../docs/providers.md#libvirt-guest-contract).

The image contains the GitHub Actions runner and a startup launcher. Add every shell, runtime, package and certificate that your workflows require to `provision.sh` before building. The example does not provide a maintained general-purpose workflow environment.

## Build the image

Use an AMD64 Linux host with KVM acceleration, Packer 1.14.0 or later, QEMU, OVMF, an ISO creation tool such as `xorriso`, OpenSSH tools including `ssh-keygen`, and outbound access to Ubuntu images, GitHub releases, Packer plugins and Ubuntu package repositories.

The template expects readable firmware files at `/usr/share/OVMF/OVMF_CODE_4M.fd` and `/usr/share/OVMF/OVMF_VARS_4M.fd`.
Check that your OVMF package supplies them, or adjust the template's firmware paths for your host before building.

From the repository root:

```sh
cd examples/libvirt-ubuntu-runner
ssh-keygen -q -t ed25519 -N '' -f packer-key
packer init .
packer build -var "ssh_private_key_file=$PWD/packer-key" .
rm -f packer-key packer-key.pub
```

The output is `output/runnerfoundry-ubuntu.qcow2`. The temporary private key is a build credential; keep it out of logs and remove both key files after the build.

## Install and configure the image

Install the image and domain template at stable paths:

```sh
sudo install -D -m 0644 output/runnerfoundry-ubuntu.qcow2 \
  /var/lib/runnerfoundry/images/runnerfoundry-ubuntu.qcow2
sudo install -D -m 0644 domain.xml /etc/runnerfoundry/domain.xml
```

Complete the [installation and GitHub App setup](../../docs/installation.md), then start from a complete daemon configuration such as the [quick-start configuration](../../README.md#2-configure-a-runner-and-its-pool).
Before the configuration has first run, you can replace its `scale_sets` list with the following list; keep its `github` and `state` settings.
For an existing deployment, add only the `- name: linux-vm` list entry beneath its existing `scale_sets:` key, then reload and try the first job below.
If you want to remove an existing pool afterwards, follow [pool retirement](../../docs/operations.md#retire-a-pool-or-app) so its remote registrations are handled deliberately.
Do not add a second top-level `scale_sets` key.

```yaml
scale_sets:
  - name: linux-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/domain.xml
```

After saving the configuration, [reload RunnerFoundry](../../docs/operations.md#reload-configuration), or start it if this is a new installation.

The sample XML expects an active libvirt network named `default` with guest access to GitHub. The libvirt QEMU process must be able to traverse the image directory and read the qcow2 image under the host's discretionary access control, access control lists, SELinux or AppArmor policy. Access by the `runnerfoundry` service account alone does not establish QEMU access.

Do not replace the backing image while domains are using it. Each domain uses a transient disk, so guest writes are discarded when the domain ends and the backing image remains operator-owned.

## Run a first job

Confirm that the `linux-vm` Runner Pool is available in GitHub. Follow the [complete first-job workflow instructions](../../README.md#4-run-your-first-job), replacing that workflow's `jobs` block with:

```yaml
jobs:
  smoke-test:
    runs-on: linux-vm
    steps:
      - run: uname -a
```

The guest starts one JIT-configured runner and powers off after the job exits. Use the [libvirt guest contract](../../docs/providers.md#libvirt-guest-contract) to diagnose a guest that starts without accepting a job.

## Update and rebuild the image

The image pins its Ubuntu cloud image URL and SHA-256 checksum, GitHub Actions runner version and checksum, and Packer plugin version in `ubuntu.pkr.hcl`. Follow [Update runner images](../../docs/providers.md#update-runner-images) to review upstream releases, update each affected version and checksum together, rebuild the image, and replace the installed backing image only after existing domains have stopped.

[GitHub publishes runner asset checksums in each runner release](https://github.com/actions/runner/releases). The pinned checksum must match the Linux x64 archive named by `github_runner_version`. Treat the Ubuntu image URL and checksum as one pin and verify them against Ubuntu's release metadata.

## Maintainer validation

Maintainers changing the launcher can run its hermetic check separately from the operator build and first-job path:

```sh
bash runnerfoundry-jit-launcher.test.sh
```
