# Optional Windows libvirt runner image

This example builds a Windows Server 2025 Standard Core qcow2 image for a libvirt-backed Runner Pool. Packer is optional: an image built with other tooling is compatible when it satisfies the [libvirt guest contract](../../docs/providers.md#libvirt-guest-contract).

The image contains the GitHub Actions runner and a Windows PowerShell startup launcher. It does not install PowerShell 7, Docker, workflow-specific tools, Windows updates or production hardening. Provision every shell and tool your workflows require before using the image; a workflow that selects `pwsh` needs PowerShell 7 in the guest.

## Check the media restrictions

The template uses the English Windows Server 2025 Standard Core evaluation image. Microsoft limits evaluation installations to 180 days and requires online activation within the first 10 days. Use appropriately licensed media for capacity that must operate beyond the evaluation terms.

The public media URLs do not remove their license conditions. The download and build must stop on a checksum mismatch.

| Artifact | Version | SHA-256 |
| --- | --- | --- |
| Windows Server evaluation | Windows Server 2025, build 26100.32230 | `7b052573ba7894c9924e3e87ba732ccd354d18cb75a883efa9b900ea125bfd51` |
| VirtIO drivers | `0.1.302` | `303f7ae40dad495d6ae474fdc571df58958a4dbc5c37a522d80f9a203867949d` |
| GitHub Actions runner for Windows x64 | `2.338.0` | `f48e0750a21812bca5f82de5f7f5aeae71abee647fab5a582f1742d07eba455f` |

This example pins VirtIO 0.1.302 from the upstream virtio-win packaging project's stable channel. Upstream distinguishes its [stable downloads from the potentially prerelease-quality `latest` downloads](https://github.com/virtio-win/virtio-win-pkg-scripts#downloads), so use the stable channel when reviewing an update.

## Build the image

Use an AMD64 Linux host with KVM acceleration, QEMU, OVMF, Packer 1.15.4, `curl`, `openssl`, `sha256sum`, outbound access to the media and GitHub releases, and enough space for both installation media and a 64 GiB image.

The template expects readable firmware at `/usr/share/ovmf/OVMF.fd`.
Check that your OVMF package supplies it, or adjust the template's firmware path for your host before building.

Start in this directory and download the exact pinned media:

```sh
cd examples/libvirt-windows-runner

curl --fail --location --retry 3 \
  --output windows-server-2025.iso \
  'https://software-static.download.prss.microsoft.com/dbazure/998969d5-f34g-4e03-ac9d-1f9786c66749/26100.32230.260111-0550.lt_release_svc_refresh_SERVER_EVAL_x64FRE_en-us.iso'
echo '7b052573ba7894c9924e3e87ba732ccd354d18cb75a883efa9b900ea125bfd51  windows-server-2025.iso' | sha256sum --check

curl --fail --location --retry 3 \
  --output virtio-win.iso \
  'https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/archive-virtio/virtio-win-0.1.302-1/virtio-win-0.1.302.iso'
echo '303f7ae40dad495d6ae474fdc571df58958a4dbc5c37a522d80f9a203867949d  virtio-win.iso' | sha256sum --check

export PKR_VAR_winrm_password='Aa1!'$(openssl rand -hex 20)
packer init .
packer fmt -check .
packer validate .
packer build .
unset PKR_VAR_winrm_password
```

`PKR_VAR_winrm_password` is a temporary build credential. Keep it out of shell tracing and logs. The build disables WinRM and removes the temporary `packer` account and unattended provisioning files before shutdown.

The output is `output/runnerfoundry-windows.qcow2`.

## Install and configure the image

Install the image and domain template at stable paths:

```sh
sudo install -D -m 0644 output/runnerfoundry-windows.qcow2 \
  /var/lib/runnerfoundry/images/runnerfoundry-windows.qcow2
sudo install -D -m 0644 domain.xml /etc/runnerfoundry/windows-domain.xml
```

Complete the [installation and GitHub App setup](../../docs/installation.md), then start from a complete daemon configuration such as the [quick-start configuration](../../README.md#2-configure-a-runner-and-its-pool).
Before the configuration has first run, you can replace its `scale_sets` list with the following list; keep its `github` and `state` settings.
For an existing deployment, add only the `- name: windows-vm` list entry beneath its existing `scale_sets:` key, then reload and try the first job below.
If you want to remove an existing pool afterwards, follow [pool retirement](../../docs/operations.md#retire-a-pool-or-app) so its remote registrations are handled deliberately.
Do not add a second top-level `scale_sets` key.

```yaml
scale_sets:
  - name: windows-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/windows-domain.xml
```

After saving the configuration, [reload RunnerFoundry](../../docs/operations.md#reload-configuration), or start it if this is a new installation.

The sample XML expects an active libvirt network named `default` with guest access to GitHub. The libvirt QEMU process must be able to traverse the image directory and read the qcow2 image under the host's discretionary access control, access control lists, SELinux or AppArmor policy. Access by the `runnerfoundry` service account alone does not establish QEMU access.

Do not replace the backing image while domains are using it. Each domain uses a transient disk, so guest writes are discarded when the domain ends and the backing image remains operator-owned.

## Run a first job

Confirm that the `windows-vm` Runner Pool is available in GitHub. Follow the [complete first-job workflow instructions](../../README.md#4-run-your-first-job), replacing that workflow's `jobs` block with:

```yaml
jobs:
  smoke-test:
    runs-on: windows-vm
    steps:
      - shell: powershell
        run: Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion
```

The guest starts one JIT-configured runner and powers off after the job exits. The launcher limits a job to six hours; on timeout it terminates the runner process tree, removes runtime state and powers off the guest. Use the [libvirt guest contract](../../docs/providers.md#libvirt-guest-contract) to diagnose a guest that starts without accepting a job.

## Update and rebuild the image

Follow [Update runner images](../../docs/providers.md#update-runner-images) to review upstream releases, update each affected version and checksum together, rebuild the image, and replace the installed backing image only after existing domains have stopped.

The pins are maintained in these locations:

- `windows.pkr.hcl` contains the Windows ISO checksum and the GitHub Actions runner version and checksum.
- This README contains the exact Windows and VirtIO download URLs and their checksums.
- `../../.github/workflows/packer-windows-example.yml` contains the same Windows and VirtIO URLs and checksums used by maintainer CI.

[GitHub publishes runner asset checksums in each runner release](https://github.com/actions/runner/releases). The pinned checksum must match the Windows x64 archive named by `github_runner_version`. For VirtIO, resolve the [upstream stable download](https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso) to its versioned archive instead of using the moving URL in a build, then compute and review the exact artifact's SHA-256 checksum. Keep the README and maintainer workflow identical.

## Maintainer validation

Maintainers changing the launcher can run its hermetic PowerShell 7 check separately from the operator build and first-job path:

```sh
pwsh -NoProfile -File runnerfoundry-jit-launcher.Tests.ps1
```
