# libvirt runners

Use libvirt for workflows needing a full Linux or Windows VM, VM-specific hardware, firmware, or isolation.
The macOS steps below use a login account and the [foreground process](../installation-macos.md#run-in-the-foreground-instead).

## Install and prepare the host

Install RunnerFoundry using the [Linux package](../installation.md#install-the-package) or [macOS installer](../installation-macos.md),
then [create the GitHub App](../installation.md#create-the-github-app).
Install the host's libvirt/QEMU packages and `virsh`, enable its virtualization service, and grant the daemon account access to the selected connection.
For Ubuntu, follow the [host libvirt installation instructions](https://ubuntu.com/server/docs/how-to/virtualisation/libvirt/).
The image-building examples use AMD64 Linux, QEMU/KVM, EFI firmware, and an active `default` libvirt network with guest access to GitHub.

On a Linux package host, check the connection as the daemon account, substituting your URI:

```sh
sudo -u runnerfoundry virsh --connect qemu:///system uri
sudo -u runnerfoundry virsh --connect qemu:///system list --all
sudo -u runnerfoundry virsh --connect qemu:///system net-list --all
```

On macOS, run the `virsh` commands as the user who runs RunnerFoundry, using the configured reachable connection URI.

The connection and list commands must succeed. Enable or select the network and firmware your domain template requires before sending work.

## Configure the provider

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

## Prepare guest images

The [Ubuntu](../../examples/libvirt-ubuntu-runner/README.md) and [Windows Server](../../examples/libvirt-windows-runner/README.md) working examples
contain their own dependencies, pinned media, build commands, installation paths, and first-job steps.
Packer is optional image-building tooling, not a RunnerFoundry runtime dependency.
Use them when building those exact examples. For another image, install an unregistered runner plus the startup behavior in the guest contract above,
and provide every workflow shell, tool, certificate, and OS update it needs.

The Ubuntu example uses Ubuntu 26.04 LTS and a root systemd launcher that starts the runner as `runner`.
It grants that guest account passwordless sudo. Its provisioner is not a complete hardening or credential-removal policy.
The Windows example uses Windows Server 2025 Standard Core and Windows PowerShell. It does not install PowerShell 7;
a job selecting `pwsh` needs a separate installation. Its startup task runs as SYSTEM and limits the runner process to six hours,
including time waiting for work, before terminating its process tree, cleaning runtime state, and powering off.

Windows evaluation media [expires after 180 days and requires online activation within 10 days](https://www.microsoft.com/en-us/evalcenter/download-windows-server-2025).
Use appropriately licensed media for longer-lived capacity.

## Isolate disks and identities

The sample XMLs use `<transient shareBacking="yes"/>` to discard writes while allowing concurrent guests to share an operator-owned base.
This [QEMU disk configuration](https://www.libvirt.org/formatdomain.html#hard-drives-floppy-disks-cdroms) requires libvirt 7.4 or later
and a configuration supporting disk hotplug. Libvirt can create temporary overlays beside the backing image.
Allow its QEMU/storage processes the necessary directory access and free space for those overlays, in addition to reading the base.
Use host-specific ACL, SELinux, or AppArmor policy rather than broadening all image-directory permissions.

Omit fixed UUID and MAC address values from templates used concurrently so libvirt can generate distinct identities.
RunnerFoundry replaces the domain name, ownership metadata, and bootstrap OEM value; it does not replace every identity or backing path.

## Start a job

After saving a complete configuration, [start RunnerFoundry](../../README.md#3-start-runnerfoundry) for a new Linux package installation,
or [reload the running service](../operations.md#reload-configuration).
For macOS, [start the installed command](../installation-macos.md#run-in-the-foreground-instead) from its working directory.
Startup validation checks XML rendering and the connection; it does not boot the image or prove firmware, storage, network, or guest readiness.
Follow the [first-job workflow](../../README.md#4-run-your-first-job) with your pool's `runs-on` value.
Use `uname -a` for the Ubuntu image, or this job for the Windows example:

```yaml
jobs:
  verify:
    runs-on: windows-vm
    steps:
      - shell: powershell
        run: Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion
```

Success means the job completes in GitHub, the guest shuts down, and its transient domain disappears.
The backing image remains. A running domain alone is not runner acceptance.

## Update images

1. Update the image's runner version and OS-specific SHA-256 together. Keep base OS and driver media URLs and hashes consistent.
2. Build a new image at a new path and prepare a domain XML template referencing it. Verify downloads before extraction or installation.
3. Keep the prior image and template available for active domains and rollback.
4. Change `domain_xml_template` if needed, then reload. Template-file changes also need reload because the daemon reads the template during preparation.
5. Run a small job, check its runner and tools, and confirm shutdown and cleanup.
6. Remove old backing images only after no domains use them.

The [shared image guidance](../providers.md#update-runner-images) identifies upstream releases.
Ubuntu pins its cloud image URL/hash, runner version/hash, and plugin in `ubuntu.pkr.hcl`.
Windows pins its runner and Windows ISO checksum in `windows.pkr.hcl`; its README and maintained build workflow contain the media URLs and hashes.
Keep those Windows media copies aligned when updating the working example.

Changing `connection_uri` changes the recovery endpoint. Drain and clean up old attempts before moving it, or retain access to the former endpoint for recovery.

## Troubleshoot connection access

Run `virsh ... list --all` as the daemon account. On a Linux package host where the `libvirt` group was created after RunnerFoundry installation:

```sh
sudo usermod -aG libvirt runnerfoundry
sudo systemctl restart runnerfoundry
sudo -u runnerfoundry virsh --connect qemu:///system list --all
```

Check [active runner policy](../operations.md#choose-active-runner-shutdown-behaviour) before restarting during work.
If access still fails, inspect the configured URI, authentication, socket permissions, and host policy.
QEMU's access to images and overlay directories is separate from the daemon account's access to libvirt.

## Troubleshoot startup and cleanup

For a template or create error, check the named XML path, firmware, backing image, transient-disk support, and network against the target host.
Use `virsh ... domstate <domain>` to inspect an affected domain's state.
Do not print or collect live `dumpxml` output: its SMBIOS OEM strings contain bootstrap data.

If the VM boots but does not accept work, check the guest startup launcher, unregistered runner template, SMBIOS type 11 support,
and connectivity to GitHub. Preserve [runner diagnostics outside the guest](../providers.md#security-and-diagnostic-logs) before it disappears.
A Windows job using `pwsh` will fail unless PowerShell 7 was installed; use `powershell` for the supplied Windows image.

Provider cleanup can forcibly power off a domain. Restore connection access when cleanup fails, and allow RunnerFoundry to retry cleanup of the owned resource.
The provider does not delete backing images or other operator storage.
For source registration and demand problems, use [queued-job diagnosis](../operations.md#jobs-stay-queued)
and [provisioning recovery](../operations.md#provisioning-attempts-keep-retrying).
