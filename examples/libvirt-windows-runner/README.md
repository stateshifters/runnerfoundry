# Minimal Windows libvirt runner image

This optional example produces a Windows Server 2025 Standard Core image that can execute one RunnerFoundry-assigned GitHub Actions job. The operator owns the image, domain XML, firmware, network, storage, workflow tools, and image lifecycle. RunnerFoundry does not run Packer or require this example. You may adapt it, replace it, or build a compatible image with another tool.

The example installs only the GitHub Actions runner and the startup launcher required by the [libvirt guest contract](../../README.md#prepare-the-guest-image). It does not install PowerShell 7, Docker, workflow tools, Windows updates, or production hardening.

## Understand the media boundary

The template uses the English Windows Server 2025 Standard Core evaluation image. Microsoft limits evaluation installations to 180 days and requires online activation within the first 10 days. Use appropriately licensed media for capacity that must operate beyond the evaluation terms.

The public media and checksums are pinned. The download commands and workflow stop on a checksum mismatch:

| Artifact | Version | SHA-256 |
| --- | --- | --- |
| Windows Server evaluation | Windows Server 2025, build 26100.32230 | `7b052573ba7894c9924e3e87ba732ccd354d18cb75a883efa9b900ea125bfd51` |
| VirtIO drivers | `0.1.266` | `57b0f6dc8dc92dc2ae8621f8b1bfbd8a873de9bedc788c4c4b305ea28acc77cd` |
| GitHub Actions runner for Windows x64 | `2.336.0` | `d59123a43003e357b0805b5d0f611d0bd2f65ab67d51bd070dd4e7a0f685c162` |

The media URLs are public, but their license terms still apply. A new upstream image or driver release requires an explicit URL and checksum update. The Microsoft download path is opaque and is served exactly as written; do not normalize its `f34g` segment as a hexadecimal identifier. Packer validates the Windows ISO itself. Verify the VirtIO ISO before extracting its drivers as shown below.

## Build the image

Build on an AMD64 Linux host with KVM acceleration, QEMU, OVMF, Packer 1.15.4, `curl`, `openssl`, `sha256sum`, `xorriso`, and enough space for both installation media and a 64 GiB image. Start in this directory:

```sh
cd examples/libvirt-windows-runner

curl --fail --location --retry 3 \
  --output windows-server-2025.iso \
  'https://software-static.download.prss.microsoft.com/dbazure/998969d5-f34g-4e03-ac9d-1f9786c66749/26100.32230.260111-0550.lt_release_svc_refresh_SERVER_EVAL_x64FRE_en-us.iso'
echo '7b052573ba7894c9924e3e87ba732ccd354d18cb75a883efa9b900ea125bfd51  windows-server-2025.iso' | sha256sum --check

curl --fail --location --retry 3 \
  --output virtio-win.iso \
  'https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/archive-virtio/virtio-win-0.1.266-1/virtio-win-0.1.266.iso'
echo '57b0f6dc8dc92dc2ae8621f8b1bfbd8a873de9bedc788c4c4b305ea28acc77cd  virtio-win.iso' | sha256sum --check

mkdir -p 'virtio-drivers/$WinPEDriver$'
xorriso -osirrox on -indev virtio-win.iso \
  -extract /viostor/2k25/amd64 'virtio-drivers/$WinPEDriver$/viostor' \
  -extract /NetKVM/2k25/amd64 'virtio-drivers/$WinPEDriver$/NetKVM'

export PKR_VAR_winrm_password='Aa1!'$(openssl rand -hex 20)
packer init .
packer fmt -check .
packer validate .
packer build .
unset PKR_VAR_winrm_password
```

`PKR_VAR_winrm_password` is a generated build credential. Keep it out of shell tracing and logs. The sealing script deletes the `packer` account, disables the built-in Administrator account and WinRM, and removes cached unattended files and temporary provisioning state.

Packer builds a 2-vCPU, 4 GiB, 64 GiB qcow2 image with Q35, Unified Extensible Firmware Interface (UEFI), and VirtIO storage and networking. It creates an auxiliary CD with the selected storage and network drivers under the Windows Setup `$WinPEDriver$` directory. Windows Server scans that directory on every accessible drive, so unattended setup does not depend on an optical drive letter. During the build, QEMU injects a harmless `runnerfoundry.packer=` SMBIOS value. Provisioning must read that value through the same raw `GetSystemFirmwareTable(RSMB)` path used at runtime before the image is sealed.

The output is `output/runnerfoundry-windows.qcow2`.

## Install the operator-owned artifacts

Install the image and XML where the `runnerfoundry` service configuration expects them:

```sh
sudo install -D -m 0644 output/runnerfoundry-windows.qcow2 \
  /var/lib/runnerfoundry/images/runnerfoundry-windows.qcow2
sudo install -D -m 0644 domain.xml /etc/runnerfoundry/windows-domain.xml
```

The sample XML uses the libvirt `default` network and a transient overlay backed by the installed image. QEMU must be able to read the image and traverse its parent directories under the host's discretionary access control, access control lists, SELinux, or AppArmor policy. RunnerFoundry never deletes this operator-owned storage.

## Configure RunnerFoundry

Point a libvirt infrastructure provider at the installed XML:

```yaml
scale_sets:
  - name: windows-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/windows-domain.xml
```

RunnerFoundry owns the rendered transient domain name, its `urn:runnerfoundry` metadata, capacity accounting, and the just-in-time (JIT) handoff. It adds `<smbios mode="sysinfo"/>` and the first relevant type 11 value in this form:

```text
runnerfoundry.jit=<encoded-jit-configuration>
```

The guest reads raw SMBIOS data without logging or persisting that value. Its `LocalSystem` startup task copies the clean, unregistered runner template into writable runtime storage and invokes `run.cmd --jitconfig` once. Because this required interface places the JIT value on a process command line, configure Windows process-creation auditing and endpoint telemetry so they do not record this task's command-line arguments. The launcher allows at most six hours. On timeout it requires `taskkill.exe /T /F` to succeed and verifies that the runner process exits before cleanup. It removes runtime and work state in `finally`, then attempts `Stop-Computer -Force`; if that fails, it runs `shutdown.exe /s /t 0 /f`.

## Smoke-test the domain

Confirm the network and XML are usable by the same libvirt connection RunnerFoundry will use:

```sh
sudo -u runnerfoundry test -r /etc/runnerfoundry/windows-domain.xml
sudo -u runnerfoundry virsh --connect qemu:///system net-info default
sudo -u runnerfoundry virsh --connect qemu:///system create \
  /etc/runnerfoundry/windows-domain.xml --validate
```

This direct smoke test does not contain a JIT value. The startup task should take the missing-JIT path, clean runtime state, and power off. Because the domain is transient, it then disappears. If it remains active, inspect the console and stop only this test domain:

```sh
sudo -u runnerfoundry virsh --connect qemu:///system destroy runnerfoundry-windows-template
```

## Choose the infrastructure provider

Use the Docker Compose infrastructure provider when a Linux container supplies the required workflow environment and an acceptable isolation boundary. Docker receives JIT configuration through `ACTIONS_RUNNER_INPUT_JITCONFIG` and removes the one-job container through its `--rm` lifecycle.

Use the libvirt infrastructure provider when a workflow needs Windows, a complete virtual machine, VM-specific firmware or storage behavior, or a VM boundary. The operator then owns more of the lifecycle: the guest image, XML, firmware, networking, storage access, launcher, cleanup, and shutdown behavior. Neither self-hosting nor ephemerality is a security guarantee; the operator still defines credentials, isolation, and host policy.

Packer, this Windows example, and the Ubuntu example are optional operator illustrations. Only the documented guest and domain contract is required.
