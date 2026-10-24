# Run the Windows libvirt example

Prepare the daemon, libvirt connection, storage permissions, and network using the [libvirt guide](../../docs/providers/libvirt.md).
This build needs AMD64 Linux with KVM, QEMU, OVMF at `/usr/share/ovmf/OVMF.fd`, Packer 1.15.4, `curl`, `openssl`, `sha256sum`,
outbound downloads, and space for the media plus a 64 GiB image.

From the repository root, download the pinned media and build:

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

The build uses a temporary WinRM credential. Unset it after a failed build too, and keep shell tracing off.
The output is `output/runnerfoundry-windows.qcow2`. Install it and the domain template:

```sh
sudo install -D -m 0644 output/runnerfoundry-windows.qcow2 \
  /var/lib/runnerfoundry/images/runnerfoundry-windows.qcow2
sudo install -D -m 0644 domain.xml /etc/runnerfoundry/windows-domain.xml
```

Add this pool to your existing configuration, keeping its `github` and `state` settings:

```yaml
scale_sets:
  - name: windows-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/windows-domain.xml
```

[Start RunnerFoundry](../../README.md#3-start-runnerfoundry) for a new installation,
or [reload the running service](../../docs/operations.md#reload-configuration).
Use the [first-job workflow](../../README.md#4-run-your-first-job), replacing its job with:

```yaml
jobs:
  smoke-test:
    runs-on: windows-vm
    steps:
      - shell: powershell
        run: Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion
```

Confirm that the job completes and the transient domain disappears.
