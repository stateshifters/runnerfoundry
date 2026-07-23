# Optional Ubuntu Packer example

RunnerFoundry does not require Packer and does not build virtual-machine images. Operators may create the guest image and domain XML with any tooling that satisfies the [libvirt guest contract](../../README.md#prepare-the-guest-image).

This optional example shows one way to build that operator-owned image with Packer. It produces an Ubuntu 26.04 LTS guest that starts one RunnerFoundry JIT runner and powers off when it exits. It deliberately contains no workflow-specific tools.

The build host needs Packer, QEMU/KVM, OVMF, an ISO creation tool such as `xorriso`, an SSH client, and outbound access to Ubuntu images, GitHub releases, Packer plugins, and Ubuntu package repositories.

From the repository root, enter the example directory, generate a temporary build key, and build the image:

```sh
cd examples/libvirt-runner
ssh-keygen -q -t ed25519 -N '' -f packer-key
packer init .
packer build -var "ssh_private_key_file=$PWD/packer-key" .
rm -f packer-key packer-key.pub
```

Install the image and domain template at the paths used by `domain.xml`:

```sh
sudo install -D -m 0644 output/runnerfoundry-ubuntu.qcow2 \
  /var/lib/runnerfoundry/images/runnerfoundry-ubuntu.qcow2
sudo install -D -m 0644 domain.xml /etc/runnerfoundry/domain.xml
```

Configure the scale set to use the template:

```yaml
scale_sets:
  - name: linux-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/domain.xml
```

The example XML uses EFI firmware auto-selection, a transient qcow2 disk, and an active libvirt network named `default`. It does not prescribe a libvirt/QEMU version: the target host must support those XML features. Before starting RunnerFoundry, check the files and the same libvirt connection configured above:

```sh
sudo -u runnerfoundry test -r /etc/runnerfoundry/domain.xml
sudo -u runnerfoundry virsh --connect qemu:///system net-info default
sudo -u runnerfoundry virsh --connect qemu:///system create \
  /etc/runnerfoundry/domain.xml --validate
```

The final command is a host-compatibility smoke test: it verifies the service identity, XML, firmware, network, and QEMU access to the image by starting an unassigned guest. With no JIT value, the example guest powers off and the transient domain disappears. If it does not, inspect its console or libvirt logs, then stop that exact test domain with `sudo -u runnerfoundry virsh --connect qemu:///system destroy runnerfoundry-template`. Host DAC, ACL, SELinux, or AppArmor policy must allow the libvirt QEMU process to traverse the image directory and read the qcow2 file; readability by the `runnerfoundry` service account alone is not sufficient.

The guest reads `runnerfoundry.jit=` from SMBIOS type 11, runs the unregistered runner once, removes its runtime directory, and powers off. The transient disk discards guest writes when the domain ends; the backing image remains operator-owned and must not be replaced while domains are using it. Add any tools required by your workflows to `provision.sh`.
