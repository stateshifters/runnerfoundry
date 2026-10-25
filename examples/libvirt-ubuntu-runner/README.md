# Run the Ubuntu libvirt example

Prepare the daemon, libvirt connection, storage permissions, and network using the [libvirt guide](../../docs/providers/libvirt.md).
This build needs AMD64 Linux with KVM, QEMU, Packer 1.14.0 or later, `xorriso`, OpenSSH tools, and outbound access to Ubuntu, GitHub, and Packer plugins.
The template uses `/usr/share/OVMF/OVMF_CODE_4M.fd` and `/usr/share/OVMF/OVMF_VARS_4M.fd`; adjust them for your host if needed.

From the repository root, build the image:

```sh
cd examples/libvirt-ubuntu-runner
ssh-keygen -q -t ed25519 -N '' -f packer-key
packer init .
packer build -var "ssh_private_key_file=$PWD/packer-key" .
rm -f packer-key packer-key.pub
```

Remove both `packer-key` files after a failed build too. Install the generated image and template:

```sh
sudo install -D -m 0644 output/runnerfoundry-ubuntu.qcow2 \
  /var/lib/runnerfoundry/images/runnerfoundry-ubuntu.qcow2
sudo install -D -m 0644 domain.xml /etc/runnerfoundry/domain.xml
```

Add this pool to your existing configuration, keeping its `github` and `state` settings:

```yaml
scale_sets:
  - name: linux-vm
    runner_group: default
    max_runners: 2
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: /etc/runnerfoundry/domain.xml
```

[Start RunnerFoundry](../../README.md#3-start-runnerfoundry) for a new installation,
or [reload the running service](../../docs/operations.md#reload-configuration).
Run the [first-job workflow](../../README.md#4-run-your-first-job) with `runs-on: linux-vm` and `run: uname -a`.
Confirm that the job completes and the transient domain disappears.
