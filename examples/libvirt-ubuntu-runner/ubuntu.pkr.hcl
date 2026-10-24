packer {
  required_version = ">= 1.14.0"

  required_plugins {
    qemu = {
      source  = "github.com/hashicorp/qemu"
      version = "= 1.1.5"
    }
  }
}

variable "ssh_private_key_file" {
  type        = string
  description = "Private SSH key used by Packer. Its public key must exist at the same path with a .pub suffix."
  sensitive   = true
}

locals {
  github_runner_version = "2.338.0"
  github_runner_sha256  = "af4b794c1bc41d73d40535e3fe092a39f9679cd8d965954c2aca25a05ca41d32"
  ubuntu_image_url      = "https://cloud-images.ubuntu.com/releases/26.04/release-20260720/ubuntu-26.04-server-cloudimg-amd64.img"
  ubuntu_image_sha256   = "117816726abbdefc5ef3e38902e81a76f1c76c3610e709999d0885f9d5d9b477"
}

source "qemu" "ubuntu" {
  accelerator       = "kvm"
  cpus              = 2
  disable_vnc       = true
  disk_image        = true
  disk_interface    = "virtio"
  disk_size         = "20G"
  efi_boot          = true
  efi_drop_efivars  = true
  efi_firmware_code = "/usr/share/OVMF/OVMF_CODE_4M.fd"
  efi_firmware_vars = "/usr/share/OVMF/OVMF_VARS_4M.fd"
  format            = "qcow2"
  headless          = true
  iso_checksum      = "sha256:${local.ubuntu_image_sha256}"
  iso_url           = local.ubuntu_image_url
  machine_type      = "q35"
  memory            = 2048
  net_device        = "virtio-net"
  output_directory  = "${path.root}/output"
  vm_name           = "runnerfoundry-ubuntu.qcow2"

  cd_content = {
    "meta-data" = "instance-id: runnerfoundry-packer\nlocal-hostname: runnerfoundry-packer\n"
    "user-data" = <<-EOF
      #cloud-config
      ssh_pwauth: false
      disable_root: true
      users:
        - name: packer
          groups: [adm, sudo]
          lock_passwd: true
          shell: /bin/bash
          sudo: ALL=(ALL) NOPASSWD:ALL
          ssh_authorized_keys:
            - ${trimspace(file("${var.ssh_private_key_file}.pub"))}
      EOF
  }
  cd_label = "cidata"

  communicator              = "ssh"
  ssh_clear_authorized_keys = true
  ssh_private_key_file      = var.ssh_private_key_file
  ssh_timeout               = "30m"
  ssh_username              = "packer"

  shutdown_command = "sudo shutdown -P now"
  shutdown_timeout = "10m"
}

build {
  sources = ["source.qemu.ubuntu"]

  provisioner "file" {
    source      = "${path.root}/runnerfoundry-jit-launcher"
    destination = "/tmp/runnerfoundry-jit-launcher"
  }

  provisioner "file" {
    source      = "${path.root}/runnerfoundry-jit-launcher.service"
    destination = "/tmp/runnerfoundry-jit-launcher.service"
  }

  provisioner "shell" {
    environment_vars = [
      "GITHUB_RUNNER_SHA256=${local.github_runner_sha256}",
      "GITHUB_RUNNER_VERSION=${local.github_runner_version}",
    ]
    execute_command = "sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "${path.root}/provision.sh"
  }
}
