packer {
  required_version = "= 1.15.4"

  required_plugins {
    qemu = {
      source  = "github.com/hashicorp/qemu"
      version = "= 1.1.5"
    }
  }
}

variable "winrm_password" {
  type        = string
  description = "Temporary password for the Packer WinRM account."
  sensitive   = true
}

variable "windows_iso_path" {
  type        = string
  description = "Path to the pinned Windows Server 2025 evaluation ISO."
  default     = "windows-server-2025.iso"
}

variable "virtio_iso_path" {
  type        = string
  description = "Path to the pinned VirtIO driver ISO."
  default     = "virtio-win.iso"
}

locals {
  github_runner_version = "2.336.0"
  github_runner_sha256  = "d59123a43003e357b0805b5d0f611d0bd2f65ab67d51bd070dd4e7a0f685c162"
  output_directory      = abspath("${path.root}/output")
  virtio_iso_path       = abspath("${path.root}/${var.virtio_iso_path}")
  windows_iso_path      = abspath("${path.root}/${var.windows_iso_path}")
  windows_iso_sha256    = "7b052573ba7894c9924e3e87ba732ccd354d18cb75a883efa9b900ea125bfd51"
}

source "qemu" "windows" {
  accelerator       = "kvm"
  boot_command      = ["<spacebar><wait5>"]
  boot_wait         = "5s"
  cdrom_interface   = "ide"
  cpus              = 2
  disk_interface    = "virtio"
  disk_size         = "64G"
  efi_boot          = true
  efi_drop_efivars  = true
  efi_firmware_code = "/usr/share/OVMF/OVMF_CODE_4M.fd"
  efi_firmware_vars = "/usr/share/OVMF/OVMF_VARS_4M.fd"
  format            = "qcow2"
  headless          = true
  iso_checksum      = "sha256:${local.windows_iso_sha256}"
  iso_url           = local.windows_iso_path
  machine_type      = "q35"
  memory            = 4096
  net_device        = "virtio-net"
  output_directory  = local.output_directory
  shutdown_command  = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:/Windows/Temp/runnerfoundry-seal.ps1"
  shutdown_timeout  = "15m"
  vm_name           = "runnerfoundry-windows.qcow2"

  floppy_content = {
    "Autounattend.xml" = templatefile(abspath("${path.root}/Autounattend.xml"), {
      winrm_password = var.winrm_password
    })
    "enable-winrm.ps1" = file(abspath("${path.root}/enable-winrm.ps1"))
  }

  qemuargs = [
    ["-boot", "once=d,order=c"],
    ["-cpu", "host"],
    ["-drive", "file=/usr/share/OVMF/OVMF_CODE_4M.fd,if=pflash,unit=0,format=raw,readonly=on"],
    ["-drive", "file=${local.output_directory}/efivars.fd,if=pflash,unit=1,format=raw"],
    ["-drive", "file=${local.output_directory}/{{ .Name }},if=virtio,cache=writeback,discard=unmap,format=qcow2,index=2"],
    ["-drive", "file=${local.windows_iso_path},media=cdrom,index=1"],
    ["-drive", "file=${local.virtio_iso_path},media=cdrom,index=3"],
    ["-vga", "std"],
  ]

  communicator   = "winrm"
  winrm_insecure = true
  winrm_password = var.winrm_password
  winrm_timeout  = "3h"
  winrm_use_ssl  = true
  winrm_username = "packer"
}

build {
  sources = ["source.qemu.windows"]

  provisioner "file" {
    source      = "${path.root}/runnerfoundry-jit-launcher.ps1"
    destination = "C:/Windows/Temp/runnerfoundry-jit-launcher.ps1"
  }

  provisioner "file" {
    source      = "${path.root}/seal.ps1"
    destination = "C:/Windows/Temp/runnerfoundry-seal.ps1"
  }

  provisioner "powershell" {
    environment_vars = [
      "GITHUB_RUNNER_SHA256=${local.github_runner_sha256}",
      "GITHUB_RUNNER_VERSION=${local.github_runner_version}",
    ]
    script = "${path.root}/provision.ps1"
  }
}
