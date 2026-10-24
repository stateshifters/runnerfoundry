# Install RunnerFoundry

Use the Linux package instructions below for the Ubuntu 24.04 AMD64 [first-job tutorial](../README.md#quick-start),
or follow [native macOS installation](#install-on-macos) for the control plane.
The Linux package requires administrator access to the host. You need permission to create and install a GitHub App on your personal account.

## Prepare the host

The supplied Debian package targets Linux AMD64 and uses systemd 253 or newer for service startup and reload notifications.
Check your machine:

```sh
uname -m
systemctl --version
```

The architecture must be `x86_64`, and the second command must identify systemd 253 or newer.

For the Docker tutorial, install Docker Engine and the Compose plugin using the
[Docker provider setup](providers/docker.md#install-and-prepare-the-host). Docker hosts also need access to their container registry.
For libvirt or Orchard, prepare the provider using its [provider guide](providers.md).

The RunnerFoundry host needs outbound HTTPS access to GitHub. Runners need access to GitHub, workflow dependencies and services.
RunnerFoundry polls GitHub; you do not need an inbound webhook server.

## Install the package

Download the AMD64 `.deb` from [RunnerFoundry releases](https://github.com/stateshifters/runnerfoundry/releases).
In the download directory, install the exact file downloaded:

```sh
sudo apt install ./runnerfoundry_<version>_amd64.deb
runnerfoundry --version
```

Replace `<version>` with the release version. The version command prints `runnerfoundry` followed by the installed version.

The package creates a `runnerfoundry` service account, `/etc/runnerfoundry`, and `/var/lib/runnerfoundry`.
Its starter configuration needs credentials and at least one Runner Pool before the service can run.
Installation does not enable or start the service.

The account joins existing `docker` and `libvirt` groups during installation.
Confirm [Docker service-account access](providers/docker.md#install-and-prepare-the-host) before starting the Docker tutorial;
use the provider's [access troubleshooting](providers/docker.md#troubleshoot-docker-access) if denied.

## Install on macOS

Use the [signed Apple Silicon installer](installation-macos.md) for macOS 13 or later.
It installs `/usr/local/bin/runnerfoundry` and a disabled system service. Follow that guide to configure the service account, choose a provider and start the service.
Orchard macOS workers additionally require macOS 14 or later.

## Create the GitHub App

Open your personal account's Settings → Developer settings → GitHub Apps → New GitHub App.
Use GitHub's [registration instructions](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app) alongside these choices:

| Registration choice | Value |
| --- | --- |
| App name | A unique name you recognise, such as your account name followed by RunnerFoundry |
| Homepage URL | A valid project page, such as `https://github.com/stateshifters/runnerfoundry` |
| Webhook Active | Clear this checkbox; RunnerFoundry polls for work |
| Repository permissions → Administration | Read and write |
| Where can this GitHub App be installed? | Only on this account |

RunnerFoundry uses installation authentication. It does not require an OAuth callback, device flow, setup URL, or user-authorisation workflow.
Leave those optional features unconfigured. Save the App and record the numeric App ID shown in its settings.
Use this App ID in RunnerFoundry configuration, not the Client ID or installation ID.

Under Private keys, generate a key and download the PEM file. Transfer it securely to your host if you downloaded it elsewhere.
From the directory containing the downloaded key on a Linux package host, install it:

```sh
sudo install -m 0640 -o root -g runnerfoundry github-app.pem /etc/runnerfoundry/github-app.pem
sudo -u runnerfoundry test -r /etc/runnerfoundry/github-app.pem
```

Replace the first filename with your downloaded filename. The access check succeeds without output.
For macOS, follow [service configuration and key setup](installation-macos.md#prepare-configuration-and-provider-access).
Keep the private key out of source control, workflow files, and logs.

Choose Install App in GitHub and install it on your account. Start with Only select repositories and select a trusted test repository.
RunnerFoundry discovers repositories through the installation; you do not configure a repository list in YAML.
Every configured Runner Pool is made available to each discovered repository.

## Continue with your first job

For the Linux package, return to [configure a runner and its pool](../README.md#2-configure-a-runner-and-its-pool).
For macOS, continue with [package configuration and provider selection](installation-macos.md#prepare-configuration-and-provider-access).
For PostgreSQL or multiple hosts, use [deployment](deployment.md). For other virtual machines, use [libvirt](providers/libvirt.md).
