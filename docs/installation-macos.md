# Install RunnerFoundry on macOS

Use the Apple Silicon installer on an ARM64 Mac running macOS 13 or later. The package contains the RunnerFoundry control plane.
Choose a runner provider after installation: Orchard for macOS jobs, Docker Compose for Linux containers, or an accessible libvirt connection.
Provider tools have their own host requirements; Orchard macOS workers require macOS 14 or later.

You need administrator access to install the package. Run RunnerFoundry afterwards as the user whose credentials and provider
tools it should use. A background service and a dedicated service account are not installed automatically.

## Install the package

Download `runnerfoundry_<version>_darwin_arm64.pkg` from [RunnerFoundry releases](https://github.com/stateshifters/runnerfoundry/releases).
The release installer is signed with Developer ID and notarized by Apple. From the download directory, install the exact file:

```sh
sudo installer -pkg ./runnerfoundry_<version>_darwin_arm64.pkg -target /
/usr/local/bin/runnerfoundry --version
```

Replace `<version>` with the downloaded release version. Intel Macs are not supported by this installer.
The command is installed at `/usr/local/bin/runnerfoundry`. Licence, source-location, third-party notices, SBOM metadata and
an example configuration are installed under `/usr/local/share/runnerfoundry`.

An upgrade replaces those packaged files. Keep your working configuration, private key and database in a separate directory.
The installer does not start RunnerFoundry.

## Prepare configuration and provider access

Create a private working directory and copy the example configuration:

```sh
mkdir -p "$HOME/Library/Application Support/RunnerFoundry"
chmod 0700 "$HOME/Library/Application Support/RunnerFoundry"
cd "$HOME/Library/Application Support/RunnerFoundry"
cp /usr/local/share/runnerfoundry/config.example.yaml config.yaml
chmod 0600 config.yaml
```

Copy the example only for a new installation; retain your existing `config.yaml` when upgrading.

Follow [GitHub App setup](installation.md#create-the-github-app) to register and install your App.
Save its private key as `github-app.pem` in this working directory, restrict it with `chmod 0600 github-app.pem`,
and set its numeric App ID in `config.yaml`.

The package's starter configuration uses `./github-app.pem` and `./state.db`, so keep this working directory when starting RunnerFoundry.
Add at least one Runner Pool under `scale_sets` before starting. See [configuration](configuration.md) for exact settings.

Prepare your provider and add its pool using the matching guide:

- [Orchard](providers/orchard.md) for macOS guests on Apple Silicon workers running macOS 14 or later
- [Docker Compose](providers/docker.md) for Linux container jobs, with the Docker CLI and Compose plugin available to your user
- [libvirt](providers/libvirt.md) for a reachable libvirt connection and compatible guest images

The package supplies the control plane. Install provider tools and prepare runner images separately, under the user who will run RunnerFoundry.

## Start RunnerFoundry

From the working directory containing your completed configuration:

```sh
/usr/local/bin/runnerfoundry --config ./config.yaml
```

The process runs in the foreground and writes its text logs to the terminal with the example logging settings.
Confirm GitHub discovery and Runner Pool registration in those logs before submitting work.
Use the configured pool name as the workflow's `runs-on` value; the provider guide includes a first-job check.

If startup fails, read the terminal error and check the App ID, private-key permissions, configuration paths and provider access
from this same working directory. See [configuration](configuration.md) for the supported settings.
Press Control-C to request shutdown, and keep the terminal open until the command returns.
The default shutdown policy leaves accepted runners running. See the
[daemon options](configuration.md#run-the-daemon) before choosing `--active-runner-policy=terminate`, which destroys affected active runners.
