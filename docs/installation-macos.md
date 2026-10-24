# Install RunnerFoundry on macOS

Use the Apple Silicon installer on an ARM64 Mac running macOS 12 or later. The package contains the RunnerFoundry control plane.
Its current execution providers remain Docker Compose and libvirt; installing this package does not add a Tart provider.

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

For GitHub, [register and install a GitHub App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)
on your personal account. Disable webhooks, grant repository Administration read and write permission, and install it on your
chosen repositories. Save its private key as `github-app.pem` in this working directory, restrict it with `chmod 0600 github-app.pem`,
and set its numeric App ID in `config.yaml`.

The example uses `./github-app.pem` and `./state.db`, so keep this working directory when starting RunnerFoundry.
Add at least one Runner Pool under `scale_sets` before starting. See [configuration](configuration.md) for exact settings.

For Docker Compose capacity, install Docker Desktop or another Docker environment that supplies the Docker CLI and Compose plugin.
Start that environment and check that `docker info` and `docker compose version` work as the same user who will run RunnerFoundry.
Those containers run Linux jobs. The macOS package does not include Docker, libvirt, runner images or GitHub credentials.

Save this as `compose.yaml` in your working directory:

```yaml
services:
  runner:
    image: ghcr.io/actions/actions-runner:latest
    user: runner
    working_dir: /home/runner
    command: ["/home/runner/run.sh"]
```

Replace `scale_sets: []` in `config.yaml` with:

```yaml
scale_sets:
  - name: linux-docker
    max_runners: 2
    docker:
      compose_file: ./compose.yaml
      service: runner
```

Check the Compose model as the user who will run RunnerFoundry:

```sh
docker compose -f ./compose.yaml config --services
```

It must list `runner`. The official image supports Linux ARM64, so Docker can use it natively on Apple Silicon.
This is a minimal evaluation image; follow [runner-image maintenance](providers.md#update-runner-images) to add workflow tools
and pin a reviewed image before maintained use. The [provider guide](providers.md) defines the image and bootstrap contracts.

## Start RunnerFoundry

From the working directory containing your completed configuration:

```sh
/usr/local/bin/runnerfoundry --config ./config.yaml
```

The process runs in the foreground and writes its text logs to the terminal with the example logging settings.
Confirm GitHub discovery and Runner Pool registration in those logs before submitting work. A workflow can target this example
pool with `runs-on: linux-docker`; it will execute inside a Linux container, not on the Mac host.

If startup fails, read the terminal error and check the App ID, private-key permissions, configuration paths and Docker access
from this same working directory. See [configuration](configuration.md) for the supported settings.
Press Control-C to request shutdown, and keep the terminal open until the command returns.
The default shutdown policy leaves accepted runners running. See the
[daemon options](configuration.md#run-the-daemon) before choosing `--active-runner-policy=terminate`, which destroys affected active runners.
