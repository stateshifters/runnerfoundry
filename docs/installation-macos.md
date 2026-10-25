# Install RunnerFoundry on macOS

Use the Apple Silicon installer on an ARM64 Mac running macOS 13 or later.
The package installs the RunnerFoundry control plane as a system service.
Choose a runner provider after installation: Orchard for macOS jobs, Docker Compose for Linux containers,
or an accessible libvirt connection. Provider tools have their own host requirements;
Orchard macOS workers require macOS 14 or later.

You need administrator access to install the package and manage the service.
The service runs as the nonlogin `_runnerfoundry` account.
Install any provider tools and grant provider access for that account before starting it.
The package does not start or enable the service.

## Install the package

Download `runnerfoundry_<version>_darwin_arm64.pkg` from
[RunnerFoundry releases](https://github.com/stateshifters/runnerfoundry/releases).
The release installer is signed with Developer ID and notarized by Apple.
From the download directory, install the exact file:

```sh
sudo installer -pkg ./runnerfoundry_<version>_darwin_arm64.pkg -target /
/usr/local/bin/runnerfoundry --version
```

Replace `<version>` with the downloaded release version. Intel Macs are not supported by this installer.
The command is installed at `/usr/local/bin/runnerfoundry`.
Licence, source-location, third-party notices, SBOM metadata and an example configuration
are installed under `/usr/local/share/runnerfoundry`.

The package creates `/usr/local/etc/runnerfoundry/config.yaml` from the example when no configuration exists.
For a new file, it sets owner `root`, group `_runnerfoundry` and mode `0640` so the service can read it.
The package also prepares `/Library/Application Support/RunnerFoundry` for state and
`/var/log/runnerfoundry` for service output.
It installs a launchd job at `/Library/LaunchDaemons/com.stateshifterlabs.runnerfoundry.plist`
but leaves it disabled.

## Prepare configuration and provider access

Follow [GitHub App setup](installation.md#create-the-github-app) to register and install your App.
From the directory containing the downloaded private key, install it so the service account can read it:

```sh
sudo install -m 0640 -o root -g _runnerfoundry github-app.pem /usr/local/etc/runnerfoundry/github-app.pem
sudo -u _runnerfoundry test -r /usr/local/etc/runnerfoundry/github-app.pem
```

Replace the first filename with your downloaded filename. The access check succeeds without output.
Keep the key out of source control, workflow files and logs.

Edit `/usr/local/etc/runnerfoundry/config.yaml` with `sudoedit`.
Set your numeric App ID and add at least one Runner Pool under `scale_sets`.
The starter configuration points to `/usr/local/etc/runnerfoundry/github-app.pem` and
`/Library/Application Support/RunnerFoundry/state.db`.
See [configuration](configuration.md) for the supported settings and validation rules.

Prepare your provider and add its pool using the matching guide:

- [Orchard](providers/orchard.md) for a documented system-service setup with macOS guests on Apple Silicon workers running macOS 14 or later
- [Docker Compose](providers/docker.md) for Linux container jobs using the documented foreground setup
- [libvirt](providers/libvirt.md) for a reachable libvirt connection using the documented foreground setup

The service account must be able to read provider credentials and reach the provider endpoint.
The Docker Compose and libvirt macOS guides use your login account and its provider context.
Follow [foreground setup](#run-in-the-foreground-instead) for those procedures.
To use either provider with the system service, separately arrange noninteractive tools, endpoint access
and credentials for `_runnerfoundry`; access configured for your login account does not transfer to it.
Use absolute paths for service configuration and check provider access as the service account before starting.
The package supplies the control plane; prepare provider tools and runner images separately.

## Start and check the service

After configuration and provider access are ready, explicitly enable the service for future boots and load it now:

```sh
sudo launchctl enable system/com.stateshifterlabs.runnerfoundry
sudo launchctl bootstrap system /Library/LaunchDaemons/com.stateshifterlabs.runnerfoundry.plist
sudo launchctl print system/com.stateshifterlabs.runnerfoundry
```

Check the printed service state and process identifier.
The service writes standard output to `/var/log/runnerfoundry/stdout.log` and
standard error to `/var/log/runnerfoundry/stderr.log`.
Read recent output with:

```sh
sudo tail -n 100 /var/log/runnerfoundry/stdout.log /var/log/runnerfoundry/stderr.log
```

Check for startup errors, GitHub discovery and Runner Pool registration before submitting work.
Use the configured pool name as the workflow's `runs-on` value;
the provider guide includes a first-job check.

If startup fails, check the service output, App ID, key permissions,
configuration paths and provider access as `_runnerfoundry`.
Restart the service after editing configuration so it reads the changed file.

## Stop, disable or restart the service

To stop the current process without changing whether it may start at the next boot:

```sh
sudo launchctl bootout system/com.stateshifterlabs.runnerfoundry
```

To keep it stopped across boots, stop it and then disable it:

```sh
sudo launchctl bootout system/com.stateshifterlabs.runnerfoundry
sudo launchctl disable system/com.stateshifterlabs.runnerfoundry
```

To restart an enabled service after a configuration or package change, stop it and load it again:

```sh
sudo launchctl bootout system/com.stateshifterlabs.runnerfoundry
sudo launchctl bootstrap system /Library/LaunchDaemons/com.stateshifterlabs.runnerfoundry.plist
```

Check `sudo launchctl print system/com.stateshifterlabs.runnerfoundry` after loading it.
Use `sudo launchctl print-disabled system` to inspect the saved enabled or disabled setting.
Stopping RunnerFoundry requests shutdown; the default shutdown policy leaves accepted runners running.
See the [daemon options](configuration.md#run-the-daemon) before choosing
`--active-runner-policy=terminate`, which destroys affected active runners.

## Upgrade the package

Install the newer package with the same `sudo installer` command.
The installer keeps the existing configuration, private key, state and launchd enabled or disabled setting.
It does not start or restart the service. If the service was running, its current process keeps running
until you restart it with `bootout` and `bootstrap`.
Plan that restart when you are ready to use the new binary and configuration.

## Run in the foreground instead

You can run RunnerFoundry under your own login account for a manual session.
Create a private working directory and copy the example configuration for a new setup:

```sh
mkdir -p "$HOME/Library/Application Support/RunnerFoundry"
chmod 0700 "$HOME/Library/Application Support/RunnerFoundry"
cd "$HOME/Library/Application Support/RunnerFoundry"
cp /usr/local/share/runnerfoundry/config.example.yaml config.yaml
chmod 0600 config.yaml
```

Save the GitHub App private key there as `github-app.pem` with mode `0600`.
In `config.yaml`, set the App ID, add a Runner Pool, set `private_key_file: ./github-app.pem`,
and set `state.path: ./state.db`. Start from this directory:

```sh
/usr/local/bin/runnerfoundry --config ./config.yaml
```

Logs appear in the terminal with the example settings. Press Control-C to request shutdown,
and keep the terminal open until the command returns. Your login account needs access to the selected provider.
Keep this configuration and database separate from the system service's files.
