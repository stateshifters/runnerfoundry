# Operate RunnerFoundry

Use this guide to check health, change a running service, protect state, retire pools and diagnose common failures. Run service commands on each affected host.

## Check health and leadership

Check the systemd unit:

```sh
sudo systemctl status runnerfoundry
```

RunnerFoundry publishes a status containing health and leadership:

- `operational leadership=leader` means the member has no active reported failures and owns GitHub-facing leader work
- `operational leadership=standby` is a normal healthy state for a PostgreSQL cluster follower
- `degraded` means at least one observed operation is failing; unaffected pools and Apps may continue working
- `leadership=unknown` is an initial state before leadership has been observed

Health returns to `operational` when all tracked failures report recovery. Check logs to learn which operation failed. Process state alone does not prove that repository discovery, capacity publication or a provider is healthy.

## Find logs

Without `log.file`, RunnerFoundry writes application logs to standard output. The packaged service sends that output to the systemd journal:

```sh
sudo journalctl -u runnerfoundry -f
```

If `log.file` is configured, read that absolute file path instead. RunnerFoundry rotates it according to `max_size_mb` and `max_backups`. The journal still records systemd service events, but it is no longer the normal application-log destination.

Use `log.level: debug` only while you need more detail. Debug logs are more verbose. Reload the service after changing logging settings.

## Reload configuration

Reload after changing GitHub Apps, log settings, pruning or Runner Pool settings:

```sh
sudo systemctl reload runnerfoundry
```

RunnerFoundry prepares and validates the replacement before activation. The existing configuration continues running if replacement preparation fails. Confirm the result in the active log destination:

- `Runtime reload applied` confirms the replacement is active
- `Runtime reload rejected` means the reload encountered an error; nearby error fields identify the stage and cause

A rejection while loading, validating, building or preparing the replacement leaves the previous configuration active. A rejection after activation can instead report a notification, runner termination or retired-runtime cleanup failure. In that case, the replacement remains active. Review the error and current pools and resources rather than inferring the active configuration from `rejected` alone.

Every `state` setting requires a process restart. This includes backend selection, PostgreSQL endpoints, role and credential references. Restart after rotating the contents of a PostgreSQL credential file because existing database connections keep their startup credentials:

```sh
sudo systemctl restart runnerfoundry
sudo systemctl status runnerfoundry
```

GitHub App private keys are read while preparing a generation. Reload after replacing a configured GitHub private-key file.

## Stop and restart

Use systemd to stop or restart a member:

```sh
sudo systemctl stop runnerfoundry
sudo systemctl restart runnerfoundry
```

A member withdraws its published capacity during a graceful stop. Other live cluster members continue their own work. The active runner policy controls what happens to runner resources; check the policy before stopping a member with active work.

## Choose active runner shutdown behaviour

The daemon option `--active-runner-policy` accepts `leave` or `terminate`. The packaged unit uses the default, `leave`.

`leave` preserves active runner resources during shutdown and configuration replacement. This is the safest general setting for accepted jobs.

`terminate` destroys affected resources during an incompatible reload. Only pools removed from configuration or changed to a different provider kind are affected. Changing settings while keeping the same pool name and provider kind does not select that pool for termination. On service shutdown, `terminate` applies to all configured pools.

An unstarted job returns to its Runner Pool only after cleanup succeeds. A job already accepted by a runner stays in the GitHub lifecycle and is never turned into a provisioning retry. Check provider and GitHub logs before using `terminate` during active work.

To select `terminate` for the packaged service, add the option through a systemd override and restart the service. Keep `--config /etc/runnerfoundry/config.yaml` in the resulting `ExecStart` command.

## Retire a pool or App

Pruning is disabled by default. Removing a pool while `prune: false` preserves its remote GitHub runner scale sets and ownership evidence.

Before changing configuration, decide whether at least one member can remain live with the relevant GitHub App, `prune: true` and another real Runner Pool. That member is needed to lead discovery and pruning after the retiring pool has no live declarations.

To retire the pool:

1. Stop submitting work to the affected pool.
2. Wait for its outstanding GitHub jobs to finish, or cancel them in GitHub.
3. Set `prune: true` on every member which will remain live and reload them.
4. On each member which retains another real pool, remove the retiring pool and reload that member.
5. If the retiring pool is a member's only pool, stop that member instead. RunnerFoundry rejects an empty `scale_sets` list, so do not reload it with zero pools or add a dummy pool.
6. Wait until no live member declares the retired pool and a remaining leader completes successful GitHub discovery and reconciliation.
7. Confirm the pruning outcome in GitHub and the logs before removing the relevant App.

No member serves a pool after every declaration is removed. The default `leave` policy can preserve active runner resources, but it does not keep the removed pool available for new work.

RunnerFoundry prunes only when no live member declares the pool. It also requires a successful discovery of the repository and an exact match with stored ownership. A temporary discovery failure, ownership mismatch or pruning failure retains the remote scale set for a later safe retry.

If the whole deployment is stopped, or no suitable member remains live with the relevant App, RunnerFoundry cannot perform automatic pruning. Its remote scale sets and ownership evidence remain. Keep them until a correctly configured member can reconcile them, or ask for maintainer help before changing them outside RunnerFoundry.

Retire pools before removing their GitHub App. Removing an App closes its sessions and active routes, but deliberately keeps its ownership evidence and remote scale sets. Re-add that App to resume management, or remove the App and its resources through GitHub when you no longer want RunnerFoundry to manage them.

## Back up and upgrade SQLite

Before the upgrade, record `runnerfoundry --version` and keep the matching package alongside the backup. This gives you a known binary and state pair if you must restore.

Set `state_path` to the configured `state.path`. The default is shown below. Stop the service and confirm it is inactive before making a filesystem copy:

```sh
state_path=/var/lib/runnerfoundry/state.db
sudo systemctl stop runnerfoundry || exit 1
sudo test -f "$state_path" || { echo 'Check state_path before backing up' >&2; exit 1; }
if sudo systemctl is-active --quiet runnerfoundry; then
  echo 'RunnerFoundry is still active; do not copy the database' >&2
  exit 1
fi
backup_dir=/var/backups/runnerfoundry/$(date -u +%Y%m%dT%H%M%SZ)
sudo install -d -m 0750 -o root -g runnerfoundry "$backup_dir" || exit 1
for suffix in '' -journal -wal -shm; do
  state_file="${state_path}${suffix}"
  if sudo test -e "$state_file"; then
    sudo cp --archive "$state_file" "$backup_dir/" || exit 1
  fi
done
sudo ls -la "$backup_dir"
```

Keep the database and any journal, WAL and shared-memory sidecars together. SQLite documents the WAL as part of persistent database state when it exists. Copying a live database or separating it from its WAL can lose committed transactions or corrupt the copy. See SQLite's [WAL file guidance](https://www.sqlite.org/wal.html#the_wal_file) and [backup safety guidance](https://www.sqlite.org/howtocorrupt.html#_backup_or_restore_while_a_transaction_is_active).

Install the new package, then start and verify the service:

```sh
sudo apt install ./runnerfoundry_<version>_amd64.deb
sudo systemctl start runnerfoundry
sudo systemctl status runnerfoundry
sudo journalctl -u runnerfoundry --since '10 minutes ago'
```

Replace `<version>` with the downloaded package version. Startup upgrades supported SQLite schemas automatically. Check the [SQLite migration boundaries](migrations.md#sqlite-schema-transitions) before upgrading an older database.

If you must restore, stop RunnerFoundry first. Preserve the current database and all sidecars as a separate set, then restore the complete backed-up set together. Do not routinely delete journal, WAL or shared-memory files.

## Upgrade PostgreSQL

Use a controlled, stopped-cluster upgrade:

1. Record `runnerfoundry --version` on every host. From one host, also record migration status and keep the matching package:

   ```sh
   sudo -u runnerfoundry runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
   ```

2. Stop every RunnerFoundry member.
3. Take and verify a PostgreSQL backup using your database platform's supported backup and restore process.
4. Install the new RunnerFoundry package on every host without starting the services.
5. Inspect the schema from one host as the service account:

   ```sh
   sudo -u runnerfoundry runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
   ```

6. Apply forward migrations once:

   ```sh
   sudo -u runnerfoundry runnerfoundry migrate up --config /etc/runnerfoundry/config.yaml
   ```

7. Run `migrate status` again and confirm `current` equals `target`.
8. Restart every member and check its status and logs.

Migration commands must run as `runnerfoundry` because the configuration and credential files are protected for that service account. If you use another account, it needs equivalent read access to the configuration, every referenced credential file and the PostgreSQL endpoint.

The daemon also applies known forward migrations at startup. The explicit procedure above avoids letting the first restarted member choose the migration moment. See [PostgreSQL migrations and rollback](migrations.md#postgresql-schema-transitions) before changing schema versions.

## Troubleshooting

Start with the observable symptom. Take the safe action, confirm the result, then collect evidence if the problem remains.

### Service does not start

Read the failure:

```sh
sudo systemctl status runnerfoundry
sudo journalctl -u runnerfoundry -b
```

Correct the named configuration, credential, state or provider error. RunnerFoundry rejects unknown YAML fields, unreadable App keys, invalid state selection, duplicate pool names, non-positive capacity and invalid provider settings.

Restart the service and confirm it reports ready. If it still fails, collect the redacted startup error for [support](#request-support).

### Jobs stay queued

Confirm that the workflow's `runs-on` value exactly matches the Runner Pool name. Then check service health, available capacity and provider access.

Use the provider's diagnostic checks in the same account and endpoint context as the daemon:

- [Docker service and container checks](providers/docker.md#troubleshoot-queued-jobs-and-failed-attempts)
- [libvirt connection and guest checks](providers/libvirt.md#troubleshoot-startup-and-cleanup)
- [Orchard guest checks](providers/orchard.md#diagnose-a-queued-job)

Logs should show discovery, capacity publication, and reconciliation without a continuing failure.
If capacity is available but work stays queued, collect the pool name, repository, and redacted operational events for [support](#request-support).

### Runner Pool does not appear in GitHub

Check that:

- the GitHub App is installed on the repository
- the App has repository Administration read and write permission
- the configured App ID belongs to the App which issued the private key
- the private-key file is readable by `runnerfoundry`
- the configured GitHub runner group exists
- the member has outbound HTTPS access to GitHub

Compare the App settings with the [GitHub configuration](configuration.md#github-configuration), then inspect discovery and registration events in the configured log destination. Confirm the pool appears in the repository's runner settings. If it remains absent, collect the App ID, repository, pool and redacted errors for [support](#request-support).

### Provisioning attempts keep retrying

RunnerFoundry returns an unaccepted job to the back of its Runner Pool after provider cleanup succeeds. This is expected when the provider repeatedly fails to start a runner. Once a runner accepts the job, later execution failures belong to the normal GitHub workflow lifecycle and are not provisioning retries.

Inspect runner-start and cleanup events. Check the pool's [`stop_if_not_started_within` setting](configuration.md#runner-pool-configuration) and the provider environment. Docker pools must satisfy the [Docker Compose contract](providers/docker.md#prepare-and-configure-a-service). A libvirt image must satisfy the [guest contract](providers/libvirt.md#libvirt-guest-contract). Use [provider diagnostic logs](providers.md#security-and-diagnostic-logs) without recording JIT configuration.

Confirm that a test runner starts, accepts one job and is removed afterwards. If retries continue, collect the pool, provider, timeout setting and redacted daemon and runner diagnostics for [support](#request-support).

### Docker access is denied

Use [Docker access troubleshooting](providers/docker.md#troubleshoot-docker-access) to check the daemon account, Docker endpoint, and host permissions.

### Libvirt access is denied

Use [libvirt access troubleshooting](providers/libvirt.md#troubleshoot-connection-access) to check the connection account and host policy.
QEMU image and overlay access is separate from daemon access to libvirt.

### PostgreSQL does not connect

Check the host, port, database name, login credential files and permission to assume `state.postgres.role`. Confirm every `file://` path is absolute and readable by `runnerfoundry`:

```sh
sudo -u runnerfoundry test -r /etc/runnerfoundry/secrets/postgres-user
sudo -u runnerfoundry test -r /etc/runnerfoundry/secrets/postgres-password
```

If a pooled endpoint does not preserve sessions, configure a [direct session-affine endpoint](deployment.md#prepare-the-shared-database). Restart after any `state` or PostgreSQL credential change. Confirm `systemctl status` returns to operational. Escalate with the SQLSTATE and error category from logs, without the DSN or password.

### Reload is rejected

Read the `Runtime reload rejected` event and correct its named cause. State changes always require restart. Other common causes include unreadable App keys, invalid provider settings and an unavailable replacement log destination.

If loading, validation, building or preparation failed, the previous configuration remains active. If the error names readiness notification, termination or retired-runtime cleanup, the replacement may already be active. Inspect current pools and resources before taking corrective action. Reload again after correcting the cause and confirm `Runtime reload applied`.

### Member is degraded

Find the first active failure in the configured log destination. A standby member is healthy; standby leadership alone is not degradation. Correct the failing App, provider, state or cluster dependency and look for its recovery event.

Confirm `systemctl status runnerfoundry` returns to `operational`. If another App or pool continued working, include that fact when escalating because health can degrade for one isolated operation.

### Request support

Use [GitHub Issues](https://github.com/stateshifters/runnerfoundry/issues) for bugs and operator questions. Include:

- output from `runnerfoundry --version`
- deployment mode and number of members
- state backend and PostgreSQL version when applicable
- runner provider and host operating system
- Runner Pool name and affected repository
- expected and observed behaviour
- the exact command or change which exposed the problem
- relevant operational events and error categories

Remove private keys, passwords, DSNs, JIT configuration, tokens and other credentials from configuration and logs. Do not replace a secret with a partial value which could still identify it. State which fields you redacted.

