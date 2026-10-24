# Migrate RunnerFoundry state

Use this guide when an upgrade changes RunnerFoundry's durable schema. Back up state and stop every affected member before a controlled transition.

Do not infer a RunnerFoundry release from a schema number. Check [RunnerFoundry releases and release notes](https://github.com/stateshifters/runnerfoundry/releases) for the versions you are installing or restoring.

Before upgrading, record the installed `runnerfoundry --version`, the migration status when using PostgreSQL, and the exact package used. Keep that record and package with the verified state backup. They provide the safe baseline for a restore or rollback decision.

## SQLite schema transitions

The current SQLite schema is 109. RunnerFoundry automatically upgrades supported older schemas during startup.

Supported existing versions follow these paths:

| Starting schema | Transition | Operator-visible result |
| --- | --- | --- |
| 106 | 106 to 108, then 108 to 109 | preserves core job state; discovery recreates GitHub routes after App-less registration and ownership state is cleared |
| 107 | 107 to 108, then 108 to 109 | preserves core job state; discovery recreates GitHub routes after App-less registration and ownership state is cleared |
| 108 | 108 to 109 | preserves existing jobs and GitHub registrations and ownership; no route reset is required |
| 109 | none | opens without a schema transition |

Versions 106 and 107 cannot assign a trustworthy App identity to existing GitHub registrations and scale-set ownership. Their upgrade clears only that adapter state. It preserves core queued jobs, processing jobs, tombstones, membership, capacity and Runner Pool declarations. GitHub routes reappear after discovery.

The move from 108 to 109 preserves App-scoped registrations and ownership, so it does not require route recreation.

An unsupported non-zero schema is rejected. RunnerFoundry does not silently delete or reset an old database. Keep the original state files, restore the pre-upgrade backup if needed, and select a release path which supports the existing schema.

Follow the [stopped-service SQLite backup and upgrade procedure](operations.md#back-up-and-upgrade-sqlite). Keep the database, rollback journal, WAL and shared-memory sidecars together whenever they exist.

## PostgreSQL schema transitions

The current PostgreSQL catalogue target is 8, with known versions from 0 through 8. Ask the installed binary for its own range before changing the database:

```sh
sudo -u runnerfoundry runnerfoundry migrate status --config /etc/runnerfoundry/config.yaml
```

Status uses this exact shape:

```text
current=5 target=8 known=0..8
```

`current` is the database version. `target` is the installed binary's catalogue target. Do not copy the sample current version into a command without first reading your own status.

Run migration commands as the `runnerfoundry` service account so protected configuration and credential references resolve in the same access context as the service. Another account must have explicit read access to the configuration and every referenced credential file.

### Move forward

Use the [stopped-cluster PostgreSQL upgrade procedure](operations.md#upgrade-postgresql). Apply all known forward migrations from one host:

```sh
sudo -u runnerfoundry runnerfoundry migrate up --config /etc/runnerfoundry/config.yaml
```

Success prints:

```text
Migration up completed at version 8.
```

The number is the version reached by that binary. Run status again and require `current` to equal `target` before restarting members.

A daemon also applies known forward migrations on startup. For a controlled cluster upgrade, keep all members stopped and use the explicit command once.

### Cross migration 006

The move from schema 005 through 006 introduces App-scoped GitHub registration and scale-set ownership.

Migration 006 saves the schema-005 adapter state for rollback and clears the active App-less registrations and ownership. It preserves core jobs, tombstones, membership, capacity and Runner Pool declarations. Expect GitHub routes to be recreated after the first post-upgrade discovery pass.

The saved schema-005 adapter state exists for rollback. It is not a live mirror of later adapter activity.

### Roll back PostgreSQL

Rollback can discard state introduced after the target schema. Use the pre-upgrade version and status record, retained package and [release notes](https://github.com/stateshifters/runnerfoundry/releases) to establish which release supports the intended target. Stop every member and take a fresh verified database backup before proceeding.

If the pre-upgrade baseline or maintained release information does not establish a compatible release and target, do not run `migrate down`. Keep the cluster stopped and ask a maintainer for the correct recovery path.

Inspect status, then roll back only to the known version from that baseline. For example, use 5 only when your evidence identifies it as the required target:

```sh
sudo -u runnerfoundry runnerfoundry migrate down --to 5 --config /etc/runnerfoundry/config.yaml
```

Success prints the version reached:

```text
Migration down completed at version 5.
```

RunnerFoundry rejects a rollback while a leader lease is active or a cluster member is still live. If that happens, confirm every member is stopped and wait for member liveness to expire before retrying.

Rolling back across 006 restores the saved schema-005 GitHub adapter state. It does not translate or preserve current registrations and ownership created under schema 006 or later. Core schema rollback effects depend on every crossed migration, so restore the backup if the result is not acceptable.

After rollback, run status again. Install the release which supports the selected schema on every host before restarting the cluster. The schema number alone does not identify that release.

### Handle rejected schemas

RunnerFoundry rejects a PostgreSQL schema newer than its binary. It also rejects an incompatible legacy schema which predates the current migration metadata instead of deleting or adopting it.

Keep all members stopped. Preserve the database, record the exact status or startup error and choose one of these safe paths:

- install a binary which supports the database's current schema
- restore a verified backup which matches the intended binary
- follow a documented release-by-release upgrade path

Do not drop migration metadata or application tables to force startup.
