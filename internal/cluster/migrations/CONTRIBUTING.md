# PostgreSQL migrations

This directory is the authoritative history of the clustered PostgreSQL schema. Read the repository [contribution contract](../../../CONTRIBUTING.md) and [ADR 4](../../../docs/arch/0004-coordinate-multi-host-scaling-in-postgresql.md) before changing it.

## Create a migration

Migration history is sequential and append-only after release. Name files `NNN_description.sql`, beginning with the next integer after the highest existing prefix. Do not leave gaps, reuse a number, renumber released files, or edit a released migration. The PostgreSQL schema has not reached production, so the unreleased `001_initial.sql` remains the one exception: amend it to keep a fresh installation equivalent to the first released model. Once a release contains it, all later changes require a new migration.

Each file uses tern's mandatory reversible format:

```sql
ALTER TABLE scaler_nodes ADD COLUMN availability_zone text;

---- create above / drop below ----

ALTER TABLE scaler_nodes DROP COLUMN availability_zone;
```

SQL above the separator is Up. SQL below it is Down. Both directions run transactionally. The `---- tern: disable-tx ----` directive is prohibited unless the maintainers explicitly approve a concrete PostgreSQL operation that cannot run in a transaction and its recovery procedure.

All scaler-owned schema creation, alteration, indexes, constraints, data transformations, and teardown SQL belong in these files. Go code may orchestrate tern but must not own `CREATE`, `ALTER`, `DROP`, or equivalent schema setup helpers. Tern's internal creation of its version table is the only exception.

## Preserve complete rollback parity

Every migration must return the database to the schema and data model expected by the preceding version. Irreversible migrations are not accepted. Down must reverse Up in dependency-safe order, including tables, columns, sequences, indexes, constraints, defaults, and transformed data.

A destructive transformation must preserve enough information to reconstruct the previous representation. Keep temporary compatibility columns or backup tables until a later release can safely contract them. If previous data cannot be restored, redesign the migration before submitting it.

Test Up from the immediately preceding version, Down back to it, and Up again. Use representative data to prove values survive the round trip. A failed Up or Down must roll back the active migration and leave tern's recorded version at the last completely applied migration.

## Maintain rolling compatibility

Release N must run alongside release N-1 while both binaries are live. Use an expand, backfill, contract sequence:

1. Expand the schema so both releases can operate.
2. Backfill in bounded transactional work while retaining the previous representation.
3. Contract only in a later release after the older binary is outside the supported overlap.

The `scaler_schema_compatibility` row records the oldest binary schema generation that may use the applied schema. Advance that floor only in the later contraction migration. A binary below the floor must fail startup rather than interpret an incompatible model.

### Safe change patterns

- Add nullable columns first. Backfill before adding a non-null constraint or a default that changes existing behavior.
- Add constraints only after existing rows satisfy them. Down removes the constraint before reversing supporting data changes.
- Create ordinary indexes transactionally. Non-transactional concurrent indexes require explicit approval and a documented retry and cleanup procedure.
- Treat renames as expand and contract: add the new name, keep both representations synchronized during the overlap, then remove the old name later.
- Stage type changes through a second column when values cannot be converted losslessly in both directions.
- Remove a table only after a prior release stopped depending on it and rollback data has been preserved.
- Keep data transformations bounded and deterministic. Down must reconstruct the exact previous representation, not an approximation.

## Keep ownership stable

Every migration runs after the session assumes `state.postgres.role`. Tables, sequences, indexes, and tern metadata must therefore remain owned by that stable role. Do not grant scaler objects directly to transient login users. Operators grant rotating login identities membership in the stable role instead.

Provision the stable role with `NOLOGIN`, `NOSUPERUSER`, `NOCREATEDB`, `NOCREATEROLE`, and `NOREPLICATION`, and limit its ownership and privileges to the scaler database or schema. The login identities need database connectivity and membership in this role. Migration SQL must not create, alter, grant, revoke, or inspect PostgreSQL roles or memberships.

The scaler reserves separate session advisory-lock keys for migration orchestration (`0x6769746875626d67`), tern's internal migration lock (`9628173550095224` in tern v2.4.1), and leader election (`0x6769746875627363`). Migrations must not acquire or reuse those keys.

## Understand installation, upgrade, and rollback

An empty database is installed automatically when the first daemon starts. Concurrent starters wait on the migration lock, then verify the completed version. Normal upgrades also run automatically before a node publishes capacity, competes for leadership, or reports ready.

If a migration fails, fix the migration or database prerequisite and restart. The failed transaction does not advance the version. Do not manually modify tern's version table or silently baseline an existing schema.

Rollback is explicit because an older binary cannot contain future Down migrations. Stop every scaler node, wait for its heartbeat to expire, and use the currently deployed binary:

```sh
runnerfoundry migrate status --config config.yaml
runnerfoundry migrate down --to <version> --config config.yaml
```

`down` acquires the migration and leader locks and rejects live node heartbeats. Deploy the older binary only after rollback succeeds. A full rollback to version zero removes scaler domain objects; tern's version table may remain.

Restore database backups only while the scaler cluster is stopped. After restoration, run `migrate status`, confirm that the restored version is known to the binary, and run `migrate up` before restarting nodes.

## Verify a migration

Follow vertical red-green-refactor slices. Add the behavioral test first, prove it fails, then add the minimum migration needed to pass.

Migration tests MUST follow the repository's [test isolation contract](../../../CONTRIBUTING.md#test-isolation).

Before review, run the currently available repository verification gates:

```sh
make lint
make fmt-check
make vet
```

Review must confirm all of the following:

- The filename is the next sequence and released history is unchanged.
- An edit to `001_initial.sql` is made only while it remains unreleased; otherwise the change uses a new migration.
- Up and Down are both present, transactional, and dependency-safe.
- Schema and representative data return to the previous state after Down.
- The migration supports the N/N-1 rolling window and updates the compatibility floor only when appropriate.
- Objects remain owned by the configured stable role, with no grants to login identities.
- No scaler schema SQL was added to Go code.
- Fresh install, upgrade, rollback, failure, retry, and restore behavior remain covered by hermetic tests.
- The available verification gates pass with no new linter suppression.
