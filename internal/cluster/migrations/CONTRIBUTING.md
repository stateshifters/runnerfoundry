# PostgreSQL migrations

Read the repository [contribution contract](../../../CONTRIBUTING.md) before changing the PostgreSQL schema. This guide owns only the mechanics and safety requirements for current migration work; it does not define RunnerFoundry product behavior.

## Create a migration

Migration definitions live with the data owner:

- source-neutral PostgreSQL state migrations belong in `internal/state/postgres/migrations`;
- source-specific PostgreSQL migrations belong in that source adapter's PostgreSQL persistence package; and
- `internal/cluster` composes and executes the registered migration sets but does not own schema SQL.

Each owner exports its embedded migration filesystem and the daemon registers the owner as a migration set in `internal/daemon/migration_application.go`. That composition is the authoritative owner registry. Registration order resolves dependencies between fragments that share a global sequence: Up runs lower owner orders first and Down reverses that order. Choose an unused order with maintainers when registering a new owner, place it according to its schema dependencies, and do not change an existing owner's name or order after release.

Migration sequence numbers are global, sequential, and append-only once released. Inspect every migration directory registered by the daemon and use one more than the highest sequence present across them. Coordinate any planned but unmerged migration with maintainers before selecting that number. Before the first release, an explicitly approved schema reset may replace the unreleased sequence with a fresh `001`; no compatibility or data conversion is implied. Otherwise, name a new owner-local file `NNN_description.sql` using the next global integer. Do not leave gaps, reuse or renumber a released sequence, or edit a released migration.

More than one owner may contribute a fragment to the same global sequence when the change crosses ownership boundaries. An owner may contribute at most one fragment to a sequence. Each fragment must remain independently reversible, and later owner fragments may depend only on objects created by earlier owner orders in that sequence.

Each file uses tern's reversible format:

```sql
ALTER TABLE scaler_nodes ADD COLUMN availability_zone text;

---- create above / drop below ----

ALTER TABLE scaler_nodes DROP COLUMN availability_zone;
```

SQL above the separator is Up and SQL below it is Down. Both directions run transactionally. Do not use `---- tern: disable-tx ----` without explicit maintainer approval for a PostgreSQL operation that cannot run in a transaction and a documented safe failure procedure.

All RunnerFoundry-owned schema creation, alteration, indexing, constraints, data transformations, and teardown SQL belong in the owning package's migration files. Go code may register migration sets and orchestrate tern but must not duplicate schema-definition SQL.

## Preserve rollback parity

Down must return the database to the schema and data model expected at the target version. Reverse tables, columns, sequences, indexes, constraints, defaults, and data transformations in dependency-safe order.

Test Up, Down, and Up again with representative data. A failed direction must roll back the active transaction and leave tern's recorded version at the last completely applied migration.

## Use safe PostgreSQL changes

- Add nullable columns before backfilling and constraining them.
- Validate existing rows before adding a constraint.
- Create ordinary indexes transactionally.
- Stage renames and incompatible type changes through additional columns.
- Keep transformations bounded and deterministic.
- Preserve the data required by the documented Down operation.
- Require a stopped cluster for changes that cannot safely coexist with active members.

## Keep ownership stable

Every migration runs after the session assumes `state.postgres.role`. Tables, sequences, indexes, and tern metadata must remain owned by that stable role. Do not grant RunnerFoundry objects directly to transient login users.

The stable role must remain `NOLOGIN`, `NOSUPERUSER`, `NOCREATEDB`, `NOCREATEROLE`, and `NOREPLICATION`. Migration SQL must not create, alter, grant, revoke, or inspect PostgreSQL roles or memberships.

Do not acquire or reuse RunnerFoundry's reserved advisory-lock keys.

## Operate migrations

For installed deployments, follow the [operator upgrade procedure](../../../docs/operations.md#upgrade-postgresql) and [schema transitions and rollback](../../../docs/migrations.md). These are operator tasks, not contributor verification.

## Verify a migration

Migration tests must follow the repository's [test-isolation contract](../../../CONTRIBUTING.md#test-isolation). Before review, run:

```sh
make lint
make fmt-check
make vet
```

Review must confirm that:

- the migration is registered from the package that owns the affected data;
- the filename uses the next global sequence, or is an approved owner fragment for that sequence;
- owner names and orders are stable and do not conflict;
- Up and Down are present, transactional, and dependency-safe;
- representative schema and data complete the Up/Down/Up round trip;
- objects retain the configured stable-role ownership;
- no schema-definition SQL was added to Go code; and
- the available verification gates pass without new suppression.
