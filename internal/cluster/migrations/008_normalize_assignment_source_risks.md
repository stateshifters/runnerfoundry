# Migration 008 assignment-source normalization risks

## Preconditions and scans

Every RunnerFoundry node must be stopped before migration. The Up preflight scans `scaler_assignments`, `scaler_demand`, and `scaler_runner_lifecycle` and reports only aggregate invalid counts. Any missing demand, contradictory source or epoch, invalid assignment state or identity pair, lifecycle identity mismatch, or invalid lifecycle phase and timestamp combination aborts the transaction before mutation. Operators must repair the reported category and restart migration; row payloads are deliberately absent from the error.

## Locks and duration

The preflight and source backfill scan the assignment and lifecycle working sets. `ALTER TABLE` takes `ACCESS EXCLUSIVE` locks on `scaler_demand`, `scaler_assignments`, and `scaler_runner_lifecycle`. Constraint and `NOT NULL` validation scan the affected tables while those locks are held. The migration is therefore a stopped-cluster operation, not an online rollout. The migration and tern advisory locks serialize attempts; no additional advisory key is acquired.

## Compatibility floor

Up removes version-007 assignment columns after their values are validated against demand and advances `oldest_binary_schema_generation` to `8`. A version-007 binary must not start against the contracted schema. Upgrade every node before restarting the cluster.

## Rollback and data preservation

Down runs in the same transaction, restores the version-007 columns, reconstructs their values from each mandatory demand parent, restores the version-007 constraints, and returns the compatibility floor to `7`. It removes no assignment, demand, runner lifecycle, or cleanup history. New version-008 assignments therefore remain representable after downgrade.

## Failure recovery

An Up or Down failure rolls back schema, data, and tern version metadata together. Leave the cluster stopped, correct the precondition or lock blocker, and retry with the same binary. After a successful Down, deploy the version-007 binary only after `migrate status` reports version `7`. Backup restoration remains a stopped-cluster operation followed by `migrate status` and the required Up migration before nodes restart.
