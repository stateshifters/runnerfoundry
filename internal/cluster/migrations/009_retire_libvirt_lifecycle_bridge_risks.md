# Migration 009 legacy lifecycle contraction risks

## Preconditions and scans

Every RunnerFoundry node must be stopped before migration. The Up preflight scans the legacy and generic lifecycle tables and reports only aggregate `legacy_only` and `conflicting` counts. A legacy row is unsafe when no generic row has the same node, scale set, repository, and runner name, or when the matching generic row disagrees on provider, request identity, or pending-reap state. Any non-zero count aborts the transaction before mutation. Operators must repair the reported category and retry; row payloads are deliberately absent from the error.

## Locks and duration

The aggregate joins scan both lifecycle working sets. Dropping the trigger and table takes brief `ACCESS EXCLUSIVE` locks on `scaler_libvirt_runner_lifecycle` after the preflight succeeds. The migration is a stopped-cluster contraction, not an online rollout. The migration and tern advisory locks serialize attempts; no additional advisory key is acquired.

## Compatibility floor

Up removes the expired compatibility representation and advances `oldest_binary_schema_generation` to `9`. A binary whose maximum schema generation is `8` or lower must not start against the contracted schema. Upgrade every node before restarting the cluster; mixed-version operation across this boundary is unsupported.

## Rollback and data preservation

Down runs in the same transaction, returns the compatibility floor to `8`, recreates the version-8 table and indexes, and repopulates every libvirt row from `scaler_runner_lifecycle` before restoring the legacy trigger. Generic lifecycle rows are not updated or deleted, so timestamps, assignment relationships, phases, and cleanup reasons remain exact. The reconstructed legacy projection preserves request identity, runner name, and whether cleanup is pending.

## Failure recovery

An Up or Down failure rolls back schema, data, and tern version metadata together. Leave the cluster stopped, correct the precondition or lock blocker, and retry with the same generation-9 binary. After a successful Down, deploy the older executable only after `migrate status` reports version `8`. Backup restoration remains a stopped-cluster operation followed by `migrate status` and any required migration before nodes restart.
