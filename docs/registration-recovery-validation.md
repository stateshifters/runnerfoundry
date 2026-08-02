# Registration-recovery validation

Use this checklist only during an authorized deployment window on an installation where creating and deleting test runner scale sets is acceptable. The project test suite does not perform these actions and does not claim live production validation.

## Preconditions

- Select a disposable repository visible to the RunnerFoundry GitHub App.
- Configure at least two scale sets in the same runner group and use workflows that target both names.
- Record the current scale-set IDs, service status, provider inventory, and lifecycle-store backup.
- Confirm that no production workflow can target the disposable repository during the exercise.
- Keep `prune` disabled unless pruning is itself part of the authorized test.

## Partial-registration recovery

1. Temporarily deny creation of the second test scale set at a controlled API boundary.
2. Trigger discovery and confirm that the first registration remains present while the repository has no active session group.
3. Restore creation access and observe the pending repository repair retry without triggering another discovery or reload.
4. Confirm that only the missing registration is created and both sessions become operational together.

## External-drift recovery

1. With both test sessions healthy and idle, delete one test registration through the authorized GitHub administration path.
2. Confirm that the next audit logs `repository_registration_drift`, recreates the missing registration, closes the old session generation, and activates a complete replacement generation.
3. Queue one workflow for each scale-set name and confirm that both are admitted without deleting any provider instance manually.

## Busy-runner safety

1. Start a controlled long-running job and record its exact runner and provider identity.
2. Replace its repository registration as above.
3. Confirm that the busy provider instance and its capacity charge remain until the job becomes terminal or the provider authoritatively reports that exact instance absent.
4. Confirm that durable cleanup then removes only the recorded instance and GitHub runner registration.

## Failover and reload

1. In clustered mode, transfer leadership during a healthy test job and confirm that the successor reconstructs the complete repository session group without deleting registrations.
2. Reload a configuration with the same desired scale sets and confirm that replacement registrations are verified before the old sessions close.
3. Restore the original configuration and verify `leader operational` or successful standalone reconciliation.

## Completion evidence

- Preserve structured logs for audit, missing registration, creation, drift, session close, replacement, and any cleanup retry.
- Record before-and-after scale-set IDs and provider identities.
- Confirm that no non-test repository, runner, demand row, lifecycle row, or provider instance changed.
- Restore any temporary permission or fault-injection control.
