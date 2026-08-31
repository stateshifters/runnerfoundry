---
name: verification
description: MUST USE EVERY SINGLE TIME WHEN finishing up a task, this skill is used to verify the correctness of your work.
---

Before you finish a task, you must stop, take a step back.
Stop focusing on the details of the task and look at the bigger picture. Ask yourself the following questions:

- Have I followed the guidelines set out in [Contributing](../../../CONTRIBUTING.md)?
- Have I built all my changes towards the goals set out in [PRD](../../../docs/PRD.md)?
- Is the solution the simplest it can be to achieve the goals of the task being worked on?
- Have I ensured that the scope hasn't been inflated beyond the goals of the actual task being worked on?

If the answer to any of these questions is "no", you must go back and revise your output until all answers are "yes".

DO NOT hyper-focus on the details of the task and forget to look at the bigger picture. The goal is to ensure that your output is correct, complete, and aligned with the project's goals and guidelines. Verification does not expand the requested outcome, authorize broader refactoring, or create a requirement for additional tests, gates, harnesses, or production mechanisms.

## Choose evidence by deliverable

- Production behavior: use the tests and project gates that own the affected contract.
- Production refactor: use existing behavior tests and relevant project gates.
- Documentation: inspect the changed reader journey and run only applicable documentation checks, never TDD or production software tests.
- Configuration: use parser, schema, or tool-native validation appropriate to the changed configuration, never TDD or production software tests.
- Mechanical edit or removal: use targeted search, diff inspection, and applicable static checks, never TDD or production software tests.
- Analysis or plan: reconcile claims with their evidence and check scope and internal consistency, never TDD or production software tests. Do not run a build or application merely to make the analysis look verified.

RunnerFoundry's built executable, installed package, container, and deployed instance are not software-test subjects. Do not execute the application through any of those forms as contributor or CI verification. Compiler, linker, and packager output is not itself a software-test subject, although those tools may run inside a listed project gate or produce a requested artifact. Production behavior is proved through production code at the stable test seams defined by the project. Use only the verification gates listed in `CONTRIBUTING.md`; do not invent another gate because a check is technically automatable.

## Verification result contract

A verification tool passes only when its underlying process exits with status 0.
The completion status of an outer shell, orchestration script, or tool call is
not evidence that a nested verification command passed.

- Inspect the exit status returned by every verification process.
- Propagate every non-zero child exit status through wrappers and orchestration
  layers so the overall operation fails.
- Treat a missing or unavailable exit status as unverified, never as passed.
- When one invocation contains multiple verification commands, stop at the
  first failure or inspect and report every child status explicitly.
- Run final verification after the last tracked-file change. Any later tracked
  change invalidates the affected verification results.
- Identify the verified revision and worktree state in the final handoff, and
  report the exit status of every verification command.

Use the coverage terminology required by [Contributing](../../../CONTRIBUTING.md#coverage-follows-purpose). Report the gate
result separately from the evidence that the suite adequately protects the required behaviors and invariants.

When no verification command is appropriate, report the inspection evidence used instead. Do not run an unrelated command solely to obtain an exit status.
