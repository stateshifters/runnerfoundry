---
name: verification
description: MUST USE EVERY SINGLE TIME WHEN finishing up a task, this skill is used to verify the correctness of your work.
---

Before you finish a task, you must stop, take a step back.
Stop focusing on the details of the task and look at the bigger picture. Ask yourself the following questions:

- Have I followed the guidelines set out in [Contributing](./CONTRIBUTING.md)?
- Have I built all my changes towards the goals set out in [PRD](./docs/PRD.md)?
- Is the solution the simplest it can be to achieve the goals of the task being worked on?
- Have I ensured that the scope hasn't been inflated beyond the goals of the actual task being worked on?

If the answer to any of these questions is "no", you must go back and revise your output until all answers are "yes".

DO NOT hyper-focus on the details of the task and forget to look at the bigger picture. The goal is to ensure that your output is correct, complete, and aligned with the project's goals and guidelines. Sometimes a solution doesn't come from local patching, but might require a more holistic approach and broader refactoring of your output. THAT IS FINE. We do not want to patch over problems, we want to solve them.
Refactoring is a natural part of the development process, and it is encouraged when it leads to a better solution.
Involve the user if needed.

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
