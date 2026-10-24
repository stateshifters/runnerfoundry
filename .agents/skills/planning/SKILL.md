---
name: planning
description: MUST USE before starting a task
---

Before you begin, you must stop, take a step back, and plan your approach to the task.

Read and truly internalize the [Contributing](../../../CONTRIBUTING.md). That will provide you with the context and guidelines necessary to ensure that your work is aligned with the project's standards.

DO NOT ignore the Contributing guidelines. If you don't follow them, your work will be rejected and cause friction. The guidelines are there to ensure a smooth development process and maintain code quality.

If the task is ambiguous or a massive task, stop and talk to the user to clarify the task and break it down into smaller, more manageable tasks. This will help you avoid scope creep and ensure that your work is focused and aligned with the project's goals.

DO NOT start working on the task until you have a clear plan of action. This will help you avoid wasting time and effort on work that may not be aligned with the project's goals.

Verify your plans against the [Contributing](../../../CONTRIBUTING.md) guidelines to ensure that your approach is aligned with the project's standards. If you find that your plans are not aligned, revise them until they are.

## Classify the deliverable

Before selecting tests or verification commands, classify each changed surface. Treat mixed tasks as separate surfaces:

- A change to production-code behavior uses test-first development through an agreed stable code seam.
- A production refactor relies on existing behavior tests; add a test only for a genuine unprotected behavior or invariant.
- Documentation uses documentation-specific inspection and checks, never TDD or production software tests.
- Configuration uses parser, schema, or tool-native static validation, never TDD or production software tests.
- A mechanical edit or removal uses targeted search, diff inspection, and applicable static checks, never TDD or production software tests.
- Analysis or planning uses evidence reconciliation, scope review, and internal consistency checks, never TDD or production software tests.

Do not treat configuration as production-code behavior merely because it is executable or affects runtime behavior. Do not plan application, installed-package, container, or deployed-runtime execution unless the user and project contract explicitly make that runtime operation the task.
