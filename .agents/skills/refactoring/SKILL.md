---
name: refactoring
description: MUST USE for large architectural refactorings, package-boundary cleanup, dependency inversion, module moves, or broad internal restructuring intended to preserve RunnerFoundry's product behaviour.
---

# Architectural refactoring

Large architectural refactorings may replace substantial internal structure while preserving RunnerFoundry's product promise. Treat the product contract and target architecture as fixed acceptance boundaries. The end-to-end suite provides evidence that the refactoring has not broken that promise across the system.

The end-to-end suite is intentionally expensive. Do not prepare or run it unless the user explicitly requests it. Follow `CONTRIBUTING.md` for the suite's scope, prerequisites, and commands instead of restating them here.

## Protect the acceptance evidence

Treat every end-to-end test as immutable during a refactoring session. Do not edit, remove, weaken, skip, replace, or regenerate an end-to-end test as part of making the refactoring pass.

If an end-to-end test appears to require modification, stop before changing it. Identify the exact modification and explain why it is necessary, then obtain the user's explicit consent for that modification. Consent applies only to the single identified modification. It does not authorize any other end-to-end test change, even in the same file or refactoring session. General permission to refactor or change tests is not consent to modify end-to-end tests.

## Self-verification

Before handing off a refactoring:

- Confirm that the implementation still conforms to the product contract and target architecture.
- Confirm that no end-to-end suite preparation or execution occurred without an explicit user request.
- Confirm that no end-to-end test changed without consent for that exact modification.
- Confirm that any requested end-to-end verification followed `CONTRIBUTING.md` rather than commands copied into this skill.
