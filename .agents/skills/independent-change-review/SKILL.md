---
name: independent-change-review
description: MUST USE after implementing and verifying changes to production code, project documentation, or project configuration in a software project whose deliverables include production code, before final handoff or an authorized commit
---

# Independent Change Review

Correctness and architectural fitness take precedence over review cost. Minimize review invocations without weakening those outcomes.

Apply this protocol when both of these conditions hold:

- the active work is in a software project whose deliverables include production code; and
- the reviewed change includes that project's production code, project documentation, or project configuration.

Run the protocol after implementation and the implementing agent's own verification, but before final handoff or an authorized commit. Bind a completed review to the exact reviewed change state. The same review may satisfy both handoff and commit when the relevant project files have not changed since it completed.

Review orchestration is internal. Do not ask the user for permission to run these required reviews, and do not mechanically narrate routine dispatch, clean results, or findings resolved within the authorized scope. Continue to communicate anything required by higher-priority instructions, including unresolved risks, scope or authority decisions, architectural conflicts, and blockers.

## Reviewer independence

Each reviewer must:

- be independent of the implementation;
- be dispatched without forked conversation context;
- receive a compact review packet containing the requested outcome, binding requirements, authorized scope, relevant changed files or diff, and available verification evidence;
- inspect the repository or supplied artifacts only to the extent allowed by its review lane; and
- return evidence-backed findings without editing files.

The implementing agent owns reviewer selection, adjudication, corrections, verification, and completion of the review cycle. Reviewers must not commission further reviews.

## Review lanes

Run every lane applicable to the reviewed project change and no others.

### Production-code correctness

For changes to production code, dispatch one independent reviewer to verify requirement fit, correctness, relevant failure paths, regression risk, test coverage, and consistency with project quality standards.

The reviewer must inspect every code comment added or materially changed by the reviewed change and apply the repository's code-comment policy. Report every violation as a blocking finding. The review cannot complete while an accepted, in-scope violation remains unresolved.

### Architectural fitness

Additionally dispatch a separate independent reviewer only when both of these conditions hold:

- **Substantial scope:** the production-code change spans multiple production components, requires coordinated structural changes across a meaningful portion of the production system, or reorganizes a meaningful portion of the production code; and
- **Architectural impact:** the change creates, removes, or materially changes production-system component boundaries, responsibilities, dependency direction, public contracts, persistence or material data flows, deployment topology, or a cross-cutting architectural pattern.

The architecture review packet must include the approved plan, the documented or inferred current and target architecture, and the project evidence supporting that baseline. If no plan was required, include the architectural baseline and project evidence established for the handoff.

### Documentation comprehension

For changes to project documentation, dispatch an independent reader that must not inspect source code, tests, configuration, implementation diffs, or other implementation evidence. It may read only the changed documentation and documentation that it directly references.

Ask the reader to explain the intended audience, purpose, behavior, actions, assumptions, and expected outcome in its own words, and to identify ambiguity, contradiction, missing prerequisites, or claims it cannot understand from the documentation alone.

This lane tests comprehension and internal coherence; it does not establish implementation accuracy. The implementing agent remains responsible for verifying documentation against the implementation.

### Configuration effect

For changes to project configuration, dispatch an independent reviewer to consult authoritative documentation for the installed or explicitly targeted tool version and verify that the change produces the requested effect.

The reviewer must consider syntax, defaults, precedence, environment or version conditions, failure behavior, and material security or operational consequences. It may inspect the changed configuration and the version-identifying manifests or lockfiles needed for that verification.

## Mixed changes

Apply every relevant lane to mixed project changes while preserving each lane's inspection restrictions.

## Adjudication and correction cycle

When review feedback returns, consult the `addressing-code-review-findings` skill before presenting or acting on any finding. Adjudicate every finding under that skill.

Correct every finding classified as **accepted—in scope**, then run targeted verification and the relevant complete project gates. Review feedback does not expand the authorized scope. Preserve and surface findings that require user authority, expose an architectural conflict, or are accepted but outside the authorized scope.

After corrections, repeat only the review lanes whose evidence the corrections invalidated. Each repeated reviewer must again be independent and receive no forked conversation context. Do not repeat a lane when no relevant project artifact or behavior materially changed, or merely because a finding was invalid, duplicated, already covered, or unverified.

The review cycle is complete when no applicable lane has an unresolved **accepted—in scope** finding and no unresolved decision prevents handoff or commit.
