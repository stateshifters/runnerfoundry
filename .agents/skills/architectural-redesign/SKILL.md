---
name: architectural-redesign
description: MUST USE when existing code needs to be assessed and redesigned for architectural fitness and code quality, including when repeated implementation friction shows that a proposed architecture is wrong. This is strict architecture design, not code review, cleanup, or implementation planning.
---

# Architectural redesign

You are redesigning the correct architectural form of existing code.

Do not reduce this work to tidying, simplifying a diff, reviewing local style, or planning a refactor. The purpose is to determine what the selected code should become so that it fits the application's architecture and meets the required standard of code quality.

The current implementation is evidence about the present state. It is not the design authority.

A proposed target form is also not beyond correction. Implementation evidence can reveal constraints that the design failed to account for. When that evidence forms a repeated pattern, preserve the lessons and replace the design rather than protecting the sketch with accumulating workarounds.

## Scope

The user provides the scope.

Keep the proposed changes within that scope. Inspect consumers, dependencies, neighbouring boundaries, and execution paths wherever necessary to understand the architecture.

If architectural fitness cannot be achieved without changes outside the supplied scope, identify the external change as a prerequisite. Do not conceal the problem with a local workaround or silently expand the proposal.

## Establish the architectural role

Determine the role the selected code must perform in the application.

Establish:

- the domain responsibility it owns;
- the behaviour and invariants it protects;
- the architectural layer or boundary it belongs to;
- the decisions it is authoritative for;
- the state and resources it owns;
- the responsibilities it must not absorb; and
- the reason it should exist as a distinct architectural element.

Derive this role from product requirements, domain meaning, and the target architecture. Do not derive it solely from current names, packages, interfaces, or tests.

## Reconstruct the current architecture

Describe how the selected code actually participates in the system.

Examine:

- incoming and outgoing dependencies;
- consumers and their expectations;
- public and de facto interfaces;
- ownership of data, state, policy, and side effects;
- control and data flow;
- construction, lifecycle, and concurrency;
- success, failure, and recovery paths;
- boundaries crossed at runtime; and
- tests that encode or depend on the current design.

Look beyond declared interfaces. Hidden coupling, shared representations, global state, call ordering, duplicated decisions, and consumer workarounds are part of the effective architecture.

## Judge architectural fitness

Architectural fitness requires more than conformance to a package diagram.

The design must demonstrate:

- one clear owner for each responsibility and invariant;
- cohesive responsibilities within boundaries;
- dependencies that follow the intended direction;
- narrow interfaces expressed in domain terms;
- implementation decisions hidden from consumers;
- explicit state, dependencies, side effects, and assumptions;
- control and data flow that can be understood locally;
- deliberate lifecycle and failure behaviour;
- limited change propagation;
- meaningful behaviour that can be tested through stable boundaries; and
- no complexity without a current, evidenced purpose.

A structure is not architecturally fit merely because it resembles an established pattern. The pattern must be appropriate to the responsibility, ownership, lifecycle, and execution context.

## Judge code quality

Code quality is part of the architecture, not a finishing activity.

Assess the applicable qualities of:

- readability;
- correctness;
- reliability;
- operability;
- efficiency;
- maintainability;
- safety; and
- consistency.

Pay particular attention to whether the design supports local reasoning, explicit dependencies, visible state, predictable behaviour, high cohesion, loose coupling, small surfaces, protected invariants, and safe incremental change.

Do not preserve complexity, abstraction, configurability, or indirection without evidence that it serves required behaviour or a real constraint.

## Recognise when the design is wrong

Implementation friction invalidates a proposed architecture when the same underlying mismatch appears repeatedly. The signal is a pattern, not the mere existence of difficult cases.

Material signals include:

- the same shape of workaround recurring across otherwise unrelated code;
- unrelated edge cases requiring equivalent special-case branches;
- types requiring escape hatches, casts, or nominally optional fields that are always required in practice;
- synchronization being added where the design claimed state was not shared;
- callers needing knowledge of an abstraction's internal rules to use it correctly; and
- two or more independent implementation deviations with the same underlying cause.

Use judgment. A few edge cases do not condemn an architecture. Legitimate complexity in the domain or data does not by itself show complexity in the design. The invalidation signal is repeated friction of the same shape caused by a mismatch between the proposed form and implementation reality.

When the signal is present, discard the affected sketch. Treat the implemented work and its friction as architectural evidence. Re-establish the required role and constraints as if the newly learned facts had been known from the start. Remove faulty responsibilities, boundaries, abstractions, and mechanisms before introducing replacements. The revised target should begin smaller than the rejected form and grow only where required behaviour or evidenced constraints demand it.

Do not bolt corrective machinery onto a design whose premises have been disproved. Do not preserve the rejected form merely to salvage implementation effort.

## Judge the test architecture

Tests are evidence about behaviour and design pressure.

Determine:

- which meaningful behaviours and invariants require protection;
- which architectural boundary should expose those behaviours;
- whether the current tests exercise that boundary;
- whether the tests depend on implementation details;
- whether test difficulty reveals hidden state, excessive coupling, or misplaced ownership; and
- whether existing tests preserve accidental or unsupported behaviour.

Design testability into the architecture. Do not distort production interfaces solely to accommodate tests.

The target form must allow meaningful behaviour to be verified directly through stable seams.

## Identify defects

Report defects only when there is evidence that the current form conflicts with required behaviour, architectural intent, or code-quality standards.

For each defect, state:

- the current condition;
- why it is architecturally or qualitatively wrong;
- the consequence;
- the responsibility or boundary involved; and
- what the target design must correct.

Distinguish defects from preferences. Distinguish facts from inferences and unknowns.

Trace symptoms back to the owning architectural problem. Do not recommend consumer-local patches for defects owned elsewhere.

## Stop when no redesign is needed

If the selected code has a justified role, fits the target architecture, and has no evidenced defect within the supplied scope, report that conclusion and stop. State that no redesign or implementation plan is needed. Do not search CI, packaging, release, deployment, or other adjacent systems for follow-up work merely to produce a target design or actionable recommendation.

## Design the target architecture

Design the simplest form that fully satisfies the required role.

Specify:

- responsibilities and explicit non-responsibilities;
- boundary and ownership;
- application-facing interface;
- dependency direction;
- owned domain types and invariants;
- state and resource ownership;
- control and data flow;
- lifecycle and concurrency;
- failure and recovery behaviour;
- internal organisation;
- test boundaries; and
- obsolete concepts that should disappear.

Every abstraction, interface, type, and layer in the proposed form must have a clear architectural purpose.

Do not preserve the current shape for convenience. Do not introduce speculative flexibility. Do not trade architectural correctness for a smaller implementation diff.

The design must be complete enough that a separate implementation task can realise it without inventing missing architectural decisions.

## Demonstrate fitness

Compare the proposed form with both the current architecture and the intended architecture.

Show:

- which responsibilities move, remain, or disappear;
- which interfaces change and why;
- how ownership becomes clearer;
- how dependency direction improves;
- how coupling and change propagation are reduced;
- how state and side effects become explicit;
- how meaningful tests attach to stable boundaries;
- which unnecessary complexity is removed; and
- which quality risks remain.

Architectural fitness must be demonstrated through these relationships. Do not assert that the proposal is cleaner or better without explaining why.

## Authority

This skill produces architectural analysis and a target design.

It does not authorize implementation. Do not modify production code, tests, documentation, configuration, or Git state unless the user separately requests implementation.

## Output

When redesign is needed, present the complete design as:

1. `Scope`
2. `Required architectural role`
3. `Current architecture`
4. `Architectural fitness assessment`
5. `Code-quality assessment`
6. `Test architecture assessment`
7. `Defects`
8. `Target architecture`
9. `Fitness of the proposed design`
10. `External prerequisites`
11. `Unresolved decisions`

Omit `External prerequisites` and `Unresolved decisions` when there are none.

When no redesign is needed, present only sections 1 through 7, state the no-change conclusion in `Defects`, and omit sections 8 through 11.

## Self-verification

Before presenting the design, verify that:

- the supplied scope was respected;
- the required role came from product, domain, and architectural evidence;
- effective dependencies and interfaces were examined, not only declared ones;
- every defect has evidence and a concrete consequence;
- code quality was treated as a design requirement;
- tests were considered as part of the architecture;
- each proposed abstraction, when redesign is needed, has a necessary responsibility;
- ownership, dependencies, state, flow, lifecycle, and failure behaviour are explicit where redesign is needed;
- any proposal corrects root architectural problems rather than hiding symptoms;
- repeated implementation friction was assessed as a pattern rather than as isolated anecdotes;
- any design invalidated by that pattern was replaced using the implementation lessons as evidence;
- architectural fitness was demonstrated rather than asserted;
- any proposed design is complete enough to implement without architectural invention; and
- no implementation authority was inferred.
