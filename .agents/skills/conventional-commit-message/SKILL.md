---
name: conventional-commit-message
description: Generate the final Conventional Commit message text by inspecting the intended git commit surface, with staged changes as the default source of truth. Use this whenever the user asks for a commit message, wants staged changes or the working tree summarized into a Conventional Commit, asks what type or scope to use, or needs help turning staged changes into release-note-ready commit text instead of code-level narration.
---

# Conventional Commit Message

Treat commit messages as release metadata, not implementation diaries. Inspect the intended commit
surface, classify its release meaning, and write through the branch assigned to that type.

## Output contract

When asked for the message itself, return only the final message text. Default to one line with
an optional scope:

`<type>[optional scope]: <description>`

Use [scope.md](references/scope.md) to decide whether a scope helps. Return a full message only for a breaking change whose impact
cannot be stated responsibly in the subject. Do not return analysis, alternatives, or a walkthrough
unless requested.

If asked only for the type, scope, or breaking status, answer that question directly. Do not force a
complete message or run the independent reviewer.

## Required workflow

For a complete message, follow every step in order. For a narrower answer, run only its required
decision path: steps 1-2 for type, the scope reference for scope, or steps 1 and 5 for breaking
status. Carry the state used by that path from `surface`, `type`, `branch`, `effect_sentence`,
`scope`, and `candidate_message` as hidden state. Set each value at its assigned step and do not
silently replace it later.

### 1. Settle the commit surface

Infer the change set most likely intended for the next commit. Read `git status --short`, then:

- use `git diff --staged` as the default source of truth whenever staged changes exist;
- use `git diff` only when nothing is staged, unless the user explicitly requests the full working
  tree; and
- use recent history only to break naming ties, never to override the current patch.

If staged and unstaged changes imply materially different messages and the user is not clearly
asking about the staged set, ask one short clarifying question instead of blending them. Tests show
intent but do not prove external impact; never infer `feat` or `fix` from tests or internal work
alone.

Set `surface`. Do not summarize it as an architecture move, technique, or mechanism before
classification.

### 2. Settle the type silently

Read [type-classification.md](references/type-classification.md) completely. From the start of
classification until `type` is settled, emit no user-facing prose. Tool reads are allowed. If any
patch narration appears, discard that classification and restart silently from `surface`.

Set `type` once. Later wording difficulty cannot promote or demote it.

### 3. Write through the matching branch

Set `branch` directly from `type`:

- `feat` and `fix` use the external branch; read
  [external-writing.md](references/external-writing.md) completely and follow it exactly.
- `ci`, `docs`, `style`, `refactor`, `test`, and `chore` use the internal branch. Concisely and
  truthfully name the engineering work according to `type`, without diff narration or invented
  user or operator impact.

Do not change `type` or `branch` while wording the subject. Reopen classification only if rereading
`surface` proves it wrong, not because another subject is easier to write.

### 4. Settle the scope

Read [scope.md](references/scope.md) completely. Set `scope` to the chosen scope or omit it, and
preserve that decision in the subject.

### 5. Decide whether the change is breaking

Use `!` only when changed public behavior, interface expectations, or operator workflow creates
incompatibility or requires action. A rewritten implementation is not breaking by itself. Make the
impact explicit; if the subject cannot do so responsibly, return a full message with a short
breaking-impact explanation.

### 6. Run the independent type review

For a complete commit message, draft and set `candidate_message`, then read and follow
[independent-review.md](references/independent-review.md) completely. This gate does not apply to a
type-only, scope-only, or breaking-status answer.

## Final check

Before returning, verify all of the following:

- `surface` is the requested surface and `type` reflects release meaning, not implementation effort.
- The subject uses the exact settled type, includes the settled scope only when chosen, and
  includes `!` only for a breaking change.
- The writing branch still matches `type`; no wording pass changed either value.
- External wording still derives from the settled effect sentence and passes every role, phrase,
  shape, and word check in the external-writing reference.
- Internal wording names the engineering work without inventing external impact.
- Breaking syntax represents a real compatibility break.
- For a complete message, the independent gate ran exactly once unless its recursion guard or local
  fallback applied, and the final text is the approved candidate, authoritative replacement, or
  locally revalidated fallback.
- The output contract is intact: no analysis, reviewer verdict, orchestration detail, or fallback
  warning accompanies the requested message.

Revise before returning if any check fails.
