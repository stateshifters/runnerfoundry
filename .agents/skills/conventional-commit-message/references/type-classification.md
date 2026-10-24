# Type classification

Choose type from the settled surface's release meaning, never effort, size, or code movement. Use
surrounding reads only to resolve the surface boundary; do not turn them into a new patch summary.

## Silent protocol

Perform this step without user-facing prose:

1. Answer the boundary questions below from `surface`.
2. Read surrounding code only when needed to resolve a boundary.
3. Do not narrate the patch, compare candidate types, or write a bridge or tentative summary.
4. Set only `type`.

If visible patch prose appears before `type` is settled, restart classification silently.

## Allowed types

Use the repository or user's established set when clear. Otherwise use:

- `feat`: a new user-facing capability or materially expanded behavior.
- `fix`: an existing surface stops producing wrong outcomes during normal use.
- `ci`: CI, workflow, or pipeline work whose primary outcome is CI behavior.
- `docs`: work whose primary outcome is documentation.
- `style`: formatting only, with no behavioral effect.
- `refactor`: internal restructuring that preserves relied-on surface outcomes.
- `test`: test-only work.
- `chore`: maintenance or project work that fits none of the above, is not CI-specific, and does not
  describe user-facing behavior.

Treat `feat` and `fix` conservatively because they are release signals.

## Boundary gate

Treat a surface as behavior that callers, users, operators, or downstream systems rely on; it is not
limited to UI. Classify the served operation, not the layer where its implementation lives.

Ask in order:

1. Does the patch add a surface or change an existing one?
2. For an existing surface, what wrong outcome stops happening?
3. Can that change be named in ordinary boundary terms?

Choose `fix` only when correcting that relied-on boundary outcome is the primary shipped meaning.
Choose `refactor` only when the same operations preserve the same relied-on outcomes. Defensive,
structural, storage, or data-layer implementation does not demote a genuine correction to
`refactor`.

Conversely, do not promote extraction, decomposition, centralization, helper movement, internal
relocation, storage mechanics, or defensive technique to `fix` when no boundary outcome changes.
A subordinate safer failure inside primarily internal work does not make the whole surface a `fix`.

Set `type` at this checkpoint. Do not emit an explanation, reconsider it while writing, or change it
because plain-language wording is difficult.
