# Independent type review

This gate applies only to a requested complete commit message. After drafting the complete message,
set `candidate_message` and keep it settled during review.

Before delegating, apply this recursion guard:

- If the configured agent name is `commit_type_reviewer`, do not invoke or spawn an agent; continue
  directly to the final check.
- Otherwise invoke `commit_type_reviewer` exactly once without parent conversation history
  (`fork_turns = "none"` or equivalent), providing `candidate_message` explicitly as the proposed
  complete message.

Do not provide a patch summary, implementation rationale, tentative type comparison, or argument
for the chosen type. The reviewer must inspect the same Git surface and classify independently.

Handle the result once:

- On exact `APPROVE`, keep `candidate_message` unchanged.
- On `REJECT`, use the complete message in `Replacement:` as authoritative; do not review it again.
- If invocation fails, the reviewer returns `UNABLE TO REVIEW`, or the response is malformed, rerun
  the classification and breaking checks locally once against settled `surface`. Revise
  `candidate_message` only if those checks prove it wrong, and do not delegate again.

Keep this orchestration silent. Return only the approved, replaced, or locally revalidated message,
without a verdict, trace, warning, or other commentary.
