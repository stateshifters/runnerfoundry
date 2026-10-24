# Auditstage scope

Every ordinary commit message requires exactly one Auditstage Jira key as `scope`. Standard work
uses the actual task key, matching `^AS-[1-9]\d*$`, such as `AS-44`. The repository's
[`@auditstage/commitlint-config`](../../../../packages/commitlint-config/index.js) owns this format.

Choose scope in this order:

1. Inspect the current branch for an Auditstage task key. Use it when present; a ticketed branch
   must never use `AS-0`.
2. Otherwise use the valid Auditstage key explicitly supplied by the user or task context.
3. If none is available, ask the user to confirm all three exception conditions: the change is an
   ad hoc small patch, has no Jira ticket, and will be delivered as a single-commit pull request.
4. Use `AS-0` only after that explicit confirmation. If any condition is false or uncertain, ask
   for the actual Jira key.

Never invent a key. Reject a missing scope, lowercase key such as `as-44`, another project key such
as `ABC-44`, a descriptive scope such as `web`, multiple scopes such as `AS-44,AS-45`, or unconfirmed
`AS-0`.

Set `scope` once and preserve it. Use `<type>(<scope>): <description>` or, for a real breaking
change, `<type>(<scope>)!: <description>`.
