# Commit scope

Scope is optional. Omit it by default.

Add a scope only when it provides clear meaning outside the immediate patch context. Use a stable
component, package, or recognizable product area. Do not manufacture a scope from whichever
directory changed the most.

In the external writing branch, omit technical internals such as storage layers or helpers unless
they name a stable user-facing or operator-facing area.

Set `scope` once, including the decision to omit it. Use `<type>: <description>` without a scope
or `<type>(<scope>): <description>` with one. Add `!` before the colon only for a real breaking change.
