---
name: github-scaleset
description: MUST USE whenever work touches RunnerFoundry's GitHub Git source, GitHub adapter, runner scale sets, scale-set sessions or messages, GitHub job acquisition or lifecycle events, JIT runner configuration, or GitHub capacity publication. Prevents incorrect ARC and legacy GitHub API assumptions.
---

# GitHub scale-set source

RunnerFoundry integrates directly with the standalone `github.com/actions/scaleset` library pinned
in `go.mod`. It does not use Actions Runner Controller, Kubernetes controller behavior, or an older
GitHub runner API. Similar terminology does not make ARC documentation or implementation behavior
applicable.

Read [RunnerFoundry's scale-set mechanics](../../../docs/references/github-scaleset-mechanics.md)
before reasoning about this integration. When that reference does not settle a question, inspect
the pinned library version and the official [`actions/scaleset`](https://github.com/actions/scaleset)
source at the commit pinned in `go.mod` instead of inferring behavior from ARC or legacy APIs.

`RunnerRequestID == 0`, sometimes described loosely as GitHub Job ID `0`, is expected in some
scale-set lifecycle messages and is not by itself an error. `RunnerRequestID` is an acquisition
handle for `JobAvailable`, not a universal validity condition. A lifecycle event with a zero request
ID may correlate through its RunnerFoundry runner name. An event with no usable correlation cannot
change lifecycle state, but it does not invalidate the response statistics. Do not confuse these
upstream identifiers with RunnerFoundry's internal durable `core.JobID`.

## Self-verification

- Confirm the reasoning is based on RunnerFoundry's pinned `actions/scaleset` behavior, not ARC or a legacy API.
- Confirm no zero upstream job or request ID was treated as erroneous without event-specific evidence.
- Confirm GitHub-specific conclusions remain inside the GitHub adapter boundary.
