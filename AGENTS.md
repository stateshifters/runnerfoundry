Must adhere to local skills, the normative [product contract](docs/PRD.md), the operator [README](README.md), and [Contributing](CONTRIBUTING.md). Output must be correct, complete, and aligned with those documents. No implementation or documentation may define a job lifecycle that competes with the product contract.

## Code comments

Do not add a code comment unless it is strictly necessary to preserve material information that cannot be expressed through names, types, structure, tests, or external documentation.

Comments that restate identifiers, signatures, declarations, control flow, or language semantics are prohibited. Exported status does not justify a comment.

When reviewing code, treat every unnecessary comment added or materially changed by the change as a blocking finding. Do not approve or hand off the change until the comment is removed.
