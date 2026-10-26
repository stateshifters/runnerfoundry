# External writing branch

Use this branch only for `feat` and `fix`. Write for users, operators, support, release managers,
and history readers who never saw the patch. Translate the settled external meaning into the
experienced effect; do not name implementation or mechanism merely because it feels concrete.

## Set the effect sentence

The first sentence written in this branch must be the candidate `effect_sentence`, with no lead-in,
rationale, patch summary, or type reconsideration. It must:

- name the affected product, domain, user, or operator concern in ordinary language;
- say what works, stops going wrong, becomes possible, or must be known after shipping;
- use a consequence verb rather than an engineering-method verb; and
- stand alone for someone who never read the patch.

If its key nouns or actors require implementation knowledge, discard and rewrite it without
changing `type`.

Set and preserve these hidden roles from the surviving sentence:

- `affected_thing`: the domain, user-facing, operator-facing thing, or bad outcome;
- `changed_consequence`: what is prevented, corrected, enabled, or otherwise changed; and
- `actor`: its ordinary-language actor, when present.

Set `effect_sentence` and these roles once. They are the subject's source of truth.

## Compress without drift

Compress by deleting detail. Reorder only while preserving the same everyday nouns and verbs, or
obvious everyday synonyms. Never substitute technical precision for ordinary meaning.

Validate in this order:

1. Preserve `affected_thing`, `changed_consequence`, and `actor`.
2. Keep the head noun on the affected thing and the main verb on the changed consequence.
3. Keep qualifiers on the changed consequence or shipping circumstance, not an implementation
   step, access path, lifecycle, or method.
4. Reject internal containers, storage units, local artifacts, components, code paths, helpers,
   algorithms, architecture terms, transport or lifecycle mechanics, file or function identifiers,
   and engineering shorthand unless the experienced problem genuinely requires the term.
5. Validate multiword noun phrases as well as individual words. Reject any phrase that depends on
   implementation knowledge, answers "what code changed?", or changes a settled role.
6. Score every content word for jargon proximity: `0` everyday, `1` slightly specialized but
   natural to a non-technical reader, `2` mildly technical, `3` engineering jargon, `4` strong
   implementation terminology, `5` highly technical term of art.
7. Accept only scores `0` and `1`. If any word scores `2` or higher, return to `effect_sentence` and
   rewrite.
8. Return the first subject that passes role, shape, phrase, and word checks while preserving the
   experienced effect.

Shape outranks isolated vocabulary. Plain words still fail when arranged around implementation
knowledge. If the subject and `effect_sentence` concern different nouns or actors, or the subject
answers what code changed rather than what differs after shipping, rewrite from `effect_sentence`
instead of editing the failed subject.

Keep the settled `scope`, when present, unchanged while applying these checks to the description. Difficulty
finding plain wording is never permission to leave this branch or change `type`. When uncertain,
prefer the version a release reader understands fastest without extra context.
