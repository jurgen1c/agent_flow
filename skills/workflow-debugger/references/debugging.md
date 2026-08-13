# Debugging reference

Use this order:

1. `validate` for parse, schema, reference, policy, and authority errors.
2. `lint` for risky but structurally valid authoring patterns.
3. `explain` to inspect normalized steps, artifacts, sessions, policy, and warnings.
4. `graph` to expose missing targets, cycles, ownership, and reachability.
5. fixture-backed `simulate` to reproduce one control-flow outcome without running steps.

Classify failures as:

- definition: malformed YAML, wrong type, missing field, duplicate ID;
- reference: missing or premature step, session, artifact, input, or workflow;
- control flow: dead branch, missing fallback, unintended cycle, missing terminal;
- policy: unsafe command, missing bound or approval, excessive scope;
- collaboration: missing authority, owner ambiguity, overlapping writers;
- fixture: missing outcome, invalid value, or branch coverage mismatch.

Do not silence a diagnostic by removing a required safety constraint. Show the minimal failing YAML and explain why the corrected invariant is valid.
