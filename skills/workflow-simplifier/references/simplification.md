# Simplification patterns

- Prefer `pipeline` over recovery or collaboration when no recovery route or independent authority is required.
- Remove sessions that add no distinct judgment, ownership, or provider need.
- Collapse pass-through conditions and artifact transforms only when the durable contract stays explicit.
- Merge duplicate policy declarations toward the narrowest shared scope.
- Remove unreachable steps and branches after graph and fixture evidence confirms they are dead.
- Replace repeated branches with one bounded route when their outputs and policy are identical.

Never simplify away:

- approval, authority, file-scope, secret, or unsafe-operation gates;
- retry, loop, duration, or model-call bounds;
- failure evidence, decision records, or required retention;
- explicit unresolved, exhausted, rejected, and disagreement outcomes;
- ownership boundaries needed to prevent conflicting writes.

Validate the final YAML and simulate every retained branch with fixtures. Simulation is traversal, not execution.
