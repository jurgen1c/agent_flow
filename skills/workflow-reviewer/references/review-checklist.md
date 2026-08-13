# Workflow review checklist

## Schema and references

- Parse and validate the YAML. Find unknown keys or step types, missing required fields, duplicate IDs, invalid expressions, and missing step, session, workflow, input, output, or artifact references.
- Confirm every artifact is produced before use and every overwrite is explicit.

## Dead paths and termination

- Use the graph and branch fixtures to find unreachable steps, branches that can never match, missing defaults, accidental cycles, and terminal states with outgoing work.
- Require explicit outcomes for failure, timeout, budget exhaustion, rejected approval, unresolved review, and disagreement.

## Unsafe policy

- Find destructive or download-and-execute commands, secret exposure, unconstrained file writes, unbounded loops/retries/model calls/duration, and consequential actions without approval.
- Confirm runtime-denied operations fail or pause closed and retention preserves required evidence.

## Ownership and authority

- Map every artifact, decision, and writable scope to one owner.
- Find writers without `can_modify_files`, reviewers without review authority, blockers or approvers without explicit authority, self-review, overlapping writers without reconciliation, and publication authority that bypasses policy.

## Report format

For each finding provide severity, location, evidence, impact, and minimal correction. If no defects remain, say which commands and fixture paths were checked and note any untested behavior.
