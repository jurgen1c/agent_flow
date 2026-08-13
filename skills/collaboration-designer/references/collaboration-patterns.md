# Collaboration patterns

Use collaboration only when separate roles materially improve the workflow. Set `collaboration.enabled: true`, a positive `max_review_cycles`, and a terminal `on_disagreement` policy.

Authority rules:

- writers require `can_modify_files: true` and an effective `file_scope`;
- formal reviewers require `can_request_changes: true` and `can_approve: true`;
- approval actors require `can_approve: true`;
- blocking advisers require `can_block: true`; ordinary advisers stay non-blocking;
- merge or publication authority must be explicit and policy-gated.

Ownership rules:

- declare who owns each produced artifact and decision;
- do not let two writers share an effective scope unless overlap and a conflict policy are explicit;
- keep review subjects distinct from reviewers;
- route changes requested back to the artifact owner;
- invalidate approvals when protected inputs change;
- give every disagreement a bounded resolution path and human escape hatch.

Use `explain` and `graph` to inspect normalized authority, ownership, and scopes without executing the workflow.
