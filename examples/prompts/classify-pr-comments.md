Classify PR comments from the provided GitHub PR state.

Return actionable comments only.

Ignore:
- Praise
- Non-blocking suggestions unless explicitly requested
- Already resolved comments

Output JSON with:
- count
- comments[]
- requires_user

