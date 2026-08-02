Classify the CI failure from the provided failure payload and logs.

Return JSON with:
- kind
- confidence
- summary
- recommended_owner
- safe_to_retry
- requires_user

Return exactly these six fields. Use `low`, `medium`, or `high` for confidence,
non-empty text for summary and recommended_owner, and JSON booleans for
safe_to_retry and requires_user.

Known kinds:
- flake
- formatting_error
- implementation_error
- environment_error
- missing_requirement
- unsafe_change
- unknown
