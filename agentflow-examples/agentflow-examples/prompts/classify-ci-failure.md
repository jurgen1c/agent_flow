Classify the CI failure from the provided failure payload and logs.

Return JSON with:
- kind
- confidence
- summary
- recommended_owner
- safe_to_retry
- requires_user

Known kinds:
- flake
- formatting_error
- implementation_error
- environment_error
- missing_requirement
- unsafe_change
- unknown

