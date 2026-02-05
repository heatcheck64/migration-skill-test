# Incremental and Upserts

## When to Use Incremental

Use incremental models when:
- Full refresh is too expensive
- Legacy logic is explicitly incremental

Prefer full refresh when possible.

## Required Inputs

- unique_key
- updated_at or change signal
- late-arriving data policy

## Safe Incremental Design

- Avoid pure `updated_at > max(updated_at)`
- Use lookback windows if late data exists
- Deduplicate within the increment if needed

## Documentation Requirements

For every incremental model, document:
- unique_key
- strategy
- cutoff logic
- late-arriving handling
