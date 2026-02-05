# Parity Validation

## Required Checks

1. Row count parity
2. Aggregate parity (sums, counts)
3. Null-rate parity
4. Record-level spot checks

## Common Mismatch Causes

- Join fanout
- Filter differences
- Null-handling differences
- Timezone casting
- Deduplication logic

## Output

Record results in the migration report:
- checks performed
- pass/fail
- known differences
