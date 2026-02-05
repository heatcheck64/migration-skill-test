# Migration Report: <procedure_name>

**Date:**  
**Warehouse:**  
**Legacy Object:**  

## Summary
What the stored procedure did and how dbt replaces it.

## Inputs and Outputs

### Sources
- ...

### Targets
- ...

## Legacy → dbt Mapping

| Legacy Step | dbt Model | Layer | Notes |
|------------|-----------|-------|-------|

## Materializations

| Model | Materialization | Rationale |
|------|-----------------|-----------|

## Incremental Contract (if applicable)

- unique_key:
- strategy:
- cutoff logic:
- late-arriving handling:

## Tests Added

- PK tests:
- relationship tests:
- business rule tests:

## Parity Validation

### Checks Performed
- row counts
- aggregates
- samples

### Results
- pass/fail
- notes

## Assumptions and Risks
- assumptions
- known differences
- operational concerns
