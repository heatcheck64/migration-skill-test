# Legacy Patterns → dbt Patterns

## Temp Tables
Default: each temp table becomes an intermediate model.

## MERGE / UPSERT
Translate to incremental models with:
- `unique_key`
- appropriate strategy (merge or delete+insert)

## INSERT
Convert `INSERT INTO ... SELECT ...` to a model containing the SELECT.

## UPDATE
Rewrite as a SELECT that derives updated column values (CASE expressions).

## DELETE
Filter out deleted rows declaratively.

## IF / ELSE
Prefer set-based SQL (UNION ALL + filters).
Use Jinja conditionals sparingly.

## Loops / Cursors
Redesign as set-based SQL or incremental batching.
Do not simulate loops in Jinja.

## Dynamic SQL
Prefer macros or model decomposition.
Avoid compile-time `run_query()` for business logic.

## Multiple Outputs
Each output becomes its own mart.
Share logic via intermediate models.
