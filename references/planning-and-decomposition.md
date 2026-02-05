# Planning and Decomposition

## Goal

Convert imperative stored procedure logic into a declarative dbt DAG.

## Decomposition Checklist

1. List all source tables and final outputs.
2. Identify temp tables and intermediate artifacts.
3. Identify DML operations (INSERT, UPDATE, DELETE, MERGE).
4. Group logic into transformation units.
5. Define grain and primary key for each output.

## Mapping Rules

- Source tables → `source()` + staging models
- Temp tables → intermediate models
- Final outputs → marts

## Grain Statement (Required)

For each mart, write:

> Grain: one row per \<entity\> per \<time or dimension\>

If the grain cannot be stated clearly, correctness cannot be validated.

## Naming Guidance

- Staging: `stg_<source>__<entity>s`
- Intermediate: `int_<entity>s_<verb>`
- Marts: plural business entities