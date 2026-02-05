# Reference: Migrating from Stored Procedures to dbt

This guide follows official dbt Labs recommendations.

## Step 1: Paradigm Shift
Stored procedures are imperative.
dbt models are declarative and dependency-driven.

## Step 2: Plan the Migration
Identify logical transformation units and build a DAG.

## Step 3: Translate SQL Operations

- INSERT → model SELECT
- UPDATE → CASE logic in SELECT
- DELETE → filtered SELECT
- MERGE → incremental model

## Step 4: Refactor into Layers
Use staging, intermediate, and marts.

## Step 5: Audit Outputs
Validate parity with legacy results.

## Step 6: Iterate
Add tests, docs, macros, and refine materializations.

## Key Takeaways
- Use `ref()` and `source()`
- Avoid dynamic SQL
- Favor maintainability over procedural fidelity
