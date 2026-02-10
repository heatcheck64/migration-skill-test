# Parity Validation

Ask the user which strategy to use during the **Inputs** phase. Follow the appropriate section below.

---

## Option 1: Audit Helper (Full Data Comparison)

**Prerequisite:** The warehouse user has SELECT access to legacy output tables.

### Setup

Ensure `dbt-audit-helper` is in the project's `packages.yml`:

```yaml
packages:
  - package: dbt-labs/audit_helper
    version: [">=0.12.0", "<1.0.0"]
```

Run `dbt deps` to install.

### Comparison Macros

Use `dbt show --inline` to run audit_helper macros without creating files.

**1. Row-level comparison (`compare_relations`)**

```sql
{%- set old_relation = adapter.get_relation(
    database="<legacy_db>",
    schema="<legacy_schema>",
    identifier="<legacy_table>"
) -%}

{%- set new_relation = ref('<new_dbt_model>') -%}

{{ audit_helper.compare_relations(
    a_relation=old_relation,
    b_relation=new_relation,
    primary_key="<primary_key>"
) }}
```

This returns a summary of rows that match, are only in the old table, or only in the new table.

**2. Column-level match rates (`compare_all_columns`)**

```sql
{%- set old_relation = adapter.get_relation(
    database="<legacy_db>",
    schema="<legacy_schema>",
    identifier="<legacy_table>"
) -%}

{%- set new_relation = ref('<new_dbt_model>') -%}

{{ audit_helper.compare_all_columns(
    a_relation=old_relation,
    b_relation=new_relation,
    primary_key="<primary_key>"
) }}
```

This returns per-column match percentages.

**3. Single column drill-down (`compare_column_values`)**

```sql
{%- set old_relation = adapter.get_relation(
    database="<legacy_db>",
    schema="<legacy_schema>",
    identifier="<legacy_table>"
) -%}

{%- set new_relation = ref('<new_dbt_model>') -%}

{{ audit_helper.compare_column_values(
    a_relation=old_relation,
    b_relation=new_relation,
    primary_key="<primary_key>",
    column_to_compare="<column_name>"
) }}
```

**4. Schema comparison (`compare_relation_columns`)**

```sql
{%- set old_relation = adapter.get_relation(
    database="<legacy_db>",
    schema="<legacy_schema>",
    identifier="<legacy_table>"
) -%}

{%- set new_relation = ref('<new_dbt_model>') -%}

{{ audit_helper.compare_relation_columns(
    a_relation=old_relation,
    b_relation=new_relation
) }}
```

### How to Run

Run each comparison with `dbt show`:

```bash
dbt show --inline "<sql_from_above>" --limit 50
```

### Interpreting Results

- **100% match on all columns** → parity confirmed
- **Row count mismatch** → investigate join fanout, filter differences, or deduplication logic
- **Column value mismatches** → drill down with `compare_column_values` on the failing column
- **Schema differences** → check for renamed columns, type casting differences, or missing columns

### Common Mismatch Causes

- Join fanout
- Filter differences
- Null-handling differences
- Timezone casting
- Deduplication logic

---

## Option 2: Schema-Only Validation

**Use when:** The warehouse user does NOT have access to legacy output tables, but can provide DDL, documentation, or column metadata.

### What to Compare

1. **Column names** — Every column in the legacy output should exist in the new model (or be explicitly documented as dropped/renamed)
2. **Column types** — Data types should match or be compatible (e.g. `VARCHAR(100)` → `TEXT` is acceptable; `INT` → `VARCHAR` is not without justification)
3. **Primary key / grain** — The grain of the new model matches the documented grain of the legacy output
4. **Nullability** — Columns that were NOT NULL in legacy should remain NOT NULL

### How to Validate

Ask the user to provide one of:
- `CREATE TABLE` DDL for the legacy output
- Column list from documentation or data catalog
- Screenshot or export of table schema

Then compare against the dbt model's compiled SQL and YAML column definitions.

Use `dbt show` to inspect the new model's output:

```bash
dbt show --select <model> --limit 5
```

### Output

Summarize in the conversation:
- Columns matched / missing / added
- Type mismatches and whether they are acceptable
- Grain confirmation

---

## Option 3: Skip Validation

**Use when:** No access to legacy tables AND no schema documentation is available.

### What to Do

1. **Document the skip** — Clearly state in the conversation that parity validation was not performed and why
2. **Provide a self-validation guide** — Give the user instructions they can follow later when they have access:

#### Self-Validation Guide for the User

> To validate parity after migration, follow these steps:
>
> 1. **Install audit_helper:** Add `dbt-labs/audit_helper` to your `packages.yml` and run `dbt deps`
> 2. **Run `compare_relations`** between the legacy table and your new dbt model using the primary key
> 3. **Run `compare_all_columns`** to check per-column match rates
> 4. **Check row counts:** `SELECT COUNT(*) FROM <legacy_table>` vs `dbt show --inline "SELECT COUNT(*) FROM {{ ref('<new_model>') }}"`
> 5. **Spot-check aggregates:** Compare SUM/AVG on key numeric columns between old and new
> 6. **Investigate any mismatches** using `compare_column_values` on failing columns
>
> Common mismatch causes: join fanout, filter differences, null handling, timezone casting, deduplication logic.

3. **Add TODO comments** in the model YAML as reminders:

```yaml
# TODO: Parity validation pending — requires access to <legacy_table>
```
