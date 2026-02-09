# Legacy Migration Agent Testing Project

## Overview

Testing and refining a dbt migration agent that converts stored procedure pipelines into well-structured dbt projects following best practices. Using a realistic but small sample pipeline (4-7 SPROCs) to iterate on agent behavior, rules, and output quality.

## Sample Pipeline: Sales Analytics (`sample_a`)

**Domain**: Sales data processing with orders, customers, and products  
**Location**: `/sample_pipelines/sample_a/`  
**Complexity Feature Examples**:
- Temp table chains (intermediate staging)
- MERGE/UPSERT operations (incremental updates)
- Window functions and CTEs (rankings, running totals)
- Conditional logic (IF/ELSE flows)
- Multi-grain aggregations (daily, monthly summaries)
- Late-arriving data handling (backdated orders)
- Cross-table dependencies (6-7 SPROCs forming a DAG)
- Single SPROC that creates multiple database objects

**Target**: Convert to dbt project at `/sample_pipelines/sample_a/dbt_project/`

## Execution Steps

### Phase 1: Setup Sample Pipeline ✓ (COMPLETED)

- [x] Create sample SPROC definitions in `/sample_pipelines/sample_a/sprocs/`
- [x] Document source tables and schemas in `/sample_pipelines/sample_a/README.md`
- [x] Define SPROC execution order and dependencies
- [x] Document expected outputs and business logic

**Summary**: Created 7 stored procedures covering all complexity features:
- 3 staging SPROCs (orders, customers, products) with MERGE/upsert patterns
- 1 intermediate SPROC (order_enriched) with 3-way joins
- 3 mart SPROCs with various aggregation grains and window functions
- Special case: `sp_build_product_performance_multi` creates 2 output tables from 1 SPROC

**Files Created**:
- `/sample_pipelines/sample_a/sprocs/` - 7 SQL files
- `/sample_pipelines/sample_a/README.md` - Complete documentation with schema definitions
- `/sample_pipelines/sample_a/DEPENDENCIES.md` - Visual dependency graph

**Ready for Phase 2**: Agent migration can begin

### Phase 2: Agent Migration (Iterate Here)

- [ ] Invoke migration agent with sample pipeline
- [ ] Review decomposition plan (staging → intermediate → marts)
- [ ] Validate model structure and naming conventions
- [ ] Check incremental strategy choices
- [ ] Review tests and documentation coverage

### Phase 3: Validation

- [ ] Run `dbt compile --select <models>` on generated project
- [ ] Check compilation errors and fix agent rules if needed
- [ ] Verify model lineage matches SPROC dependencies
- [ ] Review migration report quality

### Phase 4: Iteration

- [ ] Document issues found in "Lessons Learned" below
- [ ] Update agent rules in `SKILL.md` or reference guides
- [ ] Delete `/sample_pipelines/sample_a/dbt_project/`
- [ ] Re-run migration with updated agent
- [ ] Repeat until satisfied

## Validation Checklist

Quick checks before considering a migration successful:

- [ ] `dbt compile` runs without errors
- [ ] All models follow naming conventions (`stg_`, `int_`, mart names)
- [ ] Staging models are 1:1 with sources (no joins/aggregations)
- [ ] Source definitions use `source()` only in staging
- [ ] Primary keys have `unique` + `not_null` tests
- [ ] Incremental models have `unique_key` defined
- [ ] Model lineage DAG matches SPROC dependency order
- [ ] Migration report is complete and accurate

## Lessons Learned

### Phase 1 Setup
*Completed: Sample pipeline creation*

- **SPROC Design Choices**:
  - Created 7 SPROCs with realistic patterns (not overly complex)
  - Included all target complexity features: temp tables, MERGE/upsert, window functions, CTEs, multi-grain aggregations, late-arriving data, multi-table joins
  - Special attention to `sp_build_product_performance_multi` - single SPROC creating 2 output tables (common legacy pattern that should stress-test agent decomposition logic)
  
- **Documentation Quality**:
  - README includes complete source schemas with data characteristics
  - Documented execution order and dependencies clearly
  - Added DEPENDENCIES.md with visual DAG - should help agent understand flow
  - Expected dbt model structure documented for validation reference

- **Complexity Balance**:
  - Kept each SPROC focused but realistic (100-250 lines typical)
  - Avoided exotic SQL patterns that rarely appear in real migrations
  - Focus on bread-and-butter patterns: staging → enrichment → aggregation

- **Next Phase Expectations**:
  - Agent should decompose into ~8-9 dbt models (7 SPROCs → 8 models due to multi-output SPROC)
  - Staging models should be simple (1:1 with sources)
  - Incremental models should identify correct unique_key from MERGE ON clauses
  - Window functions and CTEs should translate cleanly to dbt SQL
  - Test if agent recognizes late-arriving data pattern and suggests appropriate lookback strategy

### Iteration 1
*Document what worked well, what broke, what needs fixing*

- **Agent Behavior**:
  - 
- **Output Quality**:
  - 
- **Rules to Add/Change**:
  - 

### Iteration 2
- **Agent Behavior**:
  - 
- **Output Quality**:
  - 
- **Rules to Add/Change**:
  - 

### Iteration 3
- **Agent Behavior**:
  - 
- **Output Quality**:
  - 
- **Rules to Add/Change**:
  - 

## Quick Reset Command

```bash
# Blow away the dbt project and start fresh
rm -rf sample_pipelines/sample_a/dbt_project/
```

## Notes

- Keep SPROCs realistic but simple (avoid production complexity)
- Focus on patterns that stress-test the agent rules
- Prioritize agent improvements that generalize to real migrations
- Update reference guides if discovering new edge cases
