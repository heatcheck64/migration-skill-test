# Sample Pipeline A - File Summary

## Directory Structure

```
sample_pipelines/sample_a/
├── sprocs/
│   ├── sp_load_staging_orders.sql (124 lines)
│   ├── sp_load_staging_customers.sql (50 lines)
│   ├── sp_load_staging_products.sql (44 lines)
│   ├── sp_build_order_enriched.sql (155 lines)
│   ├── sp_build_daily_sales_summary.sql (145 lines)
│   ├── sp_build_monthly_customer_summary.sql (131 lines)
│   └── sp_build_product_performance_multi.sql (234 lines)
├── README.md (comprehensive documentation)
└── DEPENDENCIES.md (visual dependency graph)
```

## Created Files

### Stored Procedures (7 total)

1. **sp_load_staging_orders.sql**
   - Stages raw orders with late-arriving data handling
   - MERGE/upsert pattern
   - Status standardization, line total calculation
   - 90-day lookback window

2. **sp_load_staging_customers.sql**
   - Stages customer dimension
   - Full refresh pattern (TRUNCATE + INSERT)
   - Country code standardization, email normalization

3. **sp_load_staging_products.sql**
   - Stages product dimension
   - Full refresh pattern
   - Margin calculation, default values

4. **sp_build_order_enriched.sql**
   - Enriches orders with customer/product dimensions
   - 3-way join across staging tables
   - MERGE/upsert pattern
   - Date dimension breakout, profit calculations

5. **sp_build_daily_sales_summary.sql**
   - Daily sales aggregation
   - Window functions: ROW_NUMBER(), SUM() OVER(), AVG() OVER()
   - Running totals and 7-day moving averages
   - Multi-dimensional grain (date × category × segment)
   - CTEs for complex transformations

6. **sp_build_monthly_customer_summary.sql**
   - Monthly customer metrics
   - Customer-level aggregation (different grain)
   - RFM-style tiering logic
   - Rankings within segments
   - Recency tracking

7. **sp_build_product_performance_multi.sql**
   - **SPECIAL CASE**: Single SPROC → 2 output tables
   - Creates `product_daily_metrics` (daily grain)
   - Creates `product_summary` (all-time aggregate)
   - Performance tier classification, velocity calculations

### Documentation Files (2 total)

1. **README.md**
   - Complete source table schemas (orders, customers, products)
   - Data characteristics and quality issues
   - SPROC execution order and dependencies
   - Expected output schema
   - Complexity features matrix
   - Business logic summary
   - dbt migration considerations

2. **DEPENDENCIES.md**
   - Visual ASCII dependency graph
   - Execution groups (parallel vs sequential)
   - Dependency matrix table
   - Expected dbt model lineage

## Complexity Features Coverage

✓ Temp table chains (all SPROCs)
✓ MERGE/UPSERT operations (5 SPROCs)
✓ Window functions (2 SPROCs: daily_sales, monthly_customer)
✓ CTEs (3 SPROCs: daily_sales, monthly_customer, product_multi)
✓ Conditional logic / CASE statements (all SPROCs)
✓ Multi-grain aggregations (daily × category × segment, monthly × customer)
✓ Late-arriving data handling (staging_orders: 90-day lookback)
✓ Cross-table dependencies (order_enriched: 3-way join)
✓ Single SPROC → multiple tables (product_performance_multi: 2 outputs)

## Testing the Agent

When running the migration agent:

1. **Input**: Point agent to `/sample_pipelines/sample_a/`
2. **Expected Output**: dbt project at `/sample_pipelines/sample_a/dbt_project/`
3. **Expected Model Count**: 8-9 dbt models
   - 3 staging models (stg_orders, stg_customers, stg_products)
   - 5-6 mart models (order_enriched, daily_sales_summary, monthly_customer_summary, product_daily_metrics, product_summary)

4. **Agent Should**:
   - Recognize staging SPROCs and create 1:1 staging models
   - Decompose product_performance_multi into 2 separate models
   - Use incremental materialization for MERGE patterns
   - Extract unique_key from MERGE ON clauses
   - Create source definitions in sources.yml
   - Add appropriate tests (unique, not_null on PKs)
   - Generate migration report

5. **Validation Points**:
   - Check if staging models use source() function correctly
   - Verify incremental models have unique_key configured
   - Confirm window functions translated correctly
   - Ensure model lineage matches SPROC dependencies
   - Review whether late-arriving data pattern is documented

## Quick Stats

- Total SPROCs: 7
- Total Output Tables: 8 (one SPROC creates 2 tables)
- Total Lines of SQL: ~883 lines
- Dependency Layers: 3 (staging → intermediate → marts)
- Parallel Execution Groups: 2 (staging layer, marts layer)
