# Sample Pipeline A: Dependency Graph

## Visual Execution Order

```
┌─────────────────────────────────────────────────────────────┐
│                      SOURCE TABLES                          │
│                     (raw_data schema)                       │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ raw_data.    │     │ raw_data.    │     │ raw_data.    │
│ orders       │     │ customers    │     │ products     │
└──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│      (1)     │     │      (2)     │     │      (3)     │
│ sp_load_     │     │ sp_load_     │     │ sp_load_     │
│ staging_     │     │ staging_     │     │ staging_     │
│ orders       │     │ customers    │     │ products     │
└──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ staging.     │     │ staging.     │     │ staging.     │
│ stg_orders   │     │ stg_customers│     │ stg_products │
└──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                              ▼
                     ┌──────────────┐
                     │      (4)     │
                     │ sp_build_    │
                     │ order_       │
                     │ enriched     │
                     └──────────────┘
                              │
                              ▼
                     ┌──────────────┐
                     │ analytics.   │
                     │ order_       │
                     │ enriched     │
                     └──────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│      (5)     │     │      (6)     │     │      (7)     │
│ sp_build_    │     │ sp_build_    │     │ sp_build_    │
│ daily_sales_ │     │ monthly_     │     │ product_     │
│ summary      │     │ customer_    │     │ performance_ │
│              │     │ summary      │     │ multi        │
└──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        ▼                     ▼                     ├──────┐
┌──────────────┐     ┌──────────────┐              │      │
│ analytics.   │     │ analytics.   │              ▼      ▼
│ daily_sales_ │     │ monthly_     │     ┌─────────────────────┐
│ summary      │     │ customer_    │     │ analytics.          │
└──────────────┘     │ summary      │     │ product_daily_      │
                     └──────────────┘     │ metrics             │
                                          └─────────────────────┘
                                                    │
                                                    ▼
                                          ┌─────────────────────┐
                                          │ analytics.          │
                                          │ product_summary     │
                                          └─────────────────────┘
```

## Execution Groups

### Group 1: Staging Layer (Parallel)
Can run in parallel - no interdependencies:
- `sp_load_staging_orders`
- `sp_load_staging_customers`
- `sp_load_staging_products`

### Group 2: Intermediate/Enrichment (Sequential)
Must wait for Group 1:
- `sp_build_order_enriched` (depends on all 3 staging tables)

### Group 3: Marts Layer (Parallel)
Can run in parallel after Group 2:
- `sp_build_daily_sales_summary` (depends on order_enriched)
- `sp_build_monthly_customer_summary` (depends on order_enriched)
- `sp_build_product_performance_multi` (depends on order_enriched + stg_products)

## Dependency Matrix

| SPROC | Depends On |
|-------|------------|
| sp_load_staging_orders | raw_data.orders |
| sp_load_staging_customers | raw_data.customers |
| sp_load_staging_products | raw_data.products |
| sp_build_order_enriched | staging.stg_orders, staging.stg_customers, staging.stg_products |
| sp_build_daily_sales_summary | analytics.order_enriched |
| sp_build_monthly_customer_summary | analytics.order_enriched |
| sp_build_product_performance_multi | analytics.order_enriched, staging.stg_products |

## dbt Model Lineage (Expected)

When migrated to dbt, the DAG should look like:

```
sources:
  - raw_orders
  - raw_customers
  - raw_products

staging models:
  - stg_orders (source: raw_orders)
  - stg_customers (source: raw_customers)
  - stg_products (source: raw_products)

intermediate/mart models:
  - order_enriched (refs: stg_orders, stg_customers, stg_products)
  - daily_sales_summary (ref: order_enriched)
  - monthly_customer_summary (ref: order_enriched)
  - product_daily_metrics (refs: order_enriched)
  - product_summary (refs: order_enriched, stg_products)
```

Key: The `sp_build_product_performance_multi` SPROC creates **two** output tables, so it should become **two** separate dbt models.
