# Sample Pipeline A: Sales Analytics

## Overview

A realistic sales analytics pipeline that processes orders, customers, and products data through multiple transformation stages. This pipeline demonstrates common patterns found in legacy stored procedure architectures.

## Source Tables

### Raw Data Schema (`raw_data`)

#### `raw_data.orders`
Raw transactional order data with potential quality issues and inconsistent status values.

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | INT | Primary key, unique order identifier |
| `customer_id` | INT | Foreign key to customers |
| `product_id` | INT | Foreign key to products |
| `order_date` | DATE | Date order was placed |
| `shipped_date` | DATE | Date order was shipped (nullable) |
| `quantity` | INT | Units ordered |
| `unit_price` | DECIMAL(10,2) | Price per unit |
| `discount_percent` | DECIMAL(5,2) | Discount applied (0-100) |
| `order_status` | VARCHAR(50) | Status: 'complete', 'pending', 'cancelled', etc. (inconsistent values) |
| `created_at` | DATETIME | Record creation timestamp |
| `updated_at` | DATETIME | Record last updated timestamp |

**Data Characteristics**:
- Late-arriving data: Orders can be backdated up to 90 days
- Inconsistent status values need standardization
- Some records may have null or invalid data

#### `raw_data.customers`
Customer master data (dimension).

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | INT | Primary key |
| `customer_name` | VARCHAR(200) | Full name (may have whitespace issues) |
| `email` | VARCHAR(200) | Email address (inconsistent casing) |
| `phone` | VARCHAR(50) | Phone number |
| `city` | VARCHAR(100) | City |
| `state` | VARCHAR(100) | State/province |
| `country` | VARCHAR(100) | Country (inconsistent naming) |
| `customer_segment` | VARCHAR(50) | Segment: 'Enterprise', 'SMB', 'Consumer' |
| `signup_date` | DATE | Customer signup date |
| `is_active` | BIT | Active status flag |

**Data Characteristics**:
- Country names need standardization to ISO codes
- Email addresses need lowercasing
- Whitespace trimming needed on text fields

#### `raw_data.products`
Product catalog (dimension).

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | INT | Primary key |
| `product_name` | VARCHAR(200) | Product name |
| `category` | VARCHAR(100) | Product category |
| `subcategory` | VARCHAR(100) | Product subcategory |
| `brand` | VARCHAR(100) | Brand name |
| `list_price` | DECIMAL(10,2) | Current list price |
| `cost_price` | DECIMAL(10,2) | Cost to company (for margin calc) |
| `is_active` | BIT | Currently active product |
| `discontinued_date` | DATE | Date discontinued (nullable) |

**Data Characteristics**:
- Margin calculation needed (list_price - cost_price)
- Category may be null (needs default)

## Stored Procedure Pipeline

### Execution Order and Dependencies

```
1. sp_load_staging_orders         (depends on: raw_data.orders)
2. sp_load_staging_customers      (depends on: raw_data.customers)
3. sp_load_staging_products       (depends on: raw_data.products)
   ↓
4. sp_build_order_enriched        (depends on: 1, 2, 3)
   ↓
5. sp_build_daily_sales_summary   (depends on: 4)
6. sp_build_monthly_customer_summary (depends on: 4)
7. sp_build_product_performance_multi (depends on: 4, 3)
```

### SPROC Details

#### 1. `sp_load_staging_orders`
**Purpose**: Stage and clean raw orders data  
**Pattern**: Incremental load with MERGE (upsert)  
**Parameters**: `@load_date` (optional, defaults to today)  
**Output**: `staging.stg_orders`

**Key Features**:
- Late-arriving data handling (90-day lookback window)
- Status value standardization
- Line total calculation
- Data quality filtering (nulls, quantity > 0)
- MERGE pattern for upserts

#### 2. `sp_load_staging_customers`
**Purpose**: Stage and standardize customer dimension  
**Pattern**: Full refresh (TRUNCATE + INSERT)  
**Parameters**: None  
**Output**: `staging.stg_customers`

**Key Features**:
- Country code standardization (US, GB, CA)
- Email lowercasing and trimming
- Default value handling for nulls

#### 3. `sp_load_staging_products`
**Purpose**: Stage product dimension with calculated fields  
**Pattern**: Full refresh (TRUNCATE + INSERT)  
**Parameters**: None  
**Output**: `staging.stg_products`

**Key Features**:
- Margin percentage calculation
- Default category for uncategorized products
- NULL coalescing for flags

#### 4. `sp_build_order_enriched`
**Purpose**: Create enriched fact table with dimension attributes  
**Pattern**: Incremental with MERGE, joins staging tables  
**Parameters**: `@load_date` (optional)  
**Output**: `analytics.order_enriched`

**Key Features**:
- Multi-table joins (orders + customers + products)
- Calculated fields (days_to_ship, line_profit)
- Date dimension breakout (year, month, day, quarter, day of week)
- Conditional logic for flags (has_discount, is_completed)
- Temp table for intermediate processing

#### 5. `sp_build_daily_sales_summary`
**Purpose**: Daily sales aggregation with window functions  
**Pattern**: Incremental with MERGE, CTEs  
**Parameters**: `@start_date`, `@end_date` (optional, defaults to last 90 days)  
**Output**: `analytics.daily_sales_summary`

**Key Features**:
- Multi-dimensional aggregation (date × category × segment)
- Window functions:
  - `ROW_NUMBER()` for rankings
  - Running totals with `SUM() OVER()`
  - 7-day moving averages
- Multiple CTEs for complex transformations

#### 6. `sp_build_monthly_customer_summary`
**Purpose**: Monthly customer metrics and segmentation  
**Pattern**: Incremental with MERGE, multi-grain aggregation  
**Parameters**: `@year`, `@month` (optional, defaults to current month)  
**Output**: `analytics.monthly_customer_summary`

**Key Features**:
- Customer-level aggregation (different grain than orders)
- RFM-style customer tiering (VIP, High Value, Regular, New)
- Completion rate calculation
- Rankings within customer segment
- Recency tracking (first/last order dates)

#### 7. `sp_build_product_performance_multi`
**Purpose**: Product analytics - single SPROC creating multiple outputs  
**Pattern**: Incremental with MERGE, **multiple output tables**  
**Parameters**: `@days_back` (default 90)  
**Outputs**: 
- `analytics.product_daily_metrics` (daily grain)
- `analytics.product_summary` (aggregate)

**Key Features**:
- **Single SPROC writes to 2 different tables** (common legacy pattern)
- Daily metrics aggregation
- All-time summary statistics
- Performance tier classification
- Velocity calculations (units per day)
- Multiple temp tables and MERGE operations

## Expected Output Schema

### Staging Tables (`staging`)

- `staging.stg_orders` - Cleaned orders (1:1 with source, plus calculated fields)
- `staging.stg_customers` - Standardized customers (1:1 with source)
- `staging.stg_products` - Enhanced products (1:1 with source)

### Analytics Tables (`analytics`)

- `analytics.order_enriched` - Denormalized order facts with dimension attributes
- `analytics.daily_sales_summary` - Daily aggregates with rankings and moving averages
- `analytics.monthly_customer_summary` - Monthly customer-level metrics
- `analytics.product_daily_metrics` - Daily product sales metrics
- `analytics.product_summary` - All-time product performance summary

## Complexity Features Demonstrated

| Feature | SPROC(s) |
|---------|----------|
| Temp table chains | All SPROCs use #temp tables |
| MERGE/UPSERT operations | sp_load_staging_orders, sp_build_order_enriched, sp_build_daily_sales_summary, sp_build_monthly_customer_summary, sp_build_product_performance_multi |
| Window functions (rankings, running totals) | sp_build_daily_sales_summary, sp_build_monthly_customer_summary |
| CTEs | sp_build_daily_sales_summary, sp_build_monthly_customer_summary, sp_build_product_performance_multi |
| Conditional logic (CASE statements) | All staging SPROCs, sp_build_order_enriched |
| Multi-grain aggregations | sp_build_daily_sales_summary (daily × category × segment), sp_build_monthly_customer_summary (monthly × customer) |
| Late-arriving data handling | sp_load_staging_orders (90-day lookback) |
| Cross-table dependencies | sp_build_order_enriched joins 3 staging tables |
| Single SPROC → multiple tables | sp_build_product_performance_multi (creates 2 output tables) |

## Business Logic Summary

**Staging Layer**: 
- Cleanse and standardize raw data
- Apply business rules (status standardization, country codes)
- Calculate derived fields (line_total, margin_percent)
- Handle data quality issues

**Analytics Layer**:
- Enrich orders with dimensional context
- Aggregate at multiple grains (daily, monthly)
- Calculate business metrics (revenue, profit, customer tiers)
- Apply rankings and time-series analytics (moving averages, running totals)

## dbt Migration Considerations

When migrating to dbt, expect:

1. **Staging models** (one per source table):
   - `stg_orders.sql`
   - `stg_customers.sql` 
   - `stg_products.sql`

2. **Intermediate models** for complex logic:
   - May need intermediate models to break down multi-step transformations

3. **Mart models** for final outputs:
   - `order_enriched.sql` (incremental)
   - `daily_sales_summary.sql` (incremental)
   - `monthly_customer_summary.sql` (incremental)
   - `product_daily_metrics.sql` (incremental)
   - `product_summary.sql` (incremental or view)

4. **Special considerations**:
   - `sp_build_product_performance_multi` creates 2 tables → split into 2 separate dbt models
   - MERGE patterns → dbt incremental models with `unique_key`
   - Window functions → translate directly to dbt SQL
   - Late-arriving data → incremental strategy with lookback window
   - Parameters → use dbt vars or macros

5. **Source definitions** in `sources.yml` for all `raw_data.*` tables
