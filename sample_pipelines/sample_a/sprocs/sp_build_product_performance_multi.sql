-- =============================================
-- SPROC: sp_build_product_performance_multi
-- Purpose: Single SPROC that creates multiple analytics objects
--          Demonstrates pattern where one SPROC populates multiple tables
-- Dependencies: 
--   - analytics.order_enriched
--   - staging.stg_products
-- Outputs: 
--   - analytics.product_daily_metrics (daily grain)
--   - analytics.product_summary (aggregate view)
-- =============================================
CREATE PROCEDURE sp_build_product_performance_multi
    @days_back INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_date DATE = DATEADD(DAY, -@days_back, CAST(GETDATE() AS DATE));
    DECLARE @end_date DATE = CAST(GETDATE() AS DATE);
    
    -- ========================================
    -- OUTPUT 1: Product Daily Metrics
    -- ========================================
    
    IF OBJECT_ID('tempdb..#temp_product_daily') IS NOT NULL
        DROP TABLE #temp_product_daily;
    
    WITH daily_product_sales AS (
        SELECT 
            o.product_id,
            o.product_name,
            o.product_category,
            o.product_subcategory,
            o.order_date,
            
            -- Metrics
            COUNT(DISTINCT o.order_id) AS order_count,
            SUM(o.quantity) AS units_sold,
            SUM(o.line_total) AS revenue,
            SUM(o.line_profit) AS profit,
            AVG(o.unit_price) AS avg_selling_price,
            
            -- Discount analysis
            AVG(o.discount_percent) AS avg_discount_pct,
            SUM(CASE WHEN o.has_discount = 1 THEN 1 ELSE 0 END) AS discounted_orders,
            
            GETDATE() AS etl_inserted_at
        FROM analytics.order_enriched o
        WHERE o.order_date BETWEEN @start_date AND @end_date
            AND o.is_completed = 1
        GROUP BY 
            o.product_id,
            o.product_name,
            o.product_category,
            o.product_subcategory,
            o.order_date
    )
    SELECT *
    INTO #temp_product_daily
    FROM daily_product_sales;
    
    -- MERGE into product_daily_metrics table
    MERGE INTO analytics.product_daily_metrics AS target
    USING #temp_product_daily AS source
        ON target.product_id = source.product_id
        AND target.order_date = source.order_date
    WHEN MATCHED THEN
        UPDATE SET
            product_name = source.product_name,
            product_category = source.product_category,
            product_subcategory = source.product_subcategory,
            order_count = source.order_count,
            units_sold = source.units_sold,
            revenue = source.revenue,
            profit = source.profit,
            avg_selling_price = source.avg_selling_price,
            avg_discount_pct = source.avg_discount_pct,
            discounted_orders = source.discounted_orders,
            etl_updated_at = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            product_id, product_name, product_category, product_subcategory,
            order_date, order_count, units_sold, revenue, profit,
            avg_selling_price, avg_discount_pct, discounted_orders,
            etl_inserted_at, etl_updated_at
        )
        VALUES (
            source.product_id, source.product_name, source.product_category, 
            source.product_subcategory, source.order_date, source.order_count, 
            source.units_sold, source.revenue, source.profit,
            source.avg_selling_price, source.avg_discount_pct, 
            source.discounted_orders, source.etl_inserted_at, GETDATE()
        );
    
    -- ========================================
    -- OUTPUT 2: Product Summary (all-time)
    -- ========================================
    
    IF OBJECT_ID('tempdb..#temp_product_summary') IS NOT NULL
        DROP TABLE #temp_product_summary;
    
    WITH product_totals AS (
        SELECT 
            product_id,
            product_name,
            product_category,
            SUM(units_sold) AS total_units_sold,
            SUM(revenue) AS total_revenue,
            SUM(profit) AS total_profit,
            AVG(avg_selling_price) AS overall_avg_price,
            COUNT(DISTINCT order_date) AS days_with_sales,
            MAX(order_date) AS last_sale_date,
            MIN(order_date) AS first_sale_date
        FROM #temp_product_daily
        GROUP BY product_id, product_name, product_category
    ),
    product_enriched AS (
        SELECT 
            pt.*,
            p.list_price,
            p.margin_percent AS product_margin_pct,
            p.is_active,
            
            -- Performance indicators
            CASE 
                WHEN pt.total_revenue >= 50000 THEN 'Star'
                WHEN pt.total_revenue >= 10000 THEN 'Strong'
                WHEN pt.days_with_sales >= 30 THEN 'Steady'
                ELSE 'Low'
            END AS performance_tier,
            
            -- Calculate velocity
            CASE 
                WHEN DATEDIFF(DAY, pt.first_sale_date, pt.last_sale_date) > 0
                THEN CAST(pt.total_units_sold AS FLOAT) / 
                     DATEDIFF(DAY, pt.first_sale_date, pt.last_sale_date)
                ELSE 0
            END AS avg_daily_velocity,
            
            GETDATE() AS etl_inserted_at
        FROM product_totals pt
        LEFT JOIN staging.stg_products p
            ON pt.product_id = p.product_id
    )
    SELECT *
    INTO #temp_product_summary
    FROM product_enriched;
    
    -- MERGE into product_summary table
    MERGE INTO analytics.product_summary AS target
    USING #temp_product_summary AS source
        ON target.product_id = source.product_id
    WHEN MATCHED THEN
        UPDATE SET
            product_name = source.product_name,
            product_category = source.product_category,
            total_units_sold = source.total_units_sold,
            total_revenue = source.total_revenue,
            total_profit = source.total_profit,
            overall_avg_price = source.overall_avg_price,
            days_with_sales = source.days_with_sales,
            last_sale_date = source.last_sale_date,
            first_sale_date = source.first_sale_date,
            list_price = source.list_price,
            product_margin_pct = source.product_margin_pct,
            is_active = source.is_active,
            performance_tier = source.performance_tier,
            avg_daily_velocity = source.avg_daily_velocity,
            etl_updated_at = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            product_id, product_name, product_category,
            total_units_sold, total_revenue, total_profit,
            overall_avg_price, days_with_sales, last_sale_date,
            first_sale_date, list_price, product_margin_pct,
            is_active, performance_tier, avg_daily_velocity,
            etl_inserted_at, etl_updated_at
        )
        VALUES (
            source.product_id, source.product_name, source.product_category,
            source.total_units_sold, source.total_revenue, source.total_profit,
            source.overall_avg_price, source.days_with_sales, source.last_sale_date,
            source.first_sale_date, source.list_price, source.product_margin_pct,
            source.is_active, source.performance_tier, source.avg_daily_velocity,
            source.etl_inserted_at, GETDATE()
        );
    
    -- Log completion
    SELECT 
        'sp_build_product_performance_multi' AS sproc_name,
        'product_daily_metrics' AS output_table,
        COUNT(*) AS rows_processed
    FROM #temp_product_daily
    UNION ALL
    SELECT 
        'sp_build_product_performance_multi' AS sproc_name,
        'product_summary' AS output_table,
        COUNT(*) AS rows_processed
    FROM #temp_product_summary;
    
END;
