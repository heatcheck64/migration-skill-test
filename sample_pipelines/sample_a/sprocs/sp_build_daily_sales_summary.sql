-- =============================================
-- SPROC: sp_build_daily_sales_summary
-- Purpose: Aggregate daily sales metrics with rankings
-- Dependencies: analytics.order_enriched
-- Output: analytics.daily_sales_summary
-- =============================================
CREATE PROCEDURE sp_build_daily_sales_summary
    @start_date DATE = NULL,
    @end_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Default date range: last 90 days
    IF @start_date IS NULL
        SET @start_date = DATEADD(DAY, -90, CAST(GETDATE() AS DATE));
    IF @end_date IS NULL
        SET @end_date = CAST(GETDATE() AS DATE);
    
    -- Temp table for daily aggregation
    IF OBJECT_ID('tempdb..#temp_daily') IS NOT NULL
        DROP TABLE #temp_daily;
    
    -- Aggregate to daily level with window functions
    WITH daily_metrics AS (
        SELECT 
            order_date,
            product_category,
            customer_segment,
            
            -- Aggregations
            COUNT(DISTINCT order_id) AS order_count,
            COUNT(DISTINCT customer_id) AS unique_customers,
            SUM(line_total) AS total_revenue,
            SUM(line_profit) AS total_profit,
            AVG(line_total) AS avg_order_value,
            SUM(quantity) AS total_quantity,
            
            -- Completed orders only
            SUM(CASE WHEN is_completed = 1 THEN line_total ELSE 0 END) AS completed_revenue,
            SUM(CASE WHEN is_completed = 1 THEN 1 ELSE 0 END) AS completed_order_count
        FROM analytics.order_enriched
        WHERE order_date BETWEEN @start_date AND @end_date
            AND standardized_status <> 'cancelled'
        GROUP BY 
            order_date,
            product_category,
            customer_segment
    ),
    -- Add window functions for rankings and running totals
    enriched_metrics AS (
        SELECT 
            order_date,
            product_category,
            customer_segment,
            order_count,
            unique_customers,
            total_revenue,
            total_profit,
            avg_order_value,
            total_quantity,
            completed_revenue,
            completed_order_count,
            
            -- Rankings
            ROW_NUMBER() OVER (
                PARTITION BY order_date 
                ORDER BY total_revenue DESC
            ) AS revenue_rank_by_day,
            
            -- Running totals within category
            SUM(total_revenue) OVER (
                PARTITION BY product_category 
                ORDER BY order_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS category_running_revenue,
            
            -- Moving averages
            AVG(total_revenue) OVER (
                PARTITION BY product_category
                ORDER BY order_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS category_7day_avg_revenue,
            
            -- Metadata
            GETDATE() AS etl_inserted_at
        FROM daily_metrics
    )
    SELECT *
    INTO #temp_daily
    FROM enriched_metrics;
    
    -- MERGE into summary table
    MERGE INTO analytics.daily_sales_summary AS target
    USING #temp_daily AS source
        ON target.order_date = source.order_date
        AND target.product_category = source.product_category
        AND target.customer_segment = source.customer_segment
    WHEN MATCHED THEN
        UPDATE SET
            order_count = source.order_count,
            unique_customers = source.unique_customers,
            total_revenue = source.total_revenue,
            total_profit = source.total_profit,
            avg_order_value = source.avg_order_value,
            total_quantity = source.total_quantity,
            completed_revenue = source.completed_revenue,
            completed_order_count = source.completed_order_count,
            revenue_rank_by_day = source.revenue_rank_by_day,
            category_running_revenue = source.category_running_revenue,
            category_7day_avg_revenue = source.category_7day_avg_revenue,
            etl_updated_at = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            order_date, product_category, customer_segment,
            order_count, unique_customers, total_revenue, total_profit,
            avg_order_value, total_quantity, completed_revenue, 
            completed_order_count, revenue_rank_by_day,
            category_running_revenue, category_7day_avg_revenue,
            etl_inserted_at, etl_updated_at
        )
        VALUES (
            source.order_date, source.product_category, source.customer_segment,
            source.order_count, source.unique_customers, source.total_revenue, 
            source.total_profit, source.avg_order_value, source.total_quantity, 
            source.completed_revenue, source.completed_order_count, 
            source.revenue_rank_by_day, source.category_running_revenue, 
            source.category_7day_avg_revenue,
            source.etl_inserted_at, GETDATE()
        );
    
END;
