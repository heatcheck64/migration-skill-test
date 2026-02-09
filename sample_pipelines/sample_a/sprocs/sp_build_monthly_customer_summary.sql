-- =============================================
-- SPROC: sp_build_monthly_customer_summary
-- Purpose: Multi-grain aggregation - monthly customer metrics
-- Dependencies: analytics.order_enriched
-- Output: analytics.monthly_customer_summary
-- =============================================
CREATE PROCEDURE sp_build_monthly_customer_summary
    @year INT = NULL,
    @month INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Default to current month if not specified
    IF @year IS NULL OR @month IS NULL
    BEGIN
        SET @year = YEAR(GETDATE());
        SET @month = MONTH(GETDATE());
    END;
    
    -- Temp table for monthly customer aggregation
    IF OBJECT_ID('tempdb..#temp_monthly') IS NOT NULL
        DROP TABLE #temp_monthly;
    
    WITH monthly_orders AS (
        SELECT 
            customer_id,
            customer_name,
            customer_segment,
            customer_country,
            order_year,
            order_month,
            order_id,
            line_total,
            line_profit,
            product_category,
            is_completed,
            order_date
        FROM analytics.order_enriched
        WHERE order_year = @year
            AND order_month = @month
            AND standardized_status <> 'cancelled'
    ),
    customer_metrics AS (
        SELECT 
            customer_id,
            customer_name,
            customer_segment,
            customer_country,
            @year AS year,
            @month AS month,
            
            -- Order metrics
            COUNT(DISTINCT order_id) AS order_count,
            SUM(line_total) AS total_revenue,
            SUM(line_profit) AS total_profit,
            AVG(line_total) AS avg_order_value,
            
            -- Product diversity
            COUNT(DISTINCT product_category) AS distinct_categories,
            
            -- Completion rate
            CAST(SUM(CASE WHEN is_completed = 1 THEN 1 ELSE 0 END) AS FLOAT) / 
                NULLIF(COUNT(*), 0) AS completion_rate,
            
            -- Recency
            MAX(order_date) AS last_order_date,
            MIN(order_date) AS first_order_date
        FROM monthly_orders
        GROUP BY 
            customer_id,
            customer_name,
            customer_segment,
            customer_country
    ),
    -- Add customer rankings
    ranked_customers AS (
        SELECT 
            *,
            -- Rank customers by revenue within segment
            ROW_NUMBER() OVER (
                PARTITION BY customer_segment 
                ORDER BY total_revenue DESC
            ) AS revenue_rank_in_segment,
            
            -- RFM-style segmentation
            CASE 
                WHEN total_revenue >= 10000 AND order_count >= 5 THEN 'VIP'
                WHEN total_revenue >= 5000 OR order_count >= 3 THEN 'High Value'
                WHEN order_count >= 2 THEN 'Regular'
                ELSE 'New'
            END AS customer_tier,
            
            GETDATE() AS etl_inserted_at
        FROM customer_metrics
    )
    SELECT *
    INTO #temp_monthly
    FROM ranked_customers;
    
    -- MERGE into monthly summary table
    MERGE INTO analytics.monthly_customer_summary AS target
    USING #temp_monthly AS source
        ON target.customer_id = source.customer_id
        AND target.year = source.year
        AND target.month = source.month
    WHEN MATCHED THEN
        UPDATE SET
            customer_name = source.customer_name,
            customer_segment = source.customer_segment,
            customer_country = source.customer_country,
            order_count = source.order_count,
            total_revenue = source.total_revenue,
            total_profit = source.total_profit,
            avg_order_value = source.avg_order_value,
            distinct_categories = source.distinct_categories,
            completion_rate = source.completion_rate,
            last_order_date = source.last_order_date,
            first_order_date = source.first_order_date,
            revenue_rank_in_segment = source.revenue_rank_in_segment,
            customer_tier = source.customer_tier,
            etl_updated_at = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id, customer_name, customer_segment, customer_country,
            year, month, order_count, total_revenue, total_profit,
            avg_order_value, distinct_categories, completion_rate,
            last_order_date, first_order_date, revenue_rank_in_segment,
            customer_tier, etl_inserted_at, etl_updated_at
        )
        VALUES (
            source.customer_id, source.customer_name, source.customer_segment, 
            source.customer_country, source.year, source.month, 
            source.order_count, source.total_revenue, source.total_profit,
            source.avg_order_value, source.distinct_categories, 
            source.completion_rate, source.last_order_date, source.first_order_date,
            source.revenue_rank_in_segment, source.customer_tier,
            source.etl_inserted_at, GETDATE()
        );
    
END;
