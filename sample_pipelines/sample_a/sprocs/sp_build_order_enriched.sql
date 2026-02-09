-- =============================================
-- SPROC: sp_build_order_enriched
-- Purpose: Create enriched order fact table with customer/product details
-- Dependencies: 
--   - staging.stg_orders
--   - staging.stg_customers
--   - staging.stg_products
-- Output: analytics.order_enriched
-- =============================================
CREATE PROCEDURE sp_build_order_enriched
    @load_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @load_date IS NULL
        SET @load_date = CAST(GETDATE() AS DATE);
    
    -- Temp table for enriched orders
    IF OBJECT_ID('tempdb..#temp_enriched') IS NOT NULL
        DROP TABLE #temp_enriched;
    
    -- Join orders with customer and product dimensions
    SELECT 
        o.order_id,
        o.order_date,
        o.shipped_date,
        -- Calculate shipping delay
        CASE 
            WHEN o.shipped_date IS NOT NULL 
            THEN DATEDIFF(DAY, o.order_date, o.shipped_date)
            ELSE NULL
        END AS days_to_ship,
        
        -- Customer attributes
        o.customer_id,
        c.customer_name,
        c.city AS customer_city,
        c.state AS customer_state,
        c.country_code AS customer_country,
        c.customer_segment,
        
        -- Product attributes
        o.product_id,
        p.product_name,
        p.category AS product_category,
        p.subcategory AS product_subcategory,
        p.brand AS product_brand,
        
        -- Order metrics
        o.quantity,
        o.unit_price,
        o.discount_percent,
        o.line_total,
        p.cost_price,
        -- Calculate profit
        o.line_total - (o.quantity * COALESCE(p.cost_price, 0)) AS line_profit,
        
        -- Status
        o.standardized_status,
        
        -- Date dimensions
        YEAR(o.order_date) AS order_year,
        MONTH(o.order_date) AS order_month,
        DAY(o.order_date) AS order_day,
        DATEPART(QUARTER, o.order_date) AS order_quarter,
        DATENAME(WEEKDAY, o.order_date) AS order_day_of_week,
        
        -- Flags for conditional logic
        CASE WHEN o.discount_percent > 0 THEN 1 ELSE 0 END AS has_discount,
        CASE WHEN o.standardized_status = 'completed' THEN 1 ELSE 0 END AS is_completed,
        
        -- Metadata
        @load_date AS etl_load_date,
        GETDATE() AS etl_inserted_at
    INTO #temp_enriched
    FROM staging.stg_orders o
    LEFT JOIN staging.stg_customers c 
        ON o.customer_id = c.customer_id
    LEFT JOIN staging.stg_products p 
        ON o.product_id = p.product_id
    WHERE o.etl_load_date >= DATEADD(DAY, -90, @load_date); -- Process recent orders
    
    -- MERGE into analytics table
    MERGE INTO analytics.order_enriched AS target
    USING #temp_enriched AS source
        ON target.order_id = source.order_id
    WHEN MATCHED THEN
        UPDATE SET
            shipped_date = source.shipped_date,
            days_to_ship = source.days_to_ship,
            customer_name = source.customer_name,
            customer_city = source.customer_city,
            customer_state = source.customer_state,
            customer_country = source.customer_country,
            customer_segment = source.customer_segment,
            product_name = source.product_name,
            product_category = source.product_category,
            product_subcategory = source.product_subcategory,
            product_brand = source.product_brand,
            quantity = source.quantity,
            unit_price = source.unit_price,
            discount_percent = source.discount_percent,
            line_total = source.line_total,
            cost_price = source.cost_price,
            line_profit = source.line_profit,
            standardized_status = source.standardized_status,
            has_discount = source.has_discount,
            is_completed = source.is_completed,
            etl_load_date = source.etl_load_date,
            etl_updated_at = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            order_id, order_date, shipped_date, days_to_ship,
            customer_id, customer_name, customer_city, customer_state, 
            customer_country, customer_segment,
            product_id, product_name, product_category, product_subcategory, 
            product_brand,
            quantity, unit_price, discount_percent, line_total, 
            cost_price, line_profit,
            standardized_status, order_year, order_month, order_day, 
            order_quarter, order_day_of_week,
            has_discount, is_completed,
            etl_load_date, etl_inserted_at, etl_updated_at
        )
        VALUES (
            source.order_id, source.order_date, source.shipped_date, source.days_to_ship,
            source.customer_id, source.customer_name, source.customer_city, 
            source.customer_state, source.customer_country, source.customer_segment,
            source.product_id, source.product_name, source.product_category, 
            source.product_subcategory, source.product_brand,
            source.quantity, source.unit_price, source.discount_percent, 
            source.line_total, source.cost_price, source.line_profit,
            source.standardized_status, source.order_year, source.order_month, 
            source.order_day, source.order_quarter, source.order_day_of_week,
            source.has_discount, source.is_completed,
            source.etl_load_date, source.etl_inserted_at, GETDATE()
        );
    
END;
