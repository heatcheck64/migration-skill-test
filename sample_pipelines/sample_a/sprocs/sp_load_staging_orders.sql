-- =============================================
-- SPROC: sp_load_staging_orders
-- Purpose: Stage raw orders data with basic transformations
-- Dependencies: raw_data.orders (source table)
-- Output: staging.stg_orders
-- =============================================
CREATE PROCEDURE sp_load_staging_orders
    @load_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Default to today if no date provided
    IF @load_date IS NULL
        SET @load_date = CAST(GETDATE() AS DATE);
    
    -- Temp table for initial staging
    IF OBJECT_ID('tempdb..#temp_orders') IS NOT NULL
        DROP TABLE #temp_orders;
    
    SELECT 
        order_id,
        customer_id,
        product_id,
        order_date,
        shipped_date,
        quantity,
        unit_price,
        discount_percent,
        -- Calculate line total
        quantity * unit_price * (1 - discount_percent / 100.0) AS line_total,
        order_status,
        -- Standardize status values
        CASE 
            WHEN order_status IN ('complete', 'completed', 'done') THEN 'completed'
            WHEN order_status IN ('cancel', 'cancelled', 'void') THEN 'cancelled'
            WHEN order_status IN ('pending', 'processing') THEN 'processing'
            ELSE 'unknown'
        END AS standardized_status,
        created_at,
        updated_at,
        -- Metadata
        @load_date AS etl_load_date,
        GETDATE() AS etl_inserted_at
    INTO #temp_orders
    FROM raw_data.orders
    WHERE 
        -- Handle late-arriving data: process orders from last 90 days
        order_date >= DATEADD(DAY, -90, @load_date)
        AND order_date <= @load_date
        -- Data quality filter
        AND order_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND quantity > 0;
    
    -- MERGE into staging table (upsert pattern)
    MERGE INTO staging.stg_orders AS target
    USING #temp_orders AS source
        ON target.order_id = source.order_id
    WHEN MATCHED THEN
        UPDATE SET
            customer_id = source.customer_id,
            product_id = source.product_id,
            order_date = source.order_date,
            shipped_date = source.shipped_date,
            quantity = source.quantity,
            unit_price = source.unit_price,
            discount_percent = source.discount_percent,
            line_total = source.line_total,
            order_status = source.order_status,
            standardized_status = source.standardized_status,
            created_at = source.created_at,
            updated_at = source.updated_at,
            etl_load_date = source.etl_load_date,
            etl_updated_at = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            order_id, customer_id, product_id, order_date, shipped_date,
            quantity, unit_price, discount_percent, line_total,
            order_status, standardized_status, created_at, updated_at,
            etl_load_date, etl_inserted_at, etl_updated_at
        )
        VALUES (
            source.order_id, source.customer_id, source.product_id, 
            source.order_date, source.shipped_date,
            source.quantity, source.unit_price, source.discount_percent, 
            source.line_total,
            source.order_status, source.standardized_status, 
            source.created_at, source.updated_at,
            source.etl_load_date, source.etl_inserted_at, GETDATE()
        );
    
    -- Log summary
    SELECT 
        'sp_load_staging_orders' AS sproc_name,
        @load_date AS load_date,
        COUNT(*) AS rows_processed,
        GETDATE() AS completed_at
    FROM #temp_orders;
    
END;
