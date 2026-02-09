-- =============================================
-- SPROC: sp_load_staging_products
-- Purpose: Stage product dimension data
-- Dependencies: raw_data.products (source table)
-- Output: staging.stg_products
-- =============================================
CREATE PROCEDURE sp_load_staging_products
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE staging.stg_products;
    
    INSERT INTO staging.stg_products (
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        list_price,
        cost_price,
        -- Calculate margin
        margin_percent,
        is_active,
        discontinued_date,
        etl_loaded_at
    )
    SELECT 
        product_id,
        product_name,
        COALESCE(category, 'Uncategorized') AS category,
        subcategory,
        brand,
        list_price,
        cost_price,
        -- Margin calculation with null handling
        CASE 
            WHEN list_price > 0 THEN 
                ROUND(((list_price - COALESCE(cost_price, 0)) / list_price * 100), 2)
            ELSE 0
        END AS margin_percent,
        COALESCE(is_active, 1) AS is_active,
        discontinued_date,
        GETDATE() AS etl_loaded_at
    FROM raw_data.products
    WHERE product_id IS NOT NULL;
    
END;
