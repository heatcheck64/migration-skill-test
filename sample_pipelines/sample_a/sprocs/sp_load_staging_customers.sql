-- =============================================
-- SPROC: sp_load_staging_customers
-- Purpose: Stage customer dimension data
-- Dependencies: raw_data.customers (source table)
-- Output: staging.stg_customers
-- =============================================
CREATE PROCEDURE sp_load_staging_customers
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE staging.stg_customers;
    
    INSERT INTO staging.stg_customers (
        customer_id,
        customer_name,
        email,
        phone,
        city,
        state,
        country,
        -- Standardize country codes
        country_code,
        customer_segment,
        signup_date,
        is_active,
        etl_loaded_at
    )
    SELECT 
        customer_id,
        TRIM(customer_name) AS customer_name,
        LOWER(TRIM(email)) AS email,
        phone,
        city,
        state,
        country,
        -- Map country to ISO codes
        CASE 
            WHEN country IN ('United States', 'USA', 'US') THEN 'US'
            WHEN country IN ('United Kingdom', 'UK', 'GB') THEN 'GB'
            WHEN country = 'Canada' THEN 'CA'
            ELSE 'OTHER'
        END AS country_code,
        CASE 
            WHEN customer_segment IS NULL THEN 'unknown'
            ELSE customer_segment
        END AS customer_segment,
        signup_date,
        CASE 
            WHEN is_active IS NULL THEN 1
            ELSE is_active
        END AS is_active,
        GETDATE() AS etl_loaded_at
    FROM raw_data.customers
    WHERE customer_id IS NOT NULL;
    
END;
