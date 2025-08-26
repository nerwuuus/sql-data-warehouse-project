/*
============================================================================
Stored Procedure: Truncate Bronze Layer
============================================================================
Script Purpose:
  This stored procedure removes rows from the 'bronze' schema.
  It performs the following actions:
  - Truncates all bronze tables before loading new data.
  - Prints a custom message after execution.
============================================================================
*/
CREATE OR REPLACE FUNCTION bronze.clean_bronze()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE TABLE bronze.crm_prd_info;
    TRUNCATE TABLE bronze.crm_sales_details;
    TRUNCATE TABLE bronze.erp_cust_az12;
    TRUNCATE TABLE bronze.erp_loc_a101;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    -- Custom message
    RAISE NOTICE 'Bronze tables have been successfully truncated.';
END;
$$;

-- Invoke a procedure
CALL bronze.clean_bronze();
