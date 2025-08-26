-- Create a stored procedure for removing the content of tables
CREATE OR REPLACE FUNCTION bronze.clean_bronze()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE bronze.crm_prd_info;
    TRUNCATE bronze.crm_sales_details;
    TRUNCATE bronze.erp_cust_az12;
    TRUNCATE bronze.erp_loc_a101;
    TRUNCATE bronze.erp_px_cat_g1v2;
    
    -- Custom message
    RAISE NOTICE 'Bronze tables have been successfully truncated.';
END;
$$;

-- Invoke a procedure
CALL bronze.clean_bronze();
