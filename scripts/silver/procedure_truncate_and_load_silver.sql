/*
============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
============================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  - Truncates Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.
============================================================================
*/

-- Invoke a procedure
CALL silver.load_silver();

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Trim string values, replace abbreviations, remove duplicates
    -- Insert cleaned data into silver.crm_cust_info table
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status, -- Normalise marital status values to readable format
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'N/A'
        END AS cst_gndr, -- Normalise gender values to readable format
        cst_create_date
    FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    )t -- This subquery removes duplicates
    WHERE flag_last = 1; -- Select the most recent record per customer


    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Extract product key. LENGTH(prd_key) extracts up to the full length of the string
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost, -- Replace NULLs with 0
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line, -- Map product line codes to descriptive values
        prd_start_dt::DATE,
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt -- Calculate the end date as one day before the next start date
    FROM bronze.crm_prd_info;


    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE sls_order_dt::TEXT::DATE
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE sls_ship_dt::TEXT::DATE
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE sls_due_dt::TEXT::DATE
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_price,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / COALESCE(sls_quantity, 1)  -- Avoid division by zero
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;


    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix 
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL -- Set the future birth dates to NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'N/A'
        END AS gen -- Normalise gender values and handle unknown values
    FROM bronze.erp_cust_az12;


    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid, -- Remove '-' from customer id
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
            ELSE TRIM(cntry)
        END AS cntry -- Normalise and handle missing or blank country codes
    FROM bronze.erp_loc_a101;


    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    -- Custom message
    RAISE NOTICE 'Silver tables have been successfully updated.';
END;
$$;

