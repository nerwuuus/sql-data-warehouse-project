/*
============================================================================
Quality Checks
============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardisation across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardisation and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
============================================================================
*/

-- Check for NULLS or duplicates in the Primary Key
-- Expectation: no result
SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING 
    COUNT(*) > 1 OR 
    prd_id IS NULL;

-- Check for NULLs or negative numbers
-- Expectation: no result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE 
    prd_cost < 0 OR
    prd_cost IS NULL;


-- Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- Check for unwanted spaces
-- Expectation: no result
SELECT maintenance
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);



-- Check for invalid dates
SELECT NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE 
    sls_due_dt <= 0 OR
    LENGTH(sls_due_dt::TEXT) != 8 OR
    sls_due_dt > 20500101 OR
    sls_due_dt < 19000101;

SELECT *
FROM bronze.crm_sales_details
WHERE 
    sls_order_dt > sls_ship_dt OR
    sls_order_dt > sls_due_dt;


/* Check the data consistency: between sales, quantity and price.
   >> Sales = Quantity * Price
   >> Expectation: no NULLs, no zeros, no negative values
*/
SELECT
    sls_sales AS old_sls_value,
    sls_quantity,
    sls_price AS old_sls_price,
    -- Recalculate sales if invalid
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_price,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / COALESCE(sls_quantity, 1)  -- Avoid division by zero
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price OR
    sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
    sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY 
    sls_sales DESC,
    sls_quantity,
    sls_price;


/* Data standardisation & consistency in
the low cardinality columns */
-- Expectation: no results
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

    /*
        SELECT *
        FROM bronze.crm_cust_info
        WHERE cst_gndr IN (
            '2012-07-01',
            '2003-07-01',
            '2013-07-01',
            '2011-07-01'
        );

        DELETE FROM bronze.crm_cust_info
        WHERE cst_gndr IN (
            '2012-07-01',
            '2003-07-01',
            '2013-07-01',
            '2011-07-01'
        );
    */

