-- Perform the full load on CRM & ERP tables
TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    ENCODING 'UTF8'
);

/* 

=====================================
            
            WARNING !!!

=====================================
Error 'Permission denied' while uploading data. Paste the code below in pgAdmin4, PSQL Tool to load data manually:
  \copy bronze.crm_cust_info FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
  \copy bronze.crm_prd_info FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
  \copy bronze.crm_sales_details FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
  \copy bronze.erp_cust_az12 FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
  \copy bronze.erp_loc_a101 FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
  \copy bronze.erp_px_cat_g1v2 FROM 'C:\Users\a817628\OneDrive - ATOS\Desktop\Data Analytics\SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

*/
