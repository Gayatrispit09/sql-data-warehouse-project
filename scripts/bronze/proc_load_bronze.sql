-- ============================================================
-- Project: Data Warehouse & Analytics Project
-- Layer: Bronze Layer (Raw Data Ingestion)
-- File: load_bronze.sql
-- Author: Gayatri Jadhav
-- Date: 2026-04-07
-- ============================================================

-- Description:
-- This script loads raw data from CSV files into bronze layer tables.
-- It performs full refresh by truncating tables before loading data.
-- Data is sourced from CRM and ERP systems.

-- Data Sources:
-- - CRM: Customer, Product, Sales datasets
-- - ERP: Customer, Location, Product Category datasets

-- Tables Loaded:
-- - bronze_crm_cust_info
-- - bronze_crm_prd_info
-- - bronze_crm_sales_details
-- - bronze_erp_cust_az12
-- - bronze_erp_loc_a101
-- - bronze_erp_px_cat_g1v2

-- Notes:
-- 1. Requires LOCAL INFILE enabled
-- 2. CSV files must be accessible from given file paths
-- 3. Header rows are skipped using IGNORE 1 ROWS
-- 4. This script performs full reload (TRUNCATE + LOAD)

-- ============================================================

-- =========================================
-- ENABLE LOCAL INFILE (Run once per session)
-- =========================================
SET GLOBAL local_infile = 1;

-- =========================================
-- LOAD CRM CUSTOMER INFO
-- =========================================
TRUNCATE TABLE bronze_crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/gayatrikaluramjadhav/Downloads/sql-data-warehouse-project 2/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
);

-- Verify data
SELECT * FROM bronze_crm_cust_info;


-- =========================================
-- LOAD CRM PRODUCT INFO
-- =========================================
TRUNCATE TABLE bronze_crm_prd_info;

LOAD DATA LOCAL INFILE '/Users/gayatrikaluramjadhav/Downloads/sql-data-warehouse-project 2/datasets/source_crm/prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
);

-- Verify data
SELECT * FROM bronze_crm_prd_info;


-- =========================================
-- LOAD CRM SALES DETAILS
-- =========================================
TRUNCATE TABLE bronze_crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/gayatrikaluramjadhav/Downloads/sql-data-warehouse-project 2/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
);

-- Verify data
SELECT * FROM bronze_crm_sales_details;


-- =========================================
-- LOAD ERP CUSTOMER DATA
-- =========================================
TRUNCATE TABLE bronze_erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/gayatrikaluramjadhav/Downloads/sql-data-warehouse-project 2/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    cid,
    bdate,
    gen
);

-- Verify data
SELECT * FROM bronze_erp_cust_az12;


-- =========================================
-- LOAD ERP LOCATION DATA
-- =========================================
TRUNCATE TABLE bronze_erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/gayatrikaluramjadhav/Downloads/sql-data-warehouse-project 2/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    cid,
    cntry
);

-- Verify data
SELECT * FROM bronze_erp_loc_a101;


-- =========================================
-- LOAD ERP PRODUCT CATEGORY DATA
-- =========================================
TRUNCATE TABLE bronze_erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/gayatrikaluramjadhav/Downloads/sql-data-warehouse-project 2/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    id,
    cat,
    subcat,
    maintenance
);

-- Verify data
SELECT * FROM bronze_erp_px_cat_g1v2;

-- Optional: Count check
SELECT COUNT(*) FROM bronze_erp_px_cat_g1v2;
