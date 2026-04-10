-- ============================================================
-- Project: Data Warehouse & Analytics Project
-- Layer: Silver Layer (Data Cleaning & Transformation)
-- File: load_silver.sql
-- Author: Gayatri Jadhav
-- Date: 2026-04-10
-- ============================================================

-- Description:
-- This script transforms and cleans raw data from the bronze layer
-- into the silver layer. It applies data standardization, validation,
-- and business rules to ensure high-quality, structured data.

-- Transformation Logic:
-- - Trimming and standardizing text fields
-- - Handling NULL, missing, and invalid values
-- - Deduplication using window functions (ROW_NUMBER)
-- - Data type conversions (e.g., DATE formatting)
-- - Deriving calculated columns (e.g., sales, price corrections)
-- - Cleaning hidden characters and inconsistent values

-- Data Sources:
-- - Bronze Layer (Raw Data)
--   - bronze_crm_cust_info
--   - bronze_crm_prd_info
--   - bronze_crm_sales_details
--   - bronze_erp_cust_az12
--   - bronze_erp_loc_a101
--   - bronze_erp_px_cat_g1v2

-- Tables Loaded:
-- - silver_crm_cust_info
-- - silver_crm_prd_info
-- - silver_crm_sales_details
-- - silver_erp_cust_az12
-- - silver_erp_loc_a101
-- - silver_erp_px_cat_g1v2

-- Notes:
-- 1. This script performs full refresh (TRUNCATE + INSERT)
-- 2. Ensures cleaned and standardized data for downstream use
-- 3. Handles dirty data issues (extra spaces, nulls, hidden characters)
-- 4. Prepares data for Gold layer (analytics & reporting)

-- ============================================================

/* ================================
   CUSTOMER INFO
================================ */

TRUNCATE TABLE silver_crm_cust_info;

INSERT INTO silver_crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    dwh_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),

    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,

    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,

    cst_create_date,
    NOW()

FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze_crm_cust_info
    WHERE cst_id IS NOT NULL
      AND cst_id <> 0
) t
WHERE flag_last = 1;


/* ================================
   PRODUCT INFO
================================ */

TRUNCATE TABLE silver_crm_prd_info;

INSERT INTO silver_crm_prd_info(
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    dwh_create_date
)
SELECT
    prd_id,
    SUBSTRING(prd_key,7),
    REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
    prd_nm,
    IFNULL(prd_cost,0),

    CASE 
        WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
        ELSE 'n/a'
    END,

    DATE(prd_start_dt),

    DATE(
        DATE_SUB(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key 
                ORDER BY prd_start_dt
            ),
            INTERVAL 1 DAY
        )
    ),

    NOW()

FROM bronze_crm_prd_info;


/* ================================
   SALES DETAILS
================================ */

TRUNCATE TABLE silver_crm_sales_details;

INSERT INTO silver_crm_sales_details(
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
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE DATE(sls_order_dt)
    END,

    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE DATE(sls_ship_dt)
    END,

    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE DATE(sls_due_dt)
    END,

    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END

FROM bronze_crm_sales_details;


/* ================================
   ERP CUSTOMER
================================ */

TRUNCATE TABLE silver_erp_cust_az12;

INSERT INTO silver_erp_cust_az12(
    cid,
    bdate,
    gen,
    dwh_create_date
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4)
        ELSE cid
    END,

    CASE 
        WHEN bdate > CURDATE() THEN NULL
        ELSE bdate
    END,

    CASE 
        WHEN gen IS NULL OR TRIM(gen) = '' THEN 'n/a'
        WHEN LEFT(UPPER(TRIM(gen)),1) = 'F' THEN 'Female'
        WHEN LEFT(UPPER(TRIM(gen)),1) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,

    NOW()

FROM bronze_erp_cust_az12;


/* ================================
   ERP LOCATION
================================ */

TRUNCATE TABLE silver_erp_loc_a101;

INSERT INTO silver_erp_loc_a101(
    cid,
    cntry,
    dwh_create_date
)
SELECT 
    REPLACE(cid, '-',''),

    CASE 
        WHEN UPPER(TRIM(REPLACE(cntry, '\r', ''))) = 'DE' 
            THEN 'Germany'

        WHEN UPPER(TRIM(REPLACE(cntry, '\r', ''))) IN ('US','USA') 
            THEN 'United States'

        WHEN cntry IS NULL OR TRIM(cntry) = '' 
            THEN 'n/a'

        ELSE TRIM(REPLACE(cntry, '\r', ''))
    END,

    NOW()

FROM bronze_erp_loc_a101;


/* ================================
   ERP PRODUCT CATEGORY
================================ */

TRUNCATE TABLE silver_erp_px_cat_g1v2;

INSERT INTO silver_erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance,
    dwh_create_date
)
SELECT 
    id,
    cat,
    subcat,
    maintenance,
    NOW()
FROM bronze_erp_px_cat_g1v2;
