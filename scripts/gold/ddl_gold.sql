-- ============================================================
-- Project: Data Warehouse & Analytics Project
-- Layer: Gold Layer (Business-Level Views)
-- File: ddl_gold.sql
-- Author: Gayatri Jadhav
-- Date: 2026-04-11
-- ============================================================

-- Description:
-- This script creates Gold Layer views for analytics and reporting.
-- The Gold Layer represents cleaned, enriched, and business-ready data
-- derived from the Silver Layer.

-- Objects Created:
-- 1. gold_dim_customers
--    - Customer dimension table
--    - Combines CRM and ERP customer data
--    - Includes demographic details like name, gender, country, and birthdate

-- 2. gold_dim_products
--    - Product dimension table
--    - Enriched with category and subcategory information
--    - Filters only active products (prd_end_dt IS NULL)

-- 3. gold_fact_sales
--    - Sales fact table
--    - Contains transactional sales data
--    - Linked with customer and product dimensions using surrogate keys

-- Key Features:
-- - Uses ROW_NUMBER() to generate surrogate keys
-- - Applies LEFT JOINs to combine multiple data sources
-- - Implements data standardization (e.g., gender handling with COALESCE)
-- - Follows Star Schema design (Fact + Dimension tables)

-- Notes:
-- - Ensure Silver Layer tables are loaded before running this script
-- - Views are created for reporting and downstream analytics tools
-- ============================================================

CREATE VIEW gold_dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,   -- Surrogate Key

    ci.cst_id AS customer_id,                              -- Business Key
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,

    la.cntry AS country,                                   -- Location info

    ci.cst_marital_status AS marital_status,

    -- Gender logic:
    -- Use CRM gender if available, otherwise fallback to ERP
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date

FROM silver_crm_cust_info AS ci

-- Join with ERP customer table for additional attributes
LEFT JOIN silver_erp_cust_az12 AS ca 
    ON ci.cst_key = ca.cid

-- Join with location table for country info
LEFT JOIN silver_erp_loc_a101 AS la
    ON ci.cst_key = la.cid;


-- ============================================================
-- VIEW: gold_dim_products
-- Description:
-- Product dimension view enriched with category information.
-- Only active products are included.
-- ============================================================

CREATE VIEW gold_dim_products AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,  -- Surrogate Key

    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,

    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,

    pn.prd_cost AS product_cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date

FROM silver_crm_prd_info AS pn

-- Join with category table for product classification
LEFT JOIN silver_erp_px_cat_g1v2 AS pc
    ON pn.cat_id = pc.id

-- Filter only active products
WHERE prd_end_dt IS NULL;


-- ============================================================
-- VIEW: gold_fact_sales
-- Description:
-- Sales fact view containing transactional data.
-- Linked with customer and product dimensions.
-- ============================================================

CREATE VIEW gold_fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,

    pr.product_key,     -- Foreign Key to Product Dimension
    cu.customer_key,    -- Foreign Key to Customer Dimension

    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,

    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price

FROM silver_crm_sales_details AS sd

-- Join with product dimension
LEFT JOIN gold_dim_products AS pr
    ON sd.sls_prd_key = pr.product_number

-- Join with customer dimension
LEFT JOIN gold_dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id;
