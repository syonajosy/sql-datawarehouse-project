-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
    cst_id,
    COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


--remove duplicates
SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;

-- Check for unwanted Spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
    prd_id,
    COUNT(*) AS duplicate_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- ============================================
-- Title: Validate Product Keys Against Category Reference Table
-- Description: Selects product details from bronze.crm_prd_info,
--              derives cat_id from prd_key, and filters out rows 
--              where the derived cat_id does not exist in 
--              bronze.erp_px_cat_g1v2.
-- ============================================

SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
    SELECT DISTINCT id 
    FROM bronze.erp_px_cat_g1v2
);





-- ============================================
-- Title: Validate Product Keys Against Sales Details
-- Description: Splits product key into category (cat_id) 
--              and product key part, then checks for 
--              products in bronze.crm_prd_info that do 
--              not exist in bronze.crm_sales_details.
-- ============================================

SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
    SELECT sls_prd_key 
    FROM bronze.crm_sales_details
);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- ============================================
-- Data Standardization & Consistency
-- Purpose: List all distinct values of product line (prd_line)
--          from bronze.crm_prd_info to check for unexpected 
--          values (including NULLs).
-- ============================================

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check for Invalid Dates
SELECT 
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
   OR LEN(sls_ship_dt) != 8
   OR sls_ship_dt > 20500101
   OR sls_ship_dt < 19000101;

-- Identify Out-of-Range Dates
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;