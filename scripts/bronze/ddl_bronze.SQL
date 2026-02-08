/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL DROP TABLE bronze.crm_cust_info;
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL DROP TABLE bronze.crm_prd_info;
IF OBJECT_ID('bronze.crm_sales_details_raw', 'U') IS NOT NULL DROP TABLE bronze.crm_sales_details_raw;

IF OBJECT_ID('bronze.erp_cust_az12_raw', 'U') IS NOT NULL DROP TABLE bronze.erp_cust_az12_raw;
IF OBJECT_ID('bronze.erp_loc_a101_raw', 'U') IS NOT NULL DROP TABLE bronze.erp_loc_a101_raw;
IF OBJECT_ID('bronze.erp_px_cat_g1v2_raw', 'U') IS NOT NULL DROP TABLE bronze.erp_px_cat_g1v2_raw;
GO

/* =========================================================
   CREATE BRONZE TABLES
   ========================================================= */

-- CRM CUSTOMER
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(100),
    cst_lastname        NVARCHAR(100),
    cst_marital_status  NVARCHAR(20),
    cst_gndr            NVARCHAR(10),
    cst_create_date     DATE
);
GO

-- CRM PRODUCT
CREATE TABLE bronze.crm_prd_info (
    prd_id        INT,
    prd_key       NVARCHAR(50),
    prd_nm        NVARCHAR(255),
    prd_cost      INT,
    prd_line      NVARCHAR(100),
    prd_start_dt  DATETIME,
    prd_end_dt    DATETIME
);
GO

-- CRM SALES RAW (all NVARCHAR to avoid conversion errors)
CREATE TABLE bronze.crm_sales_details_raw (
    sls_ord_num    NVARCHAR(50),
    sls_prd_key    NVARCHAR(50),
    sls_cust_id    NVARCHAR(50),
    sls_order_dt   NVARCHAR(50),
    sls_ship_dt    NVARCHAR(50),
    sls_due_dt     NVARCHAR(50),
    sls_sales      NVARCHAR(50),
    sls_quantity   NVARCHAR(50),
    sls_price      NVARCHAR(50)
);
GO

-- ERP CUSTOMER RAW
CREATE TABLE bronze.erp_cust_az12_raw (
    cid   NVARCHAR(50),
    bdate NVARCHAR(50),
    gen   NVARCHAR(10)
);
GO

-- ERP LOCATION RAW
CREATE TABLE bronze.erp_loc_a101_raw (
    cid   NVARCHAR(50),
    cntry NVARCHAR(100)
);
GO

-- ERP PRODUCT CATEGORY RAW
CREATE TABLE bronze.erp_px_cat_g1v2_raw (
    id           NVARCHAR(50),
    cat          NVARCHAR(100),
    subcat       NVARCHAR(100),
    maintenance  NVARCHAR(50)
);
GO
