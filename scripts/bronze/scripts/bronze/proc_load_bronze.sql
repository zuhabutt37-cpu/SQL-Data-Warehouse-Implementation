/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @batch_start DATETIME, @batch_end DATETIME;
    DECLARE @start_time DATETIME, @end_time DATETIME;

    SET @batch_start = GETDATE(); -- Total batch start
    PRINT '====================================================';
    PRINT '           BRONZE LAYER BATCH START';
    PRINT 'Start Time: ' + CAST(@batch_start AS NVARCHAR);
    PRINT '====================================================';

    BEGIN TRY
        ----------------------------------------
        -- CRM TABLES
        ----------------------------------------

        -- CRM CUSTOMER
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> BULK INSERT INTO: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> CRM Customer Load Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

        -- CRM PRODUCT
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> BULK INSERT INTO: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> CRM Product Load Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

        -- CRM SALES RAW
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details_raw';
        TRUNCATE TABLE bronze.crm_sales_details_raw;

        PRINT '>> BULK INSERT INTO: bronze.crm_sales_details_raw';
        BULK INSERT bronze.crm_sales_details_raw
        FROM 'C:\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> CRM Sales Load Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

        ----------------------------------------
        -- ERP TABLES
        ----------------------------------------

        -- ERP CUSTOMER RAW
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12_raw';
        TRUNCATE TABLE bronze.erp_cust_az12_raw;

        PRINT '>> BULK INSERT INTO: bronze.erp_cust_az12_raw';
        BULK INSERT bronze.erp_cust_az12_raw
        FROM 'C:\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> ERP Customer Load Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

        -- ERP LOCATION RAW
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101_raw';
        TRUNCATE TABLE bronze.erp_loc_a101_raw;

        PRINT '>> BULK INSERT INTO: bronze.erp_loc_a101_raw';
        BULK INSERT bronze.erp_loc_a101_raw
        FROM 'C:\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> ERP Location Load Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

        -- ERP PRODUCT CATEGORY RAW
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2_raw';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2_raw;

        PRINT '>> BULK INSERT INTO: bronze.erp_px_cat_g1v2_raw';
        BULK INSERT bronze.erp_px_cat_g1v2_raw
        FROM 'C:\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> ERP Product Category Load Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

    END TRY
    BEGIN CATCH
        PRINT '============================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================';
    END CATCH

    -- Total batch end and duration
    SET @batch_end = GETDATE();
    PRINT '====================================================';
    PRINT '           BRONZE LAYER BATCH END';
    PRINT 'End Time: ' + CAST(@batch_end AS NVARCHAR);
    PRINT 'Total Bronze Layer Load Duration (seconds): ' 
          + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR);
    PRINT '====================================================';
END
GO

-- Execute procedure
EXEC bronze.load_bronze;
GO
  
