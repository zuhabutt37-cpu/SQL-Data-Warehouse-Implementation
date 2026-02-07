/*******************************************************************************
Script Purpose:
    This script initializes the Data Warehouse environment using the Medallion 
    Architecture (Bronze, Silver, Gold). It handles the cleanup of existing 
    databases to ensure a clean state for deployment.

WARNING:
    Running this script will PERMANENTLY DELETE the existing 'DataWarehouse' 
    database and all data contained within it. Ensure you have backups if 
    running this in a production-adjacent environment.
*******************************************************************************/

USE master;
GO

-- 1. Check if the database exists and drop it
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    PRINT 'Existing DataWarehouse found. Terminating connections...';
    
    -- Set to SINGLE_USER to kick out other users and rollback their transactions
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    
    -- Drop the database
    DROP DATABASE DataWarehouse;
    PRINT 'Database dropped successfully.';
END
GO

-- 2. Create the fresh database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- 3. Create the Medallion Schemas
-- Bronze: Raw data landing zone
CREATE SCHEMA bronze;
GO

-- Silver: Validated and enriched data
CREATE SCHEMA silver;
GO

-- Gold: Business-level aggregates and reporting views
CREATE SCHEMA gold;
GO

PRINT 'DataWarehouse environment created with Bronze, Silver, and Gold schemas.';
