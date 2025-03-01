-- Data warehouse development project
-- Create Database 'DataWarehouse'

USE master;

create database DataWarehouse;

use DataWarehouse

-- creating schema for each layer

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO             -- GO is separator, so that first command runs first
CREATE SCHEMA gold;


------------BRONZE LAYER------------
---CREATING DDL FOR TABLES

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO



CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO


CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

SELECT * FROM bronze.crm_cust_info
SELECT count(*) FROM bronze.crm_cust_info


----Bulk insert of data in database----


CREATE PROCEDURE bronze.load_bronze  
ALTER PROCEDURE bronze.load_bronze AS  
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
 BEGIN TRY
	PRINT '======================================'
	PRINT 'LOADING BRONZE LAYER'
	PRINT '======================================'

	PRINT 'loading crm tables'

	SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_cust_info;
	-- truncate because if you run the insert command twice, data will be inserted again
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK -- FOR PERFORMANCE OPTIMIZATION WHILE LOADING
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + ' seconds' 

	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);

	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);

	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);

	--- inserting erp data
	PRINT 'LOADING ERP TABLES'
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);

	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);


	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\Saiban N Pagarkar\OneDrive\Desktop\DE_projects\Data_Warehouse_and_Analytics_using_SQL\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);

	SET @batch_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time , @batch_end_time) AS NVARCHAR) + ' seconds' 
 END TRY
 BEGIN CATCH
	PRINT '======================================'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '======================================'
 END CATCH
END;

EXEC bronze.load_bronze

----Creating stored procedure
--for frequently use sql code we use stored procedure
--In stored procedure you need to add prints to track execution, debug issue and understand its flow IN OUTPUT



-------------------------------------------------------------------------------------------------------------------------------
------------------------------------SILVER LAYER-----------------------------------------------------------------------------------

SELECT TOP 1000 * FROM bronze.crm_cust_info
SELECT TOP 1000 * FROM bronze.crm_prd_info
SELECT TOP 1000 * FROM bronze.crm_sales_details
SELECT TOP 1000 * FROM bronze.erp_cust_az12
SELECT TOP 1000 * FROM bronze.erp_loc_a101
SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2

-------------creating table for silver layer ------

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),
    cntry           NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- checking for nulls or duplicates in primary key

select 
cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

--- check for unwanted spaces ---

select trim(cst_firstname) from bronze.crm_cust_info

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

--- doing data transformation-----------------

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(CST_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id IS NOT NULL)t
where flag_last = 1

------DATA STANDARDIZATION AND CONSISTENCY
--changing into full name eg f- female
SELECT DISTINCT cst_gndr
from bronze.crm_cust_info


--- final data transformation for crm_cust_info table-----------------
INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(CST_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' Then 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' Then 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id IS NOT NULL)t
where flag_last = 1


---------------Quality check for the silver table ----------
select * from silver.crm_cust_info

select 
cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null
--
SELECT DISTINCT cst_gndr
from silver.crm_cust_info
--
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)


-------------- data transformation for 2nd table crm_prd_info-----------------

select 
prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

--- extracting category id from prd_key
---extracting prd_key from prd_key
-- and joining the prd_info table with erp_px_cat_g1v2

---------------------------------------------------------------------------------------------------------

INSERT INTO silver.crm_prd_info(
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
REPLACE(substring(prd_key,1,5), '-', '_') as cat_id,
REPLACE(substring(prd_key, 7,len(prd_key)), '-', '_') as prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
case when upper(TRIM(prd_line)) = 'M' then 'Mountain'
     when upper(TRIM(prd_line)) = 'R' then 'Road'
	 when upper(TRIM(prd_line)) = 'S' then 'Other Sales'
	 when upper(TRIM(prd_line)) = 'T' then 'Touring'
	 else 'n/a'
end as prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(lead(prd_start_dt,1) over(partition by prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt
from bronze.crm_prd_info

select * from silver.crm_prd_info
---before inserting the record we have to make changes to the ddl table in which we will load the data
-------------------------------------------------------------------------------------------------------------------
-- checking for nulls or negative numbers

select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

--checking for invalid date orders (end date should not be earlier than start date)
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt


--- for solution of invalid date we are going to try it on 2 products only

select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt,1) over(partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-------------------------------------------------------------------------------------------------------------------------------
-----cleaning and doing transformation of the crm_sales_data

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
---------------------------------------------------------

-------------DATA QUALITY CHECKS -----------------

----- we have to convert the integer to date
--- checking if sales_order_dt is zero (if it is zero then it will make it null )
--- order date must always bdate or due datee earlier than the shipping 
-- checking for invalid date orders
---Checking data consistency : between sales, quantity and price
--->Sales = quantity * price
--->Values must not be null , zero or negative

select 
nullif(sls_order_dt, 0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or len(sls_order_dt) != 8
or sls_order_dt > 20200101
or sls_order_dt < 19000101

SELECT *
FROM bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity, sls_price
-- we have bad data


SELECT DISTINCT
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
         then sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price is NULL OR sls_price <=0
     then sls_price / nullif(sls_quantity, 0)
	 else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity, sls_price

--------------------------------------------------------------------
INSERT INTO silver.crm_sales_details(
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
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
     else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
     else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
         then sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price is NULL OR sls_price <=0
     then sls_price / nullif(sls_quantity, 0)
	 else sls_price
end as sls_price,
sls_quantity
from bronze.crm_sales_details

SELECT * FROM silver.crm_sales_details

-------------------------------------------------------------------------
---- NOW DOING TRANSFORMATION OF ERP TABLES

-----Data quality check

--removing unnecessary words from cid
-- identify out of range dates
-- data standardization and consistency


select
case when cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , len(cid))
else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12

select distinct
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

select distinct gen,
from bronze.erp_cust_az12

-----------transformation

INSERT INTO silver.erp_cust_az12(
cid, bdate, gen
)
select
case when cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , len(cid))
else cid
end as cid,
case when bdate > getdate() then null
else bdate
end as bdate,
case when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
     when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
from bronze.erp_cust_az12

select * from silver.erp_cust_az12


---------------------------------------------------------------
-- transformation on erp_loc_a101 table

-- data quality check

-- first we need to see the column we are going to match with other table and make sure that both are same
-- data standardization and consistency


select 
replace(cid, '-', '') cid,
cntry
from bronze.erp_loc_a101 

select distinct cntry
from bronze.erp_loc_a101
order by cntry


----------------------------------------------
INSERT INTO silver.erp_loc_a101
(cid,cntry)
select 
replace(cid, '-', '') cid,
case when trim(cntry) = 'DE' THEN 'Germany'
     when trim(cntry) IN ('US', 'USA') THEN 'United States'
     when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
end as cntry
from bronze.erp_loc_a101 

select * from silver.erp_loc_a101

---------------------------------------------------------------------------------------

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


--------------------------------------------------------

--creating stored procedure for all the six transformed table

CREATE PROCEDURE silver.load_silver 
AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
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
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
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
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END;

EXEC silver.load_silver

-------------------------------------------------------------------------------------------------------------



-------------------------------DATA INTEGRATION----------------------------------------------------

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM IS THE MASTER FOR GENDER INFO
     ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;



-----------------------------------------------------------------------------------------------------------------------

--checking if the fact table connects with all the dimension table as it is star schema

SELECT *
FROM gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where c.customer_key is null