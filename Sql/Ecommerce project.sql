--- Creating a new database called ECommerceProject
CREATE DATABASE Ecommerce;
USE Ecommerce;

--Creating a staging Schema + raw staging table
IF SCHEMA_ID('staging') IS NULL
   EXEC('CREATE SCHEMA staging;');

--Creating a staging table + loaded at default
IF OBJECT_ID('staging.raw_ecommerce','U') IS NOT NULL
   DROP TABLE staging.raw_ecommerce;

CREATE TABLE staging.raw_ecommerce(
            order_id     NVARCHAR(200),
			customer_id  NVARCHAR(200),
			order_date   NVARCHAR(200),
			ship_date    NVARCHAR(200),
			product_id   NVARCHAR(200),
			product_category   NVARCHAR(200),
			product_name    NVARCHAR(200),
			quantity     NVARCHAR(200),
			unit_price   NVARCHAR(200),
			total_price  NVARCHAR(200),
			discount_code   NVARCHAR(200),
			payment_method  NVARCHAR(200),
			city         NVARCHAR(200),
			zip_code     NVARCHAR(200),
			order_status NVARCHAR(200),
			loaded_at    DATETIME2 DEFAULT
SYSUTCDATETIME()
);

-- the wizard load created a new staging table in our schema, Confirming the location of the load and retrieving dataset
SELECT
      'staging.raw_ecommerce' AS table_name,
	  COUNT(*) AS total_rows
	  FROM staging.raw_ecommerce
UNION ALL
SELECT 
     'staging.ecommerce_dataset' AS table_name,
	 COUNT(*) AS total_rows
	 FROM staging.ecommerce_dataset;

--Moving the data into the correct staging table (staging.raw_ecommerce) from staging.ecommerce_dataset
INSERT INTO staging.raw_ecommerce (
            order_id, customer_id, order_date, ship_date, product_id,
	product_category, product_name, quantity, unit_price,
total_price, discount_code, payment_method, city, zip_code, order_status
)
SELECT
      order_id, customer_id, order_date, ship_date, product_id,
	product_category, product_name, quantity, unit_price,
total_price, discount_code, payment_method, city, zip_code, order_status
	  FROM staging.ecommerce_dataset;

SELECT
     *
	 FROM staging.raw_ecommerce;

--Dropping the staging ecommerce dataset
DROP TABLE staging.ecommerce_dataset;

--creating production schema for data cleaning and transformation
IF SCHEMA_ID('production') IS NULL
   EXEC('CREATE SCHEMA production');
GO

IF OBJECT_ID('production.clean_ecommerce','U') IS NOT NULL
   DROP TABLE production.clean_ecommerce;

--Creating the production table to import, clean and transform dataset
CREATE TABLE production.clean_ecommerce(
            id           INT IDENTITY(1,1) PRIMARY KEY,
            order_id     NVARCHAR(200),
			customer_id  NVARCHAR(200),
			order_date   DATETIME2 NULL,
			ship_date    DATETIME2 NULL,
			product_id   NVARCHAR(200),
			product_category   NVARCHAR(200),
			product_name    NVARCHAR(200),
			quantity     INT NULL,
			unit_price   DECIMAL(14,2) NULL,
			total_price  DECIMAL(18,2) NULL,
			discount_code   NVARCHAR(200),
			payment_method  NVARCHAR(200),
			city         NVARCHAR(200),
			zip_code     NVARCHAR(200),
			order_status NVARCHAR(200),
			loaded_at    DATETIME2 NULL,
			data_quality_flags NVARCHAR(400) NULL,
			created_at   DATETIME2 DEFAULT
	SYSUTCDATETIME()
	);


INSERT INTO production.clean_ecommerce
(order_id, customer_id, loaded_at, data_quality_flags)
SELECT
      NULLIF(LTRIM(RTRIM(order_id)), '') AS order_id,
	  NULLIF(LTRIM(RTRIM(customer_id), '') AS customer_id,
	  loaded_at,
	  CASE
	      WHEN order_id IS NULL OR LTRIM(RTRIM(order_id)) = '' THEN 'missing order_id'
		  WHEN customer_id IS NULL OR LTRIM(RTRIM(customer_id)) = '' THEN 'missing customer_id'
		  ELSE NULL
	END AS data_quality_flags
FROM staging.raw_ecommerce;

--retrieving and confirming columns headers to ensure data integrity
SELECT column_name, ordinal_position, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'staging'and TABLE_NAME = 'raw_ecommerce'
ORDER BY ORDINAL_POSITION;

-- DATA CLEANING AND TRANSFORMATION INTO THE PRODUCTION TABLE
--retrieving data from the staging table
SELECT
     *
	 FROM staging.raw_ecommerce;
--coping clean order_id + customer_id, removed extra spaces from both sides and removed duplicates
INSERT INTO production.clean_ecommerce
(order_id, customer_id)
SELECT DISTINCT
       LTRIM(RTRIM(order_id)) AS order_id,
	   LTRIM(RTRIM(customer_id)) AS customer_id
FROM staging.raw_ecommerce;

--confirming rows inserted successfully
SELECT COUNT(*) AS total_rows
FROM production.clean_ecommerce;

--Retrieving all irregular date patterns 
SELECT DISTINCT order_date
FROM staging.raw_ecommerce;

-- Converting order date into a proper date format for data consistency
UPDATE c
SET c.order_date = 
    TRY_CONVERT(DATE, s.order_date, 101)
FROM production.clean_ecommerce AS c
JOIN staging.raw_ecommerce AS s
ON c.order_id = s.order_id AND c.customer_id = s.customer_id
WHERE c.order_date IS NULL OR ISDATE(s.order_date) = 0;

UPDATE c
SET c.order_date =
    TRY_CONVERT(DATE, s.order_date, 107)
FROM production.clean_ecommerce AS c
JOIN staging.raw_ecommerce AS s
ON c.order_date = s.order_date AND c.customer_id = s.customer_id
WHERE c.order_date IS NULL;

--Retrieving data from the clean table
SELECT
      *
FROM production.clean_ecommerce;

--Creating a backup production dataset, to execute and comfirm accurate query for cleaning the date column
SELECT * INTO production.clean_ecommerce_backup
FROM production.clean_ecommerce;

ALTER TABLE production.clean_ecommerce_backup
ALTER COLUMN order_date DATE;

UPDATE production.clean_ecommerce_backup
SET order_date = CAST(order_date AS DATE)
WHERE order_date IS NOT NULL;

SELECT 
*
FROM production.clean_ecommerce_backup;

--converting datetime2 to DATE to remove extral spacing and 00;00;00 from the column
SELECT CAST(order_date AS DATE) AS c_order_date
FROM production.clean_ecommerce;

--confirming number of NULL values in the order_date
SELECT
     COUNT(*) AS total_records,
	 COUNT(order_date) AS records_with_dates,
	 COUNT(*)- COUNT(order_date) AS records_with_null_dates
FROM production.clean_ecommerce;

-- Alter table structure, to convert DATETIME to DATE
ALTER TABLE production.clean_ecommerce
ALTER COLUMN order_date DATE;

--Updating clean value into the order_date
UPDATE production.clean_ecommerce
SET order_date = CAST(order_date AS DATE)
WHERE order_date IS NOT NULL;

--Retrieving all irregular date patterns 
SELECT DISTINCT ship_date
FROM staging.raw_ecommerce;

-- Converting ship_date into a proper date format for data consistency
UPDATE c
SET c.ship_date = 
    TRY_CONVERT(DATE, s.order_date, 101)
FROM production.clean_ecommerce AS c
JOIN staging.raw_ecommerce AS s
ON c.order_id = s.order_id AND c.customer_id = s.customer_id
WHERE c.ship_date IS NULL OR ISDATE(s.ship_date) = 0;

--converting datetime2 to DATE to remove extral spacing and 00;00;00 from the column
SELECT CAST(ship_date AS DATE) AS c_ship_date
FROM production.clean_ecommerce;

--Alter table structure, to convert DATETIME to DATE
ALTER TABLE production.clean_ecommerce
ALTER COLUMN ship_date DATE;

--Updating clean value into the order_date
UPDATE production.clean_ecommerce
SET ship_date = CAST(ship_date AS DATE)
WHERE ship_date IS NOT NULL;

--Retrieving data from the clean table
SELECT
      *
FROM production.clean_ecommerce;
--Retrieving data from the staging table
SELECT
     *
	 FROM staging.raw_ecommerce;

--Retrieving distinct values to aid data cleaning
SELECT DISTINCT
	 product_id, product_category, product_name
	FROM staging.raw_ecommerce;

--Inserting, cleaning and populating values into the product columns 
UPDATE c
SET
    product_id = NULLIF(LTRIM(RTRIM(s.product_id)),''),
	product_name = NULLIF(LTRIM(RTRIM(s.product_name)),''),
	product_category = NULLIF(LTRIM(RTRIM(s.product_category)),'')
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
ON c.order_id = s.order_id 
AND c.customer_id = s.customer_id;

--confirming distinct values in the quantity column for better data input
SELECT DISTINCT quantity
FROM staging.raw_ecommerce
ORDER BY quantity;

--identifying which columns and data values are affected with the negative value of quantity column, to futher confirm if its an error or legitimate case.
SELECT *
FROM staging.raw_ecommerce
WHERE quantity = -1;

--inserting values into the quantity column
UPDATE c
SET
      quantity = s.quantity
FROM  production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
ON c.order_id = s.order_id
AND c.customer_id = s.customer_id;

--CLEANING THE UNIT & TOTAL PRICE
--Checking for distint patterns and outliers
 SELECT DISTINCT
       unit_price,
	   COUNT(*) AS frequency
FROM staging.raw_ecommerce
GROUP BY unit_price
ORDER BY frequency DESC;

SELECT TOP 20
       total_price,
	   COUNT(*) AS frequency
FROM staging.raw_ecommerce
GROUP BY total_price
ORDER BY frequency DESC;

--Retrieving values with $ to better understand data irregularities
SELECT total_price
FROM staging.raw_ecommerce
WHERE TRY_CAST(REPLACE(total_price, '$', '') AS DECIMAL(10,2)) IS NULL
AND total_price IS NOT NULL;

-- Cleaning irregular $ () , signs from the values to aid data convertion from nvarchar to nuemerical values (decimal).
SELECT TOP 20
total_price,
       CAST(
	       REPLACE(
		       REPLACE(
			       REPLACE(
			           REPLACE(total_price, '$', ''),
				   ',', ''
			       ),
				'(', ''
		        ),
			')', ''
			)
		   AS DECIMAL(10,2)
		)*
		CASE 
		    WHEN total_price LIKE '(%' THEN -1
			ELSE 1
		END as c_total_price
FROM staging.raw_ecommerce;

--error converting nvarchar data type to nuemeric value. possibly due to some signs yet to detect 
UPDATE c
SET
     unit_price = CAST(
	      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(s.unit_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', '') AS DECIMAL(10,2)
	 ) *
	  CASE
	     WHEN s.unit_price LIKE '(%' THEN -1
		 ELSE 1
	 END,
	 total_price = CAST(
	      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(s.total_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', '') AS DECIMAL(10,2)
	 )*
	 CASE
	     WHEN s.total_price LIKE '(%' THEN -1
		 ELSE 1
	 END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
      ON c.order_id = s.order_id
	  AND c.customer_id = s.customer_id;

-- Converting from nvarchar to decimal to aid convertion
ALTER TABLE production.clean_ecommerce
ALTER COLUMN unit_price DECIMAL(10,2);

ALTER TABLE production.clean_ecommerce
ALTER COLUMN total_price DECIMAL(10,2);

--identifying more signs that could possibly disrupt the convertion. [identified USD,$,(,),. and ,] . was used in the count of thausand 1.000 instead 1,000

SELECT 'unit_price' as column_name, unit_price as problematic_value
 FROM staging.raw_ecommerce
 where TRY_CAST(
	   REPLACE(REPLACE(REPLACE(REPLACE(unit_price, '$', ''), ',', ''), '(', ''), ')', '') as DECIMAL(10,2)
	   ) IS NULL
	   AND unit_price IS NOT NULL
UNION ALL
SELECT 'total_price' as column_name, total_price as problematic_value
FROM staging.raw_ecommerce
where TRY_CAST(
	   REPLACE(REPLACE(REPLACE(REPLACE(total_price, '$', ''), ',', ''), '(', ''), ')', '') as decimal(10,2)
	   ) IS NULL
	   AND total_price IS NOT NULL;

-- confirming my findings and proper query execution
SELECT TOP 20
       unit_price,
	   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(unit_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', ''), '.', '') as clean_unit,
	   total_price,
	   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(total_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', ''), '.', '') as clean_total
FROM staging.raw_ecommerce;

UPDATE production.clean_ecommerce
SET unit_price = NULL, total_price = NULL;

--updating clean values into the unit and total_price column
UPDATE c
SET
     unit_price = CAST(
	   CASE
	      WHEN s.unit_price LIKE '%.%,%' THEN
	      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(s.unit_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', ''), '.', '')
		  ELSE 
		  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(s.unit_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', '')
		END
		AS DECIMAL(10,2)
	 ) *
	  CASE
	     WHEN s.unit_price LIKE '(%' THEN -1
		 ELSE 1
	 END,
	 total_price = CAST(
	      CASE
	      WHEN s.total_price LIKE '%.%.%' THEN
	      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(s.total_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', ''), '.', '')
		  ELSE 
		  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(s.total_price, '$', ''), ',', ''), '(', ''), ')', ''), 'USD', '')
		END
		AS DECIMAL(10,2)
	 )*
	 CASE
	     WHEN s.total_price LIKE '(%' THEN -1
		 ELSE 1
	 END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
      ON c.order_id = s.order_id
	  AND c.customer_id = s.customer_id;

--Confirming if the negative values were properly added
SELECT 
     product_name,
	 unit_price,
	 quantity,
	 total_price
FROM production.clean_ecommerce
WHERE product_name = 'Electronics Item 2150';

--CLEANING THE DISCOUNT_CODE, PAYMENT_METHOD, CITY, ZIP_CODE & ORDER_STATUS
--Retrieving distinct values from the discount_code
SELECT DISTINCT discount_code
FROM staging.raw_ecommerce
ORDER BY discount_code;

--cleaning case inconsistencies and Null, None & Na should be NULL
UPDATE c
SET
     discount_code = CASE
	   WHEN NULLIF(LTRIM(RTRIM(s.discount_code)), '')IS NULL THEN NULL
	   WHEN UPPER(s.discount_code) IN ('NA', 'NONE', 'NULL') THEN NULL
	   ELSE UPPER(s.discount_code)
	END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
     ON c.order_id = s.order_id
	 AND c.customer_id = s.customer_id;

--Retrieving distinct values from the payment_method
SELECT DISTINCT payment_method
FROM staging.raw_ecommerce
ORDER BY payment_method;

--Cleaning case inconsistency and standardize all the variation and spacing.
UPDATE c
SET
     payment_method = CASE
	   WHEN LOWER(s.payment_method) IN ('Apple Pay', 'ApplePay') THEN 'Apple Pay'
	   WHEN LOWER(s.payment_method) IN ('Bank Transfer', 'BankTransfer') THEN 'Bank Transfer'
	   WHEN LOWER(s.payment_method) IN ('Cash on Delivery', 'CashonDelivery', 'cod') THEN 'Cash on Delivery'
	   WHEN LOWER(s.payment_method) IN ('Credit Card', 'creditcard', 'CC') THEN 'Credit Card'
	   WHEN LOWER(s.payment_method) IN ('Debit Card', 'DebitCard') THEN 'Debit Card'
	   WHEN LOWER(s.payment_method) IN ('Google Pay', 'GooglePay') THEN 'Google Pay'
	   WHEN LOWER(s.payment_method) IN ('Mobile Money', 'MobileMoney', 'MOMO') THEN 'Mobile Money'
	   WHEN LOWER(s.payment_method) IN ('PayPal', 'pay pal') THEN 'PayPal'
	   ELSE s.payment_method
   END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
    ON c.order_id = s.order_id
	AND c.customer_id = s.customer_id;

--Retrieving distinct values from the city column
SELECT DISTINCT city
FROM staging.raw_ecommerce
ORDER BY city;

--FIXING INCORRECT SPELLINGS & SIGNS LIKE -,.() ALSO REMOVING COUNTRY CODES LIKE (ES, DE, UK) and STANDERDIZE CAPITALIZATION. 
--Data cleaning test query (a)
SELECT DISTINCT 
    city as original,
    CASE 
        WHEN city LIKE '%twe%' OR city LIKE '%nwerp%' OR city LIKE '%ntwre%' OR city LIKE '%ntewr%' OR city LIKE '%nwt%' THEN 'Antwerp'
        WHEN city LIKE '%msterd%' OR city LIKE '%msted%' OR city LIKE '%mstred%' OR city LIKE '%msterad%' OR city LIKE '%smte%' OR city LIKE '%mset%' OR city LIKE '%mtse%' THEN 'Amsterdam'
        WHEN city LIKE '%arcelona%' OR city LIKE '%celon%' THEN 'Barcelona'
        WHEN city LIKE '%Hmburg%' THEN 'Hamburg'
        WHEN city LIKE '%Mdrid%' THEN 'Madrid'
        WHEN city LIKE '%Mnchester%' THEN 'Manchester'
        WHEN city LIKE '%Mrseille%' THEN 'Marseille'
        WHEN city LIKE '%Vlencia%' THEN 'Valencia'
        WHEN city LIKE '%Slzburg%' THEN 'Salzburg'
        WHEN city LIKE '%Pris%' THEN 'Paris'
        WHEN city LIKE '%Nples%' THEN 'Naples'
        ELSE city
    END as cleaned
FROM staging.raw_ecommerce
WHERE city LIKE 'a%'
ORDER BY original;

--insertting clean values into City column alphabetically batch (a)
UPDATE c
SET city = CASE 
        WHEN s.city LIKE '%twe%' OR s.city LIKE '%nwerp%' OR s.city LIKE '%ntwre%' OR s.city LIKE '%ntewr%' OR s.city LIKE '%nwt%' THEN 'Antwerp'
        WHEN s.city LIKE '%msterad%' OR s.city LIKE '%mstred%' OR s.city LIKE '%msted%' OR s.city LIKE '%msterd%' OR s.city LIKE '%smte%' OR s.city LIKE '%mset%' OR s.city LIKE '%mtse%' THEN 'Amsterdam'
        WHEN s.city LIKE '%celon%' OR s.city LIKE '%arcelona%' THEN 'Barcelona'
        WHEN s.city LIKE '%Hmburg%' THEN 'Hamburg'
        WHEN s.city LIKE '%Mdrid%' THEN 'Madrid'
        WHEN s.city LIKE '%Mnchester%' THEN 'Manchester'
        WHEN s.city LIKE '%Mrseille%' THEN 'Marseille'
        WHEN s.city LIKE '%Vlencia%' THEN 'Valencia'
        WHEN s.city LIKE '%Slzburg%' THEN 'Salzburg'
        WHEN s.city LIKE '%Pris%' THEN 'Paris'
        WHEN s.city LIKE '%Nples%' THEN 'Naples'
        ELSE s.city
    END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s 
    ON c.order_id = s.order_id 
    AND c.customer_id = s.customer_id
WHERE s.city LIKE 'a%';

-- Data cleaning Test Query (B)
SELECT DISTINCT 
    city as original,
    CASE 
        WHEN city LIKE 'Barcelon%' OR city LIKE '%reclon%' OR city LIKE '%crelo%' OR city LIKE '%celno%' OR city LIKE '%racel%' OR city LIKE '%arc%' THEN 'Barcelona'
		WHEN city LIKE 'Berg%' OR city LIKE '%egre%' OR city LIKE '%rege%' OR city LIKE '%ereg%' THEN 'Bergen'
		WHEN city LIKE 'Berl%' OR city LIKE '%elri%' OR city LIKE '%reli%' OR city LIKE '%eril%' THEN 'Berlin'
		WHEN city LIKE 'Birmingh%' OR city LIKE '%irimn%' OR city LIKE '%rimingh%' OR city LIKE '%irm%' THEN 'Birmingham'
		WHEN city LIKE 'Brussel%' OR city LIKE '%rsuse%' OR city LIKE '%ruses%' OR city LIKE '%ursse%' OR city LIKE '%russ%' THEN 'Brussels'
		ELSE city
	END as cleaned
FROM staging.raw_ecommerce
WHERE city LIKE 'B%'
ORDER BY original;

--insertting clean values into City column alphabetically batch(B)
UPDATE c
SET city = CASE 
        WHEN s.city LIKE 'Barcelon%' OR s.city LIKE '%reclon%' OR s.city LIKE '%crelo%' OR s.city LIKE '%celno%' OR s.city LIKE '%racel%' OR s.city LIKE '%arc%' THEN 'Barcelona'
        WHEN s.city LIKE 'Berg%' OR s.city LIKE '%egre%' OR s.city LIKE '%rege%' OR s.city LIKE '%ereg%' THEN 'Bergen' 
        WHEN s.city LIKE 'Berl%' OR s.city LIKE '%elri%' OR s.city LIKE '%reli%' OR s.city LIKE '%eril%' THEN 'Berlin'
        WHEN s.city LIKE 'Birmingh%' OR s.city LIKE '%irimn%' OR s.city LIKE '%rimingh%' OR s.city LIKE '%irm%' THEN 'Birmingham'
        WHEN s.city LIKE 'Brussel%' OR s.city LIKE '%rsuse%' OR s.city LIKE '%ruses%' OR s.city LIKE '%ursse%' OR s.city LIKE '%russ%' THEN 'Brussels'
        ELSE s.city
    END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s 
    ON c.order_id = s.order_id 
    AND c.customer_id = s.customer_id
WHERE s.city LIKE 'B%';

-- Data cleaning Test Query (C-J)
SELECT DISTINCT 
    city as original,
    CASE 
        WHEN city LIKE '%ope%' OR city LIKE '%pnеh%' OR city LIKE '%оеnh%' OR city LIKE '%epnh%' OR city LIKE '%ehag%' OR city LIKE '%nhag%'THEN 'Copenhagen'
        WHEN city LIKE '%ubl%' OR city LIKE '%bul%' OR city LIKE '%ubi%' OR city LIKE '%ulb%' THEN 'Dublin'
        WHEN city LIKE '%enev%' OR city LIKE '%Gnev%' OR city LIKE '%enve%' OR city LIKE '%enea%' OR city LIKE '%eenv%' OR city LIKE '%neev%' THEN 'Geneva'
        WHEN city LIKE 'Glas%' OR city LIKE '%lsag%' OR city LIKE '%lagso%' OR city LIKE '%alsg%' THEN 'Glasgow'
        WHEN city LIKE 'Goth%' OR city LIKE '%tohen%' OR city LIKE '%оteh%' OR city LIKE '%ohte%' OR city LIKE '%hnbu%' THEN 'Gothenburg'
        WHEN city LIKE '%ambu%' OR city LIKE '%mabur%' OR city LIKE '%amub%' OR city LIKE '%ambr%' OR city LIKE '%abmu%' THEN 'Hamburg'
        WHEN city LIKE 'eBrlin%' THEN 'Berlin'
        WHEN city LIKE 'eBrgen%' THEN 'Bergen'
        WHEN city LIKE 'eLeds%' THEN 'Leeds'
        WHEN city LIKE 'ilsbon%' THEN 'Lisbon'
        WHEN city LIKE 'iMlan%' THEN 'Milan'
        WHEN city LIKE 'iVenna%' THEN 'Vienna'
		WHEN city LIKE 'iBrmingham%' THEN 'Birmingham'
        ELSE city
    END as cleaned
FROM staging.raw_ecommerce
WHERE city >= 'C' AND city < 'J'
ORDER BY original;

--insertting clean values into City column alphabetically batch(C-J)
UPDATE c
SET city = CASE
        WHEN s.city LIKE '%ope%' OR s.city LIKE '%pnеh%' OR s.city LIKE '%оеnh%' OR s.city LIKE '%epnh%' OR s.city LIKE '%ehag%' OR s.city LIKE '%nhag%'THEN 'Copenhagen'
        WHEN s.city LIKE '%ubl%' OR s.city LIKE '%bul%' OR s.city LIKE '%ubi%' OR s.city LIKE '%ulb%' THEN 'Dublin'
        WHEN s.city LIKE '%enev%' OR s.city LIKE '%Gnev%' OR s.city LIKE '%enve%' OR s.city LIKE '%enea%' OR s.city LIKE '%eenv%' OR s.city LIKE '%neev%' THEN 'Geneva'
        WHEN s.city LIKE 'Glas%' OR s.city LIKE '%lsag%' OR s.city LIKE '%lagso%' OR s.city LIKE '%alsg%' THEN 'Glasgow'
        WHEN s.city LIKE 'Goth%' OR s.city LIKE '%tohen%' OR s.city LIKE '%оteh%' OR s.city LIKE '%ohte%' OR s.city LIKE '%hnbu%' THEN 'Gothenburg'
        WHEN s.city LIKE '%ambu%' OR s.city LIKE '%mabur%' OR s.city LIKE '%amub%' OR s.city LIKE '%ambr%' OR s.city LIKE '%abmu%' THEN 'Hamburg'
        WHEN s.city LIKE 'eBrlin%' THEN 'Berlin'
        WHEN s.city LIKE 'eBrgen%' THEN 'Bergen'
        WHEN s.city LIKE 'eLeds%' THEN 'Leeds'
        WHEN s.city LIKE 'ilsbon%' THEN 'Lisbon'
        WHEN s.city LIKE 'iMlan%' THEN 'Milan'
        WHEN s.city LIKE 'iVenna%' THEN 'Vienna'
		WHEN s.city LIKE 'iBrmingham%' THEN 'Birmingham'
        ELSE s.city 
   END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s 
    ON c.order_id = s.order_id 
    AND c.customer_id = s.customer_id
WHERE s.city >= 'C' AND s.city < 'J';

--Data cleaning Test Query (L-N)
SELECT DISTINCT 
    city as original,
    CASE 
        WHEN city LIKE '%eed%' OR city LIKE '%edes%' OR city LIKE '%ees%' THEN 'Leeds'
        WHEN city LIKE '%isbo%' OR city LIKE '%ibso%'  OR city LIKE '%isbn%'  OR city LIKE '%isob%'  OR city LIKE '%sibo%' THEN 'Lisbon'
        WHEN city LIKE 'Londo%'  OR city LIKE '%nodo%'  OR city LIKE '%odno%'  OR city LIKE '%onod%' OR city LIKE '%ondn%' THEN 'London'
        WHEN city LIKE 'Lyo%' OR city LIKE 'Lyn%'  OR city LIKE '%oyn%' THEN 'Lyon'
        WHEN city LIKE '%adri%' OR city LIKE '%aral%'  OR city LIKE 'mad%'  OR city LIKE '%ardi%'  OR city LIKE '%dari%' THEN 'Madrid'
        WHEN city LIKE '%anchest%' OR city LIKE '%naches%'  OR city LIKE '%cnhes%'  OR city LIKE '%ncehs%' OR city LIKE '%chetser%'  OR city LIKE '%anches%'  OR city LIKE '%nchse%'  OR city LIKE '%nhces%' THEN 'Manchester'
        WHEN city LIKE '%arseil%' OR city LIKE '%rasel%'  OR city LIKE '%resi%'  OR city LIKE '%arsel%' OR city LIKE '%seill%' OR city LIKE '%arsie%'  OR city LIKE '%sreil%' THEN 'Marseille'
        WHEN city LIKE 'Mila%' OR city LIKE 'Mial%'  OR city LIKE '%ilna%'  OR city LIKE '%lian%' THEN 'Milan'
        WHEN city LIKE 'Muni%' OR city LIKE '%nuic%'  OR city LIKE '%uinc%'  OR city LIKE '%unci%'  OR city LIKE '%unih%' THEN 'Munich'
        WHEN city LIKE '%aple%' OR city LIKE '%alpe%'  OR city LIKE '%apls%'  OR city LIKE '%pale%' OR city LIKE '%apel%' THEN 'Naples'
		WHEN city LIKE 'nAtwerp%' THEN 'Antwerp'
		WHEN city LIKE 'mAsterdam%' THEN 'Amsterdam'
		WHEN city LIKE 'lGasgow%' THEN 'Glasgow'
        ELSE city
    END as cleaned
FROM staging.raw_ecommerce
WHERE city >= 'L' AND city < 'O'
ORDER BY original;

--insertting clean values into City column alphabetically batch(L-O)
UPDATE c
SET city = CASE
        WHEN s.city LIKE '%eed%' OR s.city LIKE '%edes%' OR s.city LIKE '%ees%' THEN 'Leeds'
        WHEN s.city LIKE '%isbo%' OR s.city LIKE '%ibso%'  OR s.city LIKE '%isbn%'  OR s.city LIKE '%isob%'  OR s.city LIKE '%sibo%' THEN 'Lisbon'
        WHEN s.city LIKE 'Londo%'  OR s.city LIKE '%nodo%'  OR s.city LIKE '%odno%'  OR s.city LIKE '%onod%' OR s.city LIKE '%ondn%' THEN 'London'
        WHEN s.city LIKE 'Lyo%' OR s.city LIKE 'Lyn%'  OR s.city LIKE '%oyn%' THEN 'Lyon'
        WHEN s.city LIKE '%adri%' OR s.city LIKE '%aral%'  OR s.city LIKE 'mad%'  OR s.city LIKE '%ardi%'  OR s.city LIKE '%dari%' THEN 'Madrid'
        WHEN s.city LIKE '%anchest%' OR s.city LIKE '%naches%'  OR s.city LIKE '%cnhes%'  OR s.city LIKE '%ncehs%' OR s.city LIKE '%chetser%'  OR s.city LIKE '%anches%'  OR s.city LIKE '%nchse%'  OR s.city LIKE '%nhces%' THEN 'Manchester'
        WHEN s.city LIKE '%arseil%' OR s.city LIKE '%rasel%'  OR s.city LIKE '%resi%'  OR s.city LIKE '%arsel%' OR s.city LIKE '%seill%' OR s.city LIKE '%arsie%'  OR s.city LIKE '%sreil%' THEN 'Marseille'
        WHEN s.city LIKE 'Mila%' OR s.city LIKE 'Mial%'  OR s.city LIKE '%ilna%'  OR s.city LIKE '%lian%' THEN 'Milan'
        WHEN s.city LIKE 'Muni%' OR s.city LIKE '%nuic%'  OR s.city LIKE '%uinc%'  OR s.city LIKE '%unci%'  OR s.city LIKE '%unih%' THEN 'Munich'
        WHEN s.city LIKE '%aple%' OR s.city LIKE '%alpe%'  OR s.city LIKE '%apls%'  OR s.city LIKE '%pale%' OR s.city LIKE '%apel%' THEN 'Naples'
		WHEN s.city LIKE 'nAtwerp%' THEN 'Antwerp'
		WHEN s.city LIKE 'mAsterdam%' THEN 'Amsterdam'
		WHEN s.city LIKE 'lGasgow%' THEN 'Glasgow'
        ELSE s.city
    END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s 
    ON c.order_id = s.order_id 
    AND c.customer_id = s.customer_id
WHERE s.city >= 'L' AND s.city < 'O';

--Data cleaning Test Query (O-Z)
SELECT DISTINCT 
    city as original,
    CASE 
        WHEN city LIKE '%openh%' OR city LIKE '%Cpenh%'  OR city LIKE '%nhage%' THEN 'Copenhagen'
        WHEN city LIKE '%othenburg%' OR city LIKE '%henbur%' THEN 'Gothenburg'
        WHEN city LIKE '%ondon%' OR city LIKE '%Lndon%' THEN 'London'
        WHEN city LIKE 'Oslo%' OR city LIKE 'Olso%' OR city LIKE '%Osol%' OR city LIKE 'sOlo%' THEN 'Oslo'
        WHEN city LIKE 'Paris%' OR city LIKE '%airs%' OR city LIKE '%arsi%' OR city LIKE 'Prais%' THEN 'Paris'
        WHEN city LIKE 'Porto%' OR city LIKE 'Porot%' OR city LIKE 'Potro%' OR city LIKE '%Prto%' OR city LIKE 'Proto%' THEN 'Porto'
        WHEN city LIKE 'Rome%' OR city LIKE '%Rme%' OR city LIKE '%Roem%' OR city LIKE '%oRme%' OR city LIKE 'Rmoe%' OR city LIKE '%Roem%' THEN 'Rome'
        WHEN city LIKE '%otterd%' OR city LIKE '%otted%' OR city LIKE '%tterd%' OR city LIKE '%tetrd%' OR city LIKE '%ttred%' OR city LIKE '%toterd%' OR city LIKE '%ottera%' THEN 'Rotterdam'
        WHEN city LIKE '%alzburg%' OR city LIKE '%albzu%' OR city LIKE '%alzbu%' OR city LIKE '%azlbu%' OR city LIKE '%lazbu%' OR city LIKE '%alzub%' OR city LIKE '%alzbr%' THEN 'Salzburg'
        WHEN city LIKE '%tockholm%' OR city LIKE '%okcho%' OR city LIKE 'Stock%' OR city LIKE '%otckh%' OR city LIKE '%ockho%' OR city LIKE '%ochko%' OR city LIKE '%tcokh%' THEN 'Stockholm'
        WHEN city LIKE '%Dblin%' THEN 'Dublin'
        WHEN city LIKE '%Mnich%' THEN 'Munich'
        WHEN city LIKE '%urich%' OR city LIKE '%Zrich%' OR city LIKE '%ruich%' OR city LIKE '%uirch%' OR city LIKE '%urcih%' OR city LIKE '%urihc%' THEN 'Zurich'
        WHEN city LIKE '%alencia%' OR city LIKE '%aelncia%' OR city LIKE '%laenc%' OR city LIKE 'Valen%' OR city LIKE '%alecn%' OR city LIKE '%alnec%' THEN 'Valencia'
        WHEN city LIKE '%ienna%' OR city LIKE '%enna%' OR city LIKE '%einn%' OR city LIKE 'Vien%' OR city LIKE 'Vinen%' THEN 'Vienna'
        WHEN city LIKE 'yLon%' OR city LIKE '%Lon%' THEN 'Lyon'
		WHEN city LIKE 'rBussels%' THEN 'Brussels'
        ELSE city
    END as cleaned
FROM staging.raw_ecommerce
WHERE city >= 'O'
ORDER BY original;

--insertting clean values into City column alphabetically batch(O-Z)
UPDATE c
SET city = CASE
        WHEN s.city LIKE '%openh%' OR s.city LIKE '%Cpenh%'  OR s.city LIKE '%nhage%' THEN 'Copenhagen'
        WHEN s.city LIKE '%othenburg%' OR s.city LIKE '%henbur%' THEN 'Gothenburg'
        WHEN s.city LIKE '%ondon%' OR s.city LIKE '%Lndon%' THEN 'London'
        WHEN s.city LIKE 'Oslo%' OR s.city LIKE 'Olso%' OR s.city LIKE '%Osol%' OR s.city LIKE 'sOlo%' THEN 'Oslo'
        WHEN s.city LIKE 'Paris%' OR s.city LIKE '%airs%' OR s.city LIKE '%arsi%' OR s.city LIKE 'Prais%' THEN 'Paris'
        WHEN s.city LIKE 'Porto%' OR s.city LIKE 'Porot%' OR s.city LIKE 'Potro%' OR s.city LIKE '%Prto%' OR s.city LIKE 'Proto%' THEN 'Porto'
        WHEN s.city LIKE 'Rome%' OR s.city LIKE '%Rme%' OR s.city LIKE '%Roem%' OR s.city LIKE '%oRme%' OR s.city LIKE 'Rmoe%' OR s.city LIKE '%Roem%' THEN 'Rome'
        WHEN s.city LIKE '%otterd%' OR s.city LIKE '%otted%' OR s.city LIKE '%tterd%' OR s.city LIKE '%tetrd%' OR s.city LIKE '%ttred%' OR s.city LIKE '%toterd%' OR s.city LIKE '%ottera%' THEN 'Rotterdam'
        WHEN s.city LIKE '%alzburg%' OR s.city LIKE '%albzu%' OR s.city LIKE '%alzbu%' OR s.city LIKE '%azlbu%' OR s.city LIKE '%lazbu%' OR s.city LIKE '%alzub%' OR s.city LIKE '%alzbr%' THEN 'Salzburg'
        WHEN s.city LIKE '%tockholm%' OR s.city LIKE '%okcho%' OR s.city LIKE 'Stock%' OR s.city LIKE '%otckh%' OR s.city LIKE '%ockho%' OR s.city LIKE '%ochko%' OR s.city LIKE '%tcokh%' THEN 'Stockholm'
        WHEN s.city LIKE '%Dblin%' THEN 'Dublin'
        WHEN s.city LIKE '%Mnich%' THEN 'Munich'
        WHEN S.city LIKE '%urich%' OR s.city LIKE '%Zrich%' OR s.city LIKE '%ruich%' OR s.city LIKE '%uirch%' OR s.city LIKE '%urcih%' OR s.city LIKE '%urihc%' THEN 'Zurich'
        WHEN S.city LIKE '%alencia%' OR s.city LIKE '%aelncia%' OR s.city LIKE '%laenc%' OR s.city LIKE 'Valen%' OR s.city LIKE '%alecn%' OR s.city LIKE '%alnec%' THEN 'Valencia'
        WHEN S.city LIKE '%ienna%' OR s.city LIKE '%enna%' OR s.city LIKE '%einn%' OR s.city LIKE 'Vien%' OR s.city LIKE 'Vinen%' THEN 'Vienna'
        WHEN S.city LIKE 'yLon%' OR s.city LIKE '%Lon%' THEN 'Lyon'
		WHEN S.city LIKE 'rBussels%' THEN 'Brussels'
        ELSE S.city
    END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s 
    ON c.order_id = s.order_id 
    AND c.customer_id = s.customer_id
WHERE s.city >= 'O';

--Retrieving distinct city data from the clean table, to ensure no duplicate or incorrect spelling data
SELECT DISTINCT city
FROM production.clean_ecommerce;

--updating the correct spelling for Birmingham which was wrongly spelt as Bimringham
UPDATE c
SET city = 'Birmingham'
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s 
    ON c.order_id = s.order_id 
    AND c.customer_id = s.customer_id
WHERE s.city LIKE 'Bimringham%';

--Retrieving distinct values from the zip_code column
SELECT DISTINCT zip_code
FROM staging.raw_ecommerce;

--zip_code data value is totally inconsistent and messy, for quality data; creating a city_zip_mapping table, insert accurate zip codes to the cities and populate the clean values to production table. 
CREATE TABLE city_zip_mapping (
    city NVARCHAR(100),
    zip_code NVARCHAR(20)
);
-- Inserting values into the columns
INSERT INTO city_zip_mapping VALUES
('Amsterdam', '1011'),
('Antwerp', '2000'),
('Barcelona', '08001'),
('Bergen', '5003'),
('Berlin', '10115'),
('Birmingham', 'B1'),
('Brussels', '1000'),
('Copenhagen', '1050'),
('Dublin', 'D01'),
('Geneva', '1201'),
('Glasgow', 'G1'),
('Gothenburg', '41101'),
('Hamburg', '20095'),
('Leeds', 'LS1'),
('Lisbon', '1100'),
('London', 'EC1A'),
('Lyon', '69001'),
('Madrid', '28001'),
('Manchester', 'M1'),
('Marseille', '13001'),
('Milan', '20121'),
('Munich', '80331'),
('Naples', '80100'),
('Oslo', '0150'),
('Paris', '75001'),
('Porto', '4000'),
('Rome', '00100'),
('Rotterdam', '3011'),
('Salzburg', '5020'),
('Stockholm', '11129'),
('Valencia', '46001'),
('Vienna', '1010'),
('Zurich', '8001');

--populating the production.clean_ecommerce table
UPDATE c
SET zip_code = m.zip_code
FROM production.clean_ecommerce AS c
INNER JOIN city_zip_mapping AS m 
ON c.city = m.city;

--Retrieving distinct values from the zip_code column
SELECT DISTINCT order_status
FROM staging.raw_ecommerce;

--Cleaning case inconsistency and standardize all the variation and spacing.
UPDATE c
SET
     order_status = CASE
	   WHEN LOWER(s.order_status) IN ('delivered', 'delivred') THEN 'Delivered'
	   WHEN LOWER(s.order_status) IN ('in transit', 'In Transit') THEN 'In Transit'
	   WHEN LOWER(s.order_status) IN ('Cancelled') THEN 'Cancelled'
	   WHEN LOWER(s.order_status) IN ('shipped') THEN 'Shipped'
	   WHEN LOWER(s.order_status) IN ('returned') THEN 'Returned'
	   WHEN LOWER(s.order_status) IN ('pending') THEN 'Pending'
	   ELSE s.order_status
   END
FROM production.clean_ecommerce AS c
INNER JOIN staging.raw_ecommerce AS s
    ON c.order_id = s.order_id
	AND c.customer_id = s.customer_id;

--Dropped the data_quality_flag and loaded_at column
ALTER TABLE production.clean_ecommerce
DROP COLUMN data_quality_flags, loaded_at;

--Retrieving data from the staging table
SELECT
     *
	 FROM staging.raw_ecommerce;

--Retrieving data from the clean table
SELECT
     *  
FROM production.clean_ecommerce;
