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

-- Alter table structure, to convert DATETIME to DATE to remove extral spacing and 00;00;00 from the column
ALTER TABLE production.clean_ecommerce
ALTER COLUMN order_date DATE;

--Updating clean value into the order_date
UPDATE production.clean_ecommerce
SET order_date = CAST(order_date AS DATE)
WHERE order_date IS NOT NULL;

--Retrieving distinct values to aid data cleaning in the product column
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

--CLEANING THE DISCOUNT_CODE, PAYMENT_METHOD, CITY, ZIP_CODE & ORDER_STATUS

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