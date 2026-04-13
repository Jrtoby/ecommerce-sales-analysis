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