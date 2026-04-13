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