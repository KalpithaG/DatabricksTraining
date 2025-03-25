-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE sales_bronze
-- COMMENT "The customers buying finished products, ingested from /databricks-datasets."
-- TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM STREAM read_files(
  "/Volumes/cgi_dev/naval/sales",
   format => "csv"
   );

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE products_bronze
-- COMMENT "The customers buying finished products, ingested from /databricks-datasets."
-- TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM STREAM read_files(
  "/Volumes/cgi_dev/naval/products",
   format => "csv"
   );

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_bronze
-- COMMENT "The customers buying finished products, ingested from /databricks-datasets."
-- TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM STREAM read_files(
  "/Volumes/cgi_dev/naval/customers",
   format => "csv"
   );

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sales_cleaned(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT distinct * except (_rescued_data) from stream(sales_bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE product_silver;

APPLY CHANGES INTO
  product_silver
FROM
  stream(products_bronze)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum,_rescued_data)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO
  customers_silver
FROM
  stream(customers_bronze)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum,_rescued_data)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

create or refresh materialized view customers_active as
select * from cgi_dev.kalpitha_bronze.customers_silver where __END_AT is null

-- COMMAND ----------

create or replace materialized view cgi_dev.kalpitha_gold.sales_details as 
(SELECT 
    o.order_id,
    o.order_date,
    o.customer_id,
    c.customer_name,
    c.customer_email,
    c.customer_city,
    o.product_id,
    p.product_name,
    p.product_category,
    p.product_price,
    o.quantity,
    o.discount_amount,
    o.total_amount
FROM cgi_dev.kalpitha_bronze.sales_cleaned o
JOIN cgi_dev.kalpitha_bronze.product_silver p
  ON o.product_id = p.product_id
JOIN (
      SELECT DISTINCT customer_id, customer_name, customer_email, customer_city 
      FROM cgi_dev.kalpitha_bronze.customers_active
     ) c
  ON o.customer_id = c.customer_id)

-- COMMAND ----------

create or replace live view customer_revenue as
SELECT 
    customer_id, 
    customer_name, 
    round(SUM(total_amount)) AS total_revenue
FROM cgi_dev.kalpitha_gold.sales_details
GROUP BY customer_id, customer_name
ORDER BY total_revenue DESC
LIMIT 3;
