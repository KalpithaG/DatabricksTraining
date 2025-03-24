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
