-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Overwrite a file

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Can only override exisiting table, and override the existing data schema

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Append Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Does not check for duplicate

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC To check for duplicates

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

Merge into books b
using books_updates u
on b.book_id=u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *

-- COMMAND ----------


