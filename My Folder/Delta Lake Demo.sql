-- Databricks notebook source
-- MAGIC %sql
-- MAGIC create table employees
-- MAGIC  (id INT, name String, salary DOUBLE)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into employees
-- MAGIC values
-- MAGIC (1,"Adam", 3500.0),
-- MAGIC (2,"Leo", 4500.0),
-- MAGIC (3,"Chris", 5500.0),
-- MAGIC (4,"Mike", 36700.0),
-- MAGIC (5,"Pete", 2300.0),
-- MAGIC (6,"Kim", 500.0)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from employees

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe detail employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC update employees 
-- MAGIC Set salary=salary+100
-- MAGIC where name LIKE "M%"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from employees

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe history employees

-- COMMAND ----------

select * from employees version as of 1

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

delete from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

restore table employees to version as of 2

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

optimize employees
zorder by id

-- COMMAND ----------

vacuum employees

-- COMMAND ----------

SET Spark.databricks.delta.rententionDurationCheck.enabled=false;

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

drop table employees

-- COMMAND ----------


