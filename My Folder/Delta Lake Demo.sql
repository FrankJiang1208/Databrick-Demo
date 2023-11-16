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

create table managed_default

-- COMMAND ----------

drop table managed_default

-- COMMAND ----------

create table managed_default
 (width INT, length INT, height INT);

insert into managed_default
values (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

describe extended managed_default

-- COMMAND ----------

create table Ext_default
(width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';

insert into Ext_default
values (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

create schema new_default

-- COMMAND ----------

describe database extended new_default

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

use default;
drop table managed_default

-- COMMAND ----------

drop table Ext_default

-- COMMAND ----------

USE new_default;
DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

-- COMMAND ----------


