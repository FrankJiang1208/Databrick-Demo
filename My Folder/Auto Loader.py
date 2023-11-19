# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Exploring The Source Directory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Auto Loader

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC Landing New Files

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)

# COMMAND ----------


