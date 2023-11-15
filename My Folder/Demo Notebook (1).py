# Databricks notebook source
print('hello World!')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'hello world'

# COMMAND ----------

#%run ./Folder/NotebookName

#Allow to run from another notebook

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files=dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------


