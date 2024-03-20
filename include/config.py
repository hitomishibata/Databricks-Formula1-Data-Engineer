# Databricks notebook source
raw_folder_path = 'abfss://raw@dludemyes.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@dludemyes.dfs.core.windows.net'



# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dludemyes.dfs.core.windows.net",
    "youraccountkey"
)
