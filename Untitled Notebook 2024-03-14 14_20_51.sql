-- Databricks notebook source
raw_folder_path = 'abfss://raw@dludemyes.dfs.core.windows.net'
raw_covid_folder_path = 'abfss://covidraw@dludemyes.dfs.core.windows.net'
processed_covid_folder_path = 'abfss://covidprocessed@dludemyes.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@dludemyes.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@dludemyes.dfs.core.windows.net'


-- COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dludemyes.dfs.core.windows.net",
    "Tt1gweEejiPIA5ODGth2hES63ZifDSVd5tQPY8mxoV1O8bXaE8B8F4zcFG7tec3fs4PqM7ImpAEh+ASt0tonkQ=="
)
