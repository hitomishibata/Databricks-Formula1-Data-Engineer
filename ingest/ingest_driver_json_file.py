# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                  StructField("driverRef", StringType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("code", StringType(), True),
                                  StructField("name", name_schema),
                                  StructField("dob", DateType(), True),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True)
                                  ])

# COMMAND ----------

driver_df = spark.read \
.option("header", True) \
.schema(driver_schema) \
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

renamed_driver_df = driver_df.withColumnRenamed("driverId", "driver_id") \
                           .withColumnRenamed("driverRef", "driver_ref") \
                           .withColumnRenamed("number", "driver_number") \
                           .withColumnRenamed("code", "driver_code") \
                           .withColumn("driver_name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                           .withColumnRenamed("nationality", "driver_nationality") \
                           .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_driver_df = renamed_driver_df.drop(col("url"), col("name"))

# COMMAND ----------

display(final_driver_df)

# COMMAND ----------

final_driver_df.write.mode("overwrite").parquet(f"{processed_folder_path}/driver")

# COMMAND ----------

driver_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/driver")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

driver_df.write.format("parquet").saveAsTable("driver")
