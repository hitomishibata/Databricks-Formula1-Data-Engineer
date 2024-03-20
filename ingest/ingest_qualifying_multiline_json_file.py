# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), False),
                                       StructField("constructorId", IntegerType(), False),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read \
.option("multiline", True) \
.schema(qualifying_schema) \
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

renamed_qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

final_qualifying_df = ingestion_date(renamed_qualifying_df)

# COMMAND ----------

final_qualifying_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

qualifying_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

qualifying_df.write.format("parquet").saveAsTable("qualifying")
