# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

# COMMAND ----------

laptimes_schema = StructType(fields=[StructField("_c0", IntegerType(), True),
                                      StructField("_c1", IntegerType(), False),
                                      StructField("_c2", StringType(), True),
                                      StructField("_c3", IntegerType(), True),
                                      StructField("_c4", StringType(), True),
                                      StructField("_c5", IntegerType(), True),
                                     ])

# COMMAND ----------

lap_df = spark.read \
.schema(laptimes_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

renamed_laptimes_df = lap_df.withColumnRenamed("_c0", "race_id") \
                            .withColumnRenamed("_c1", "driver_id") \
                            .withColumnRenamed("_c2", "lap") \
                            .withColumnRenamed("_c3", "position") \
                            .withColumnRenamed("_c4", "time") \
                            .withColumnRenamed("_c5", "milliseconds")

# COMMAND ----------

final_laptimes_df = ingestion_date(renamed_laptimes_df)

# COMMAND ----------

final_laptimes_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

lap_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

lap_df.write.format("parquet").saveAsTable("lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.driver_name, d.driver_nationality, l.position, l.time
# MAGIC FROM formula1.driver d
# MAGIC JOIN formula1.lap_times l
# MAGIC ON d.driver_id = l.driver_id
# MAGIC WHERE position = 1
# MAGIC ORDER BY l.time
