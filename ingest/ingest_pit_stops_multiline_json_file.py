# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                     ])

# COMMAND ----------

pit_stop_df = spark.read \
.option("multiline", True) \
.schema(pit_stops_schema) \
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

renamed_pit_df = pit_stop_df.withColumnRenamed("driverId", "driver_id") \
                           .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

final_pit_df = ingestion_date(renamed_pit_df)

# COMMAND ----------

final_pit_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

pit_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

pit_df.write.format("parquet").saveAsTable("pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM formula1.pit_stops
