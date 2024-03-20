# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

# COMMAND ----------

result_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), False),
                                   StructField("driverId", IntegerType(), False),
                                   StructField("constructorId", IntegerType(), False),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("fastestLap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", StringType(), True)
                                   ])

# COMMAND ----------

result_df = spark.read \
.option("header", True) \
.schema(result_schema) \
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

final_result_df = result_df.withColumnRenamed("resultId", "result_id") \
                           .withColumnRenamed("raceId", "race_id") \
                           .withColumnRenamed("driverId", "driver_id") \
                           .withColumnRenamed("constructorId", "constructor_id") \
                           .withColumnRenamed("positionText", "position_text") \
                           .withColumnRenamed("positionOrder", "position_order") \
                           .withColumnRenamed("fastestLap", "fastest_lap") \
                           .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                           .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

final_result_df = ingestion_date(final_result_df)

# COMMAND ----------

display(final_result_df)

# COMMAND ----------

final_result_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

result_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

result_df.write.format("parquet").saveAsTable("results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM formula1.results
