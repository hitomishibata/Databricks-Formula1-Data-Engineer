# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

# COMMAND ----------

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits")
constructor_df = spark.read.parquet(f"{processed_folder_path}/constructor")
race_df = spark.read.parquet(f"{processed_folder_path}/race")
driver_df = spark.read.parquet(f"{processed_folder_path}/driver")
result_df = spark.read.parquet(f"{processed_folder_path}/results")



# COMMAND ----------

race_circuit_df = race_df.join(circuit_df, race_df.circuit_id == circuit_df.circuit_id)

# COMMAND ----------

joined_df = result_df.join(race_circuit_df, result_df.race_id == race_circuit_df.race_id) \
                     .join(driver_df, result_df.driver_id == driver_df.driver_id) \
                     .join(constructor_df, result_df.constructor_id == constructor_df.constructor_id)

# COMMAND ----------

selected_joined_df = joined_df.select(col("result_id"), col("driver_name"), col("driver_nationality"), col("dob"), col("team_name"), col("race_name"), col("circuit_name"), col("race_year"), col("position_order"), col("rank"), col("points"), col("laps"))

# COMMAND ----------

final_joined_df = ingestion_date(selected_joined_df)

# COMMAND ----------

final_joined_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/joined_results")

# COMMAND ----------

joined_result_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/joined_results")

# COMMAND ----------

joined_result_df.write.format("parquet").saveAsTable("joined_results")

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank = Window.partitionBy("race_year").orderBy(desc("total_points"))

# COMMAND ----------

driver_rank_df = joined_df.groupBy("race_year", "driver_name", "driver_nationality").agg(sum(col("points")).alias("total_points"), countDistinct(col("race_name")).alias("number_of_races"))

# COMMAND ----------

final_driver_rank_df = driver_rank_df.withColumn("rank", rank().over(driver_rank))

# COMMAND ----------

final_driver_rank_df.write.format("parquet").saveAsTable("driver_ranks")
