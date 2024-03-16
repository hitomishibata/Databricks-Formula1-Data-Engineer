# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

cir_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                StructField("circuitRef", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("location", StringType(), True),
                                StructField("country", StringType(), True),
                                StructField("lat", DoubleType(), True),
                                StructField("lng", DoubleType(), True),
                                StructField("alt", IntegerType(), True),
                                StructField("url", StringType(), True),])

# COMMAND ----------

circuit_df = spark.read.option("header",True).schema(cir_schema).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

cir_selected_df = circuit_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col( "lat"), col("lng"), col("alt"))

# COMMAND ----------

renamed_circuit_df = cir_selected_df.withColumnRenamed("circuitId", "circuit_id")\
                                  .withColumnRenamed("circuitRef", "circuit_ref")\
                                  .withColumnRenamed("name", "circuit_name")\
                                  .withColumnRenamed("country", "circuit_country")\
                                  .withColumnRenamed("lat","latitude") \
                                  .withColumnRenamed("lng","longitude") \
                                  .withColumnRenamed("alt","altitude")

# COMMAND ----------

country_lookup_df = spark.read.option("header", True).csv(f"{raw_folder_path}/country_lookup.csv")

# COMMAND ----------

joined_circuit_df = renamed_circuit_df.join(country_lookup_df, renamed_circuit_df.circuit_country == country_lookup_df.country)

# COMMAND ----------

final_circuit_df = joined_circuit_df.select(col("circuit_id"),col("circuit_ref"),col("circuit_name"), col("circuit_country"), col("country_code_2_digit"), col("continent"), col("latitude"), col("longitude"), col("altitude")).withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_circuit_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

processed_circuit_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

processed_circuit_df.write.format("parquet").saveAsTable("circuit_country")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT continent, circuit_country, count(*) AS number_of_races
# MAGIC FROM circuit_country
# MAGIC GROUP BY continent, circuit_country
# MAGIC ORDER BY number_of_races DESC
