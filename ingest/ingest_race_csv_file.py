# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                StructField("year", IntegerType(), True),
                                StructField("round", IntegerType(), True),
                                StructField("circuitId", IntegerType(), False),
                                StructField("name", StringType(), True),
                                StructField("date", DateType(), True),
                                StructField("time", StringType(), True)])

# COMMAND ----------

race_df = spark.read \
.option("header", True) \
.schema(race_schema) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

time_race_df = race_df.withColumn("ingestion_date", current_timestamp()) \
       .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

race_renamed_df = time_race_df.withColumnRenamed("raceId","race_id") \
     .withColumnRenamed("year", "race_year") \
     .withColumnRenamed("round", "race_round") \
     .withColumnRenamed("name", "race_name") \
     .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

final_race_df = race_renamed_df.select(col('race_id'), col('race_year'), col('race_round'), col('circuit_id'), col('race_name'), col('race_timestamp'), col('ingestion_date'))

# COMMAND ----------

final_race_df.write.mode("overwrite").parquet(f"{processed_folder_path}/race")

# COMMAND ----------

race_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/race")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE race

# COMMAND ----------

race_df.write.format("parquet").saveAsTable("race")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(race_name)
# MAGIC FROM formula1.race
# MAGIC WHERE race_name NOT LIKE "%Grand Prix%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM formula1.race

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_name,COUNT(race_id) AS number_of_races
# MAGIC FROM formula1.race
# MAGIC GROUP BY race_name
# MAGIC ORDER BY number_of_races DESC
