# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

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

renamed_race_df = race_df.withColumnRenamed("raceId","race_id") \
     .withColumnRenamed("year", "race_year") \
     .withColumnRenamed("round", "race_round") \
     .withColumnRenamed("name", "race_name") \
     .withColumnRenamed("circuitId", "circuit_id") \
     .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) 


# COMMAND ----------

final_race_df = ingestion_date(renamed_race_df)
final_race_df = final_race_df.drop('date', 'time')

# COMMAND ----------

display(final_race_df)

# COMMAND ----------

final_race_df.write.mode("overwrite").parquet(f"{processed_folder_path}/race")

# COMMAND ----------

race_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/race")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE formula1

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
