# Databricks notebook source
# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_function"

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

renamed_constructor_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                             .withColumnRenamed("constructorRef", "constructor_ref") \
                             .withColumnRenamed("name", "team_name") \
                             .withColumnRenamed("nationality", "team_nationality")

# COMMAND ----------

final_constructor_df = ingestion_date(renamed_constructor_df)

# COMMAND ----------

final_constructor_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructor")

# COMMAND ----------

constructor_df = spark.read.option("header", True).parquet(f"{processed_folder_path}/constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_nationality, COUNT(*) AS number_of_teams
# MAGIC FROM formula1.constructor
# MAGIC GROUP BY team_nationality
# MAGIC ORDER BY number_of_teams DESC
