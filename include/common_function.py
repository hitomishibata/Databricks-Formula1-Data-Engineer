# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
def ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df
