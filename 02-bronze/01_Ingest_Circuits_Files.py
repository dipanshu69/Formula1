# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest circuits.csv file
# MAGIC ### 1. Read the file using spark dataframe reader API
# MAGIC ### 2. Add Metadata Columns 
# MAGIC -       Source File
# MAGIC -       Ingestion Timestamp
# MAGIC ### 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using the dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")

# COMMAND ----------

v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01_Environmnet_config

# COMMAND ----------

# MAGIC %run  ../00-common/02_bronze_helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

print(source_file)
print(table_name)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

circuits_schema = StructType(
    [
        StructField("circuitId", StringType()),
        StructField("url", StringType()),
        StructField("circuitName", StringType()),
        StructField("lat", DoubleType()),
        StructField("long", DoubleType()),
        StructField("locality", StringType()),
        StructField("country", StringType()),
    ]
)

# COMMAND ----------

circuits_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(circuits_schema)
    .load(source_file)
)

# COMMAND ----------

# DBTITLE 1,Cell 5
circuits_final_df = add_ingestion_medatat(circuits_df)

# COMMAND ----------

write_to_bronze(circuits_final_df, table_name, v_batch_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.circuits
# MAGIC where batch_id = '2025-01'
