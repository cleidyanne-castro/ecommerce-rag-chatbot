# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Amazon Bronze Ingestion
# MAGIC %md
# MAGIC ### Amazon Bronze Ingestion
# MAGIC This notebook ingests the Amazon products CSV file into Delta table without applying business transformations.

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_files/amazon"

# COMMAND ----------

# DBTITLE 1,List Volume Files
amazon_path = '/Volumes/workspace/ecommerce_bronze/raw_files/amazon'

display(dbutils.fs.ls(amazon_path))

# COMMAND ----------

# DBTITLE 1,Create Schemas
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_gold;
# MAGIC
# MAGIC -- Remove schema incorreto
# MAGIC DROP SCHEMA IF EXISTS workspace.ecommerce_amazon_silver;
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace LIKE 'ecommerce*';

# COMMAND ----------

# DBTITLE 1,Ingest Amazon Products
csv_path = f"{VOLUME_PATH}/*.csv"

amazon_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("escape", '"')
    .option("multiLine", True)
    .csv(csv_path)
)

# Clean column names: replace spaces with underscores and convert to lowercase
for col in amazon_raw.columns:
    amazon_raw = amazon_raw.withColumnRenamed(col, col.replace(" ", "_").lower())

amazon_bronze = (
    amazon_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file_path", F.col("_metadata.file_path"))
    .withColumn("_source_file_name", F.col("_metadata.file_name"))
)

(
    amazon_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.amazon_products")
)

print(f"Created: {CATALOG}.{BRONZE_SCHEMA}.amazon_products")
print(f"Rows: {amazon_bronze.count():,}")
print(f"Columns: {len(amazon_bronze.columns)}")

display(amazon_bronze.limit(10))

# COMMAND ----------

# DBTITLE 1,Show Tables
# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.ecommerce_bronze

# COMMAND ----------

# DBTITLE 1,Create Ingestion Summary
# Create ingestion summary report
table_summary = []

table_name = "amazon_products"
full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
dataframe = spark.table(full_table_name)

table_summary.append({
    "table_name": table_name,
    "row_count": dataframe.count(),
    "column_count": len(dataframe.columns),
})

summary_df = spark.createDataFrame(table_summary)
display(summary_df)

# COMMAND ----------

# DBTITLE 1,Count Products
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_products
# MAGIC FROM workspace.ecommerce_bronze.amazon_products