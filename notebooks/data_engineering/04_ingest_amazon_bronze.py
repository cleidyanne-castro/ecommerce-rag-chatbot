#Setup
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_files/amazon"


amazon_path = '/Volumes/workspace/ecommerce_bronze/raw_files/amazon'

display(dbutils.fs.ls(amazon_path))

#Ingest Amazon Products
csv_path = f"{VOLUME_PATH}/*.csv"

amazon_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("escape", '"')
    .option("multiLine", True)
    .csv(csv_path)
)

# Clean column names
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

#Create Ingestion Summary
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
