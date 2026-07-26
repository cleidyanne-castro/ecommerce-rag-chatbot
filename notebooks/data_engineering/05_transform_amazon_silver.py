%md
Amazon Silver Transformation

This notebook transforms Amazon Bronze data into Silver layer tables with:

-Data quality checks and validation
-Standardized column names and types
-Deduplication
-Enriched product data for RAG applications


#Setup

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"
GOLD_SCHEMA = "ecommerce_gold"

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}"
)

print("Schemas Gold and Silver available")

#Functions

def read_bronze(table_name: str) -> DataFrame:
    """It reads a bronze table"""
    return spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    )

def save_silver(
    dataframe: DataFrame,
    table_name: str,
) -> None: 
    """It saves a dataframe as a delta table on Silver"""

    target_table = (
        f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    )

    (   
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(target_table)
    )
    row_count = dataframe.count()

    print(
        f"Table saved: {target_table} "
        f"({row_count:,} rows)"
    )


#PRODUCTS

#Read the products bronze table
products_bronze = read_bronze("amazon_products")
display(products_bronze.limit(5))
products_bronze.printSchema()

#Clean the data

# Apply deduplication window before select
window_spec = Window.partitionBy("uniq_id").orderBy(F.desc("_ingested_at"))

products_with_row_num = (
    products_bronze
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

products_silver = (
    products_with_row_num
    .select(
        F.trim("uniq_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.trim("brand_name").alias("manufacturer"),
        F.expr("try_cast(regexp_replace(selling_price, '[^0-9.]', '') as double)").alias("price"),
        F.trim("category").alias("category"),
        F.trim("product_dimensions").alias("dimensions"),
        F.trim("about_product").alias("about_product"),
        F.trim("product_description").alias("description"),
        F.col("_ingested_at"),
        F.col("_source_file_name")
    )
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("product_name").isNotNull())
    .filter(F.col("price").isNotNull())
    .dropna(subset=["product_id", "product_name", "price"])
)

print(
    "Bronze rows:",
    products_bronze.count(),
)
print(
    "Silver rows:",
    products_silver.count()
)
print(
    "Distinct product_id's:",
    products_silver.select("product_id").distinct().count()
)

display(products_silver.limit(10))
save_silver(products_silver, "amazon_products")

#PRODUCTS FOR RAG

#Read the silver products table
products_silver_table = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.amazon_products")
display(products_silver_table.limit(5))

#Prepare for RAG with enriched searchable text

products_for_rag = (
    products_silver_table
    .filter(F.col("product_name").isNotNull())
    .filter(F.col("category").isNotNull())
    .withColumn(
        "product_name_length",
        F.length(F.col("product_name"))
    )
    .withColumn(
        "description_length",
        F.coalesce(F.length(F.col("description")), F.lit(0))
    )
    .withColumn(
        "data_completeness_score",
        (
            F.when(F.col("product_name").isNotNull(), 1).otherwise(0) +
            F.when(F.col("manufacturer").isNotNull(), 1).otherwise(0) +
            F.when(F.col("description").isNotNull(), 1).otherwise(0) +
            F.when(F.col("dimensions").isNotNull(), 1).otherwise(0) +
            F.when(F.col("about_product").isNotNull(), 1).otherwise(0)
        ).cast("integer")
    )
    .withColumn(
        "searchable_text",
        F.concat_ws(
            " | ",
            F.col("product_name"),
            F.concat(F.lit("Marca: "), F.coalesce(F.col("manufacturer"), F.lit("desconhecida"))),
            F.concat(F.lit("Preco: US$ "), F.col("price")),
            F.concat(F.lit("Categoria: "), F.col("category")),
            F.when(F.col("dimensions").isNotNull(), 
                   F.concat(F.lit("Dimensoes: "), F.col("dimensions"))
            ).otherwise(F.lit("")),
            F.coalesce(F.col("about_product"), F.lit("")),
            F.coalesce(F.col("description"), F.lit(""))
        )
    )
)

print("RAG Quality Analysis:")
print(f"Silver products (all): {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.amazon_products').count():,}")
print(f"RAG-ready products: {products_for_rag.count():,}")

print("\nCompleteness Distribution:")
display(products_for_rag.groupBy("data_completeness_score").count().orderBy("data_completeness_score"))

print("\nSearchable Text Examples:")
display(products_for_rag.select("product_id", "product_name", "searchable_text").limit(10))

save_silver(products_for_rag, "amazon_products_for_rag")


%sql
SHOW TABLES IN workspace.ecommerce_silver

%sql
SELECT product_id, product_name, manufacturer, price, category, dimensions
FROM workspace.ecommerce_silver.amazon_products
LIMIT 20;

%sql
SELECT COUNT(*) as total_products, COUNT(DISTINCT category) as total_categories,
AVG(price) as avg_price,
MIN(price) as min_price,
MAX(price) as max_price,
COUNT(DISTINCT manufacturer) as total_manufacturers
FROM workspace.ecommerce_silver.amazon_products


%sql
SELECT 
product_name,
manufacturer,
price,
category,
dimensions
FROM workspace.ecommerce_silver.amazon_products
WHERE LOWER(product_name) LIKE '%iphone%'
ORDER BY price DESC
