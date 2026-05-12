from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, year, month, corr, desc
)

POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdataspark"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/reports"
CLICKHOUSE_PROPS = {
    "user": "default",
    "password": "postgres",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

spark = (
    SparkSession.builder
    .appName("BigDataSpark Reports")
    .getOrCreate()
)

# --- LOAD TABLES ---

sales = spark.read.jdbc(POSTGRES_URL, "fact_sales", properties=POSTGRES_PROPS)
products = spark.read.jdbc(POSTGRES_URL, "dim_products", properties=POSTGRES_PROPS)
customers = spark.read.jdbc(POSTGRES_URL, "dim_customers", properties=POSTGRES_PROPS)
stores = spark.read.jdbc(POSTGRES_URL, "dim_stores", properties=POSTGRES_PROPS)
suppliers = spark.read.jdbc(POSTGRES_URL, "dim_suppliers", properties=POSTGRES_PROPS)

base = (
    sales
    .join(products, "product_id")
    .join(customers, "customer_id")
    .join(stores, "store_id")
    .join(suppliers, "supplier_id")
)

# --- REPORT 1 ---
report_product_sales = (
    base.groupBy("product_name", "product_category")
    .agg(
        spark_sum("sale_quantity").alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_revenue"),
        avg("product_rating").alias("avg_rating"),
        avg("product_reviews").alias("avg_reviews")
    )
)

# --- REPORT 2 ---
report_customer_sales = (
    base.groupBy("customer_email", "customer_country")
    .agg(
        spark_sum("sale_total_price").alias("total_spent"),
        count("*").alias("orders"),
        avg("sale_total_price").alias("avg_order")
    )
)

# --- REPORT 3 ---
report_time_sales = (
    base.withColumn("year", year("sale_date"))
    .withColumn("month", month("sale_date"))
    .groupBy("year", "month")
    .agg(
        spark_sum("sale_total_price").alias("revenue"),
        count("*").alias("orders")
    )
)

# --- REPORT 4 ---
report_store_sales = (
    base.groupBy("store_name", "store_country")
    .agg(
        spark_sum("sale_total_price").alias("revenue"),
        count("*").alias("orders")
    )
)

# --- REPORT 5 ---
report_supplier_sales = (
    base.groupBy("supplier_name")
    .agg(
        spark_sum("sale_total_price").alias("revenue"),
        count("*").alias("sales")
    )
)

# --- REPORT 6 ---
report_product_quality = (
    base.groupBy("product_name")
    .agg(
        avg("product_rating").alias("rating"),
        spark_sum("sale_quantity").alias("sales")
    )
)

# --- WRITE TO CLICKHOUSE ---

reports = {
    "report_product_sales": report_product_sales,
    "report_customer_sales": report_customer_sales,
    "report_time_sales": report_time_sales,
    "report_store_sales": report_store_sales,
    "report_supplier_sales": report_supplier_sales,
    "report_product_quality": report_product_quality
}

for name, df in reports.items():
    (
        df.write
        .mode("overwrite")
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .jdbc(
            url=CLICKHOUSE_URL,
            table=name,
            properties=CLICKHOUSE_PROPS
        )
    )
    print(f"{name} -> {df.count()} rows")