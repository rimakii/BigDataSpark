from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, rank, desc, year, month, corr
)
from pyspark.sql.window import Window

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
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)


sales = spark.read.jdbc(POSTGRES_URL, "fact_sales", properties=POSTGRES_PROPS)
products = spark.read.jdbc(POSTGRES_URL, "dim_products", properties=POSTGRES_PROPS)
customers = spark.read.jdbc(POSTGRES_URL, "dim_customers", properties=POSTGRES_PROPS)
stores = spark.read.jdbc(POSTGRES_URL, "dim_stores", properties=POSTGRES_PROPS)
suppliers = spark.read.jdbc(POSTGRES_URL, "dim_suppliers", properties=POSTGRES_PROPS)



sp = sales.join(products, "product_id")

report_product_sales = (
    sp.groupBy("product_name", "product_category")
    .agg(
        spark_sum("sale_quantity").alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_revenue"),
        avg("product_rating").alias("avg_rating"),
        avg("product_reviews").alias("avg_reviews")
    )
    .withColumn(
        "category_rank",
        rank().over(
            Window.partitionBy("product_category").orderBy(desc("total_revenue"))
        )
    )
)


sc = sales.join(customers, "customer_id")

report_customer_sales = (
    sc.groupBy("customer_email", "customer_country")
    .agg(
        spark_sum("sale_total_price").alias("total_spent"),
        count("*").alias("orders"),
        avg("sale_total_price").alias("avg_order")
    )
)


report_time_sales = (
    sales
    .withColumn("year", year("sale_date"))
    .withColumn("month", month("sale_date"))
    .groupBy("year", "month")
    .agg(
        spark_sum("sale_total_price").alias("revenue"),
        count("*").alias("orders"),
        avg("sale_total_price").alias("avg_order")
    )
)


ss = sales.join(stores, "store_id")

report_store_sales = (
    ss.groupBy("store_name", "store_city", "store_country")
    .agg(
        spark_sum("sale_total_price").alias("revenue"),
        count("*").alias("orders"),
        avg("sale_total_price").alias("avg_check")
    )
)



ssup = sales.join(suppliers, "supplier_id").join(products, "product_id")

report_supplier_sales = (
    ssup.groupBy("supplier_name", "supplier_country")
    .agg(
        spark_sum("sale_total_price").alias("revenue"),
        count("*").alias("sales"),
        avg("product_price").alias("avg_product_price")
    )
)



report_product_quality = (
    sp.groupBy("product_name", "product_category")
    .agg(
        avg("product_rating").alias("avg_rating"),
        spark_sum("sale_quantity").alias("total_sales"),
        avg("product_reviews").alias("avg_reviews"),
        corr("product_rating", "sale_quantity").alias("rating_sales_corr")
    )
)


reports = {
    "report_product_sales": report_product_sales,
    "report_customer_sales": report_customer_sales,
    "report_time_sales": report_time_sales,
    "report_store_sales": report_store_sales,
    "report_supplier_sales": report_supplier_sales,
    "report_product_quality": report_product_quality,
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
