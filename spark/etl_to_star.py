from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdataspark"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark = (
    SparkSession.builder
    .appName("BigDataSpark ETL to Star")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

raw = spark.read.jdbc(
    url=POSTGRES_URL,
    table="mock_data",
    properties=POSTGRES_PROPS
)


customers = (
    raw.select(
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
        "customer_postal_code"
    ).dropDuplicates(["customer_email"])
    .withColumn("customer_id", monotonically_increasing_id())
)

products = (
    raw.select(
        "product_name",
        "product_category",
        "product_price",
        "product_rating",
        "product_reviews"
    ).dropDuplicates(["product_name", "product_category", "product_price"])
    .withColumn("product_id", monotonically_increasing_id())
)

stores = (
    raw.select(
        "store_name",
        "store_city",
        "store_country"
    ).dropDuplicates(["store_name", "store_city", "store_country"])
    .withColumn("store_id", monotonically_increasing_id())
)

suppliers = (
    raw.select(
        "supplier_name",
        "supplier_country"
    ).dropDuplicates(["supplier_name", "supplier_country"])
    .withColumn("supplier_id", monotonically_increasing_id())
)

sellers = (
    raw.select(
        "seller_first_name",
        "seller_last_name",
        "seller_email"
    ).dropDuplicates(["seller_email"])
    .withColumn("seller_id", monotonically_increasing_id())
)


facts = (
    raw
    .join(customers.select("customer_email", "customer_id"), "customer_email", "left")
    .join(
        products.select("product_name", "product_category", "product_price", "product_id"),
        ["product_name", "product_category", "product_price"],
        "left"
    )
    .join(
        stores.select("store_name", "store_city", "store_country", "store_id"),
        ["store_name", "store_city", "store_country"],
        "left"
    )
    .join(
        suppliers.select("supplier_name", "supplier_country", "supplier_id"),
        ["supplier_name", "supplier_country"],
        "left"
    )
    .join(sellers.select("seller_email", "seller_id"), "seller_email", "left")
    .select(
        col("id").alias("sale_id"),
        "customer_id",
        "product_id",
        "store_id",
        "supplier_id",
        "seller_id",
        "sale_date",
        "sale_quantity",
        "sale_total_price"
    )
)

tables = {
    "dim_customers": customers.select(
        "customer_id", "customer_first_name", "customer_last_name",
        "customer_email", "customer_country", "customer_postal_code"
    ),
    "dim_products": products.select(
        "product_id", "product_name", "product_category",
        "product_price", "product_rating", "product_reviews"
    ),
    "dim_stores": stores.select(
        "store_id", "store_name", "store_city", "store_country"
    ),
    "dim_suppliers": suppliers.select(
        "supplier_id", "supplier_name", "supplier_country"
    ),
    "dim_sellers": sellers.select(
        "seller_id", "seller_first_name", "seller_last_name", "seller_email"
    ),
    "fact_sales": facts
}

for name, df in tables.items():
    (
        df.write
        .mode("overwrite")
        .jdbc(url=POSTGRES_URL, table=name, properties=POSTGRES_PROPS)
    )
    print(f"{name} -> {df.count()} rows")

spark.stop()
