from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdataspark"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark = (
    SparkSession.builder
    .appName("BigDataSpark ETL to Star")
    .getOrCreate()
)

raw = spark.read.jdbc(
    url=POSTGRES_URL,
    table="mock_data",
    properties=POSTGRES_PROPS
)

def add_surrogate_key(df, key_name):
    return df.withColumn(
        key_name,
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

# --- DIMENSIONS ---

customers = add_surrogate_key(
    raw.select(
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
        "customer_postal_code"
    ).dropDuplicates(["customer_email"]),
    "customer_id"
)

products = add_surrogate_key(
    raw.select(
        "product_name",
        "product_category",
        "product_price",
        "product_rating",
        "product_reviews"
    ).dropDuplicates(["product_name", "product_category", "product_price"]),
    "product_id"
)

stores = add_surrogate_key(
    raw.select(
        "store_name",
        "store_city",
        "store_country"
    ).dropDuplicates(["store_name", "store_city", "store_country"]),
    "store_id"
)

suppliers = add_surrogate_key(
    raw.select(
        "supplier_name",
        "supplier_country"
    ).dropDuplicates(["supplier_name", "supplier_country"]),
    "supplier_id"
)

sellers = add_surrogate_key(
    raw.select(
        "seller_first_name",
        "seller_last_name",
        "seller_email"
    ).dropDuplicates(["seller_email"]),
    "seller_id"
)

# --- FACT TABLE ---

facts = (
    raw
    .join(customers, ["customer_email"], "left")
    .join(products, ["product_name", "product_category", "product_price"], "left")
    .join(stores, ["store_name", "store_city", "store_country"], "left")
    .join(suppliers, ["supplier_name", "supplier_country"], "left")
    .join(sellers, ["seller_email"], "left")
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

# --- WRITE TABLES ---

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