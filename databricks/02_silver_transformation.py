# Databricks notebook: 02_silver_transformation
#
# Purpose: Clean, deduplicate, type-enforce and conform Bronze data into
# Silver Delta tables. This is where business rules that don't belong in
# either "raw copy" (Bronze) or "aggregated for reporting" (Gold) live —
# e.g. dropping cancelled orders from the fact grain, standardizing
# province codes, deduping on natural keys.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "abfss://bronze@retaildataplatformadls.dfs.core.windows.net"
SILVER_PATH = "abfss://silver@retaildataplatformadls.dfs.core.windows.net"


def load_latest_bronze(table: str):
    """Bronze is append-only across ingestion runs; take the latest _ingested_at
    per natural key so re-runs and late-arriving corrections don't duplicate rows."""
    return spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")


def dedupe_latest(df, key_cols, order_col="_ingested_at"):
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )


def build_silver_orders():
    orders = load_latest_bronze("orders")
    orders = dedupe_latest(orders, ["order_id"])

    orders_clean = (
        orders.withColumn("order_date", F.to_date("order_date"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .withColumn("net_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("ship_province", F.upper(F.trim("ship_province")))
        .filter(F.col("order_status") != "Cancelled")  # exclude non-fulfilled from fact grain
        .select(
            "order_id", "customer_id", "product_id", "order_date",
            "quantity", "unit_price", "net_amount",
            "ship_city", "ship_province", "order_status",
        )
    )

    orders_clean.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(f"{SILVER_PATH}/orders")
    print(f"[silver] orders: {orders_clean.count()} rows (post-dedupe, post-cancel-filter)")


def build_silver_customers():
    customers = load_latest_bronze("customers")
    customers = dedupe_latest(customers, ["customer_id"])

    customers_clean = (
        customers.withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("province", F.upper(F.trim("province")))
        .select("customer_id", "customer_name", "segment", "province", "signup_date", "email_domain")
    )

    customers_clean.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(f"{SILVER_PATH}/customers")
    print(f"[silver] customers: {customers_clean.count()} rows")


def build_silver_products():
    products = load_latest_bronze("products")
    products = dedupe_latest(products, ["product_id"])

    products_clean = (
        products.withColumn("cost", F.col("cost").cast("decimal(10,2)"))
        .withColumn("list_price", F.col("list_price").cast("decimal(10,2)"))
        .withColumn("margin_pct", F.round((F.col("list_price") - F.col("cost")) / F.col("list_price") * 100, 1))
        .select("product_id", "product_name", "category", "cost", "list_price", "margin_pct", "supplier")
    )

    products_clean.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(f"{SILVER_PATH}/products")
    print(f"[silver] products: {products_clean.count()} rows")


if __name__ == "__main__":
    build_silver_orders()
    build_silver_customers()
    build_silver_products()
