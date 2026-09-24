# Databricks notebook: 03_gold_aggregation
#
# Purpose: Build business-facing, aggregate Gold tables in a simple star
# schema — a fact table (fct_sales) plus conformed dimensions — ready to be
# exposed through Synapse serverless SQL and consumed directly by Power BI.

from pyspark.sql import functions as F

SILVER_PATH = "abfss://silver@retaildataplatformadls.dfs.core.windows.net"
GOLD_PATH = "abfss://gold@retaildataplatformadls.dfs.core.windows.net"


def build_dim_customer():
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
    dim = customers.withColumnRenamed("customer_id", "customer_key")
    dim.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_customer")
    print(f"[gold] dim_customer: {dim.count()} rows")


def build_dim_product():
    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
    dim = products.withColumnRenamed("product_id", "product_key")
    dim.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_product")
    print(f"[gold] dim_product: {dim.count()} rows")


def build_dim_date():
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    dim = (
        orders.select(F.col("order_date").alias("date_key"))
        .distinct()
        .withColumn("year", F.year("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.col("day_of_week").isin("Saturday", "Sunday"))
    )
    dim.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_date")
    print(f"[gold] dim_date: {dim.count()} rows")


def build_fct_sales():
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    fct = orders.select(
        "order_id",
        F.col("customer_id").alias("customer_key"),
        F.col("product_id").alias("product_key"),
        F.col("order_date").alias("date_key"),
        "quantity",
        "unit_price",
        "net_amount",
        "ship_province",
        "order_status",
    )
    fct.write.format("delta").mode("overwrite").partitionBy("ship_province").save(
        f"{GOLD_PATH}/fct_sales"
    )
    print(f"[gold] fct_sales: {fct.count()} rows")


def build_agg_sales_by_category_month():
    """Pre-aggregated table so Power BI/Synapse can serve the primary dashboard
    query without scanning the full fact table on every refresh."""
    fct = spark.read.format("delta").load(f"{GOLD_PATH}/fct_sales")
    dim_product = spark.read.format("delta").load(f"{GOLD_PATH}/dim_product")

    agg = (
        fct.join(dim_product, fct.product_key == dim_product.product_key)
        .withColumn("order_month", F.date_trunc("month", "date_key"))
        .groupBy("order_month", "category", "ship_province")
        .agg(
            F.sum("net_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )
    agg.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/agg_sales_by_category_month")
    print(f"[gold] agg_sales_by_category_month: {agg.count()} rows")


if __name__ == "__main__":
    build_dim_customer()
    build_dim_product()
    build_dim_date()
    build_fct_sales()
    build_agg_sales_by_category_month()
