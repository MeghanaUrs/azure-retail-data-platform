# Databricks notebook: 01_bronze_ingestion
#
# Purpose: Land source extracts as-is into the Bronze Delta zone. No cleansing,
# no type enforcement beyond what the source encodes — Bronze is a faithful,
# append-only copy of what ADF delivered, so any downstream bug can always be
# replayed from source of truth without re-pulling from the source systems.
#
# Triggered by: ADF pipeline PL_Ingest_Bronze_Daily (this notebook mirrors the
# Copy Activities in that pipeline's ADLS-native path; kept here too so the
# same logic works if Bronze is ever re-materialized directly from landing).

from pyspark.sql import functions as F

LANDING_PATH = "abfss://landing@retaildataplatformadls.dfs.core.windows.net"
BRONZE_PATH = "abfss://bronze@retaildataplatformadls.dfs.core.windows.net"

SOURCES = {
    "orders": f"{LANDING_PATH}/orders",
    "customers": f"{LANDING_PATH}/customers",
    "products": f"{LANDING_PATH}/products",
}


def land_to_bronze(source_name: str, source_path: str) -> None:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"{BRONZE_PATH}/{source_name}")
    )

    print(f"[bronze] {source_name}: {df.count()} rows landed")


if __name__ == "__main__":
    for name, path in SOURCES.items():
        land_to_bronze(name, path)
