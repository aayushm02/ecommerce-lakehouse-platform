"""PySpark Bronze to Silver Batch Processing Job.

Demonstrates:
- Schema enforcement & casting
- Deduplication and data hygiene
- Broadcast joins for small dimension tables (Performance optimization)
- Writing partitioned Snappy-compressed Parquet to Lakehouse Silver Layer
- Loading cleaned datasets into PostgreSQL analytical warehouse staging tables
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    current_timestamp,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def create_spark_session() -> SparkSession:
    """Initialize SparkSession with S3A and Postgres connector configs."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    builder = (
        SparkSession.builder.appName("ECommerceBronzeToSilver")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    # Master configuration fallback
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    builder = builder.master(spark_master)

    return builder.getOrCreate()


def get_paths(partition_date: str, base_type: str = "minio") -> dict[str, str]:
    """Get source and destination paths based on deployment target."""
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    partition = f"year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}"

    if base_type == "local":
        return {
            "products_raw": "data/raw/products/products.csv",
            "customers_raw": f"data/raw/customers/{partition}/customers.csv",
            "orders_raw": f"data/raw/orders/{partition}/orders.csv",
            "clickstream_raw": f"data/raw/clickstream/{partition}/clickstream.json",
            "orders_silver": "data/silver/orders",
            "clickstream_silver": "data/silver/clickstream",
            "customers_silver": "data/silver/customers",
        }
    return {
        "products_raw": "s3a://raw/products/products.csv",
        "customers_raw": f"s3a://raw/customers/{partition}/customers.csv",
        "orders_raw": f"s3a://raw/orders/{partition}/orders.csv",
        "clickstream_raw": f"s3a://raw/clickstream/{partition}/clickstream.json",
        "orders_silver": "s3a://silver/orders",
        "clickstream_silver": "s3a://silver/clickstream",
        "customers_silver": "s3a://silver/customers",
    }


def process_orders_and_products(spark: SparkSession, paths: dict[str, str], partition_date: str):
    """Process orders: validate schemas, broadcast join with products, write Parquet."""
    print(">>> Processing Orders and Products...")

    # Explicit schemas for deterministic types
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("order_timestamp", StringType(), True),
    ])

    products_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("base_price", DoubleType(), True),
    ])

    # 1. Read Raw Datasets
    df_orders_raw = spark.read.option("header", "true").schema(orders_schema).csv(paths["orders_raw"])
    df_products_raw = spark.read.option("header", "true").schema(products_schema).csv(paths["products_raw"])

    # 2. Cleansing and Deduplication
    df_orders_clean = (
        df_orders_raw.dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull() & (col("total_amount") > 0))
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("ingested_at", current_timestamp())
    )

    # 3. Broadcast Join optimization (Joining high-volume orders with low-volume product catalog)
    df_orders_enriched = (
        df_orders_clean.join(broadcast(df_products_raw), on="product_id", how="left")
        .withColumnRenamed("name", "product_name")
        .withColumnRenamed("category", "product_category")
        .withColumn(
            "is_high_value_order",
            when(col("total_amount") >= 100.0, True).otherwise(False),
        )
    )

    # 4. Write to Silver Lakehouse Layer in Snappy-compressed Parquet partitioned by order_date
    (
        df_orders_enriched.write.mode("append")
        .partitionBy("order_date")
        .parquet(paths["orders_silver"])
    )
    print(f"[SUCCESS] Orders successfully processed and written to {paths['orders_silver']}")

    return df_orders_enriched


def process_clickstream(spark: SparkSession, paths: dict[str, str], partition_date: str):
    """Process clickstream JSON events, cleanse, and store in Silver Parquet."""
    print(">>> Processing Clickstream Events...")

    clickstream_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("device", StringType(), True),
        StructField("referral_source", StringType(), True),
        StructField("event_timestamp", StringType(), True),
    ])

    df_clickstream_raw = spark.read.schema(clickstream_schema).json(paths["clickstream_raw"])

    df_clickstream_clean = (
        df_clickstream_raw.dropDuplicates(["event_id"])
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn(
            "page_type",
            when(col("url").contains("checkout"), "conversion")
            .when(col("url").contains("cart"), "cart")
            .when(col("url").contains("product"), "product_view")
            .otherwise("browse"),
        )
        .withColumn("ingested_at", current_timestamp())
    )

    (
        df_clickstream_clean.write.mode("append")
        .partitionBy("event_date")
        .parquet(paths["clickstream_silver"])
    )
    print(f"[SUCCESS] Clickstream successfully processed and written to {paths['clickstream_silver']}")

    return df_clickstream_clean


def process_customers(spark: SparkSession, paths: dict[str, str]):
    """Process customer records."""
    print(">>> Processing Customers...")
    df_customers = (
        spark.read.option("header", "true")
        .csv(paths["customers_raw"])
        .dropDuplicates(["customer_id"])
        .withColumn("updated_at", to_timestamp(col("updated_at")))
    )

    df_customers.write.mode("overwrite").parquet(paths["customers_silver"])
    print(f"[SUCCESS] Customers successfully written to {paths['customers_silver']}")
    return df_customers


def write_to_postgres_warehouse(df, table_name: str):
    """Load cleaned DataFrame into Postgres Analytical Warehouse staging schema."""
    pg_host = os.getenv("WAREHOUSE_HOST", "postgres")
    pg_port = os.getenv("WAREHOUSE_PORT", "5432")
    pg_db = os.getenv("WAREHOUSE_DB", "warehouse")
    pg_user = os.getenv("WAREHOUSE_USER", "postgres")
    pg_pass = os.getenv("WAREHOUSE_PASSWORD", "postgres")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    try:
        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"raw_stage.{table_name}")
            .option("user", pg_user)
            .option("password", pg_pass)
            .mode("append")
            .save()
        )
        print(f"[SUCCESS] Loaded {table_name} into Postgres staging (raw_stage.{table_name})")
    except Exception as exc:
        print(f"[WARN] JDBC Postgres load skipped or failed: {exc}")


def main():
    parser = argparse.ArgumentParser(description="PySpark Bronze to Silver Pipeline")
    parser.add_argument("--date", type=str, required=True, help="Processing partition date YYYY-MM-DD")
    parser.add_argument("--storage-type", choices=["minio", "local"], default="minio", help="Source/sink storage")
    args = parser.parse_args()

    spark = create_spark_session()
    paths = get_paths(args.date, args.storage_type)

    try:
        df_orders = process_orders_and_products(spark, paths, args.date)
        df_clickstream = process_clickstream(spark, paths, args.date)
        df_customers = process_customers(spark, paths)

        # Sync to warehouse staging for dbt modeling
        write_to_postgres_warehouse(df_orders, "stg_raw_orders")
        write_to_postgres_warehouse(df_clickstream, "stg_raw_clickstream")
        write_to_postgres_warehouse(df_customers, "stg_raw_customers")

    except Exception as e:
        print(f"[ERROR] Spark pipeline failed: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("=== PySpark Bronze to Silver Completed Successfully ===")


if __name__ == "__main__":
    main()
