"""PySpark transformation unit tests.

Tests DataFrame deduplication, schema validation, and enrichment logic.
Automatically skips if pyspark is not installed in the current environment.
"""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark_session():
    """Create lightweight in-memory Spark session for unit testing."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_order_deduplication_and_filtering(spark_session):
    """Verify that duplicate order_ids and non-positive total_amounts are filtered."""
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("total_amount", DoubleType(), True),
    ])

    test_data = [
        ("ORD-001", "CUST-001", 150.0),
        ("ORD-001", "CUST-001", 150.0),  # Duplicate
        ("ORD-002", "CUST-002", -10.0),  # Invalid negative amount
        ("ORD-003", "CUST-003", 0.0),    # Invalid zero amount
        ("ORD-004", "CUST-004", 49.99),  # Valid
    ]

    df = spark_session.createDataFrame(test_data, schema=schema)

    df_clean = (
        df.dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull() & (col("total_amount") > 0))
    )

    results = df_clean.collect()
    assert len(results) == 2
    order_ids = {row["order_id"] for row in results}
    assert order_ids == {"ORD-001", "ORD-004"}


def test_high_value_order_flagging(spark_session):
    """Verify that orders >= 100.0 are flagged as high value."""
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("total_amount", DoubleType(), True),
    ])

    test_data = [
        ("ORD-001", 99.99),
        ("ORD-002", 100.00),
        ("ORD-003", 250.00),
    ]

    df = spark_session.createDataFrame(test_data, schema=schema)
    df_flagged = df.withColumn(
        "is_high_value_order",
        when(col("total_amount") >= 100.0, True).otherwise(False),
    )

    rows = {row["order_id"]: row["is_high_value_order"] for row in df_flagged.collect()}
    assert rows["ORD-001"] is False
    assert rows["ORD-002"] is True
    assert rows["ORD-003"] is True
