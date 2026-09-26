"""Synthetic E-Commerce Data Generator.

Generates realistic orders, customers, products, and clickstream events,
partitioned by execution date, and uploads them to MinIO (S3-compatible Lakehouse)
or writes to local disk for development/testing.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import Any

# Static reference categories and products
PRODUCT_CATALOG = [
    {"product_id": "PROD-001", "name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "base_price": 149.99},
    {"product_id": "PROD-002", "name": "Ergonomic Mechanical Keyboard", "category": "Electronics", "base_price": 99.50},
    {"product_id": "PROD-003", "name": "Ultra HD 4K Monitor 27-inch", "category": "Electronics", "base_price": 329.00},
    {"product_id": "PROD-004", "name": "Organic Cotton Crew T-Shirt", "category": "Apparel", "base_price": 24.99},
    {"product_id": "PROD-005", "name": "Waterproof Trail Running Shoes", "category": "Sports", "base_price": 119.00},
    {"product_id": "PROD-006", "name": "Stainless Steel Pour-Over Kettle", "category": "Home & Kitchen", "base_price": 45.00},
    {"product_id": "PROD-007", "name": "Smart Fitness & Sleep Tracker", "category": "Electronics", "base_price": 79.95},
    {"product_id": "PROD-008", "name": "Non-Stick Ceramic Skillet 10-inch", "category": "Home & Kitchen", "base_price": 39.99},
    {"product_id": "PROD-009", "name": "Thermal Insulated Travel Mug", "category": "Home & Kitchen", "base_price": 19.50},
    {"product_id": "PROD-010", "name": "High-Density Yoga Mat with Strap", "category": "Sports", "base_price": 34.00},
]

CITIES = ["New York", "San Francisco", "Austin", "Seattle", "Chicago", "Boston", "Denver", "Miami"]
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
ORDER_STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]
DEVICES = ["Mobile", "Desktop", "Tablet"]
REFERRAL_SOURCES = ["Google", "Direct", "Instagram", "Email", "Affiliate"]
PAGES = ["/home", "/category/electronics", "/category/apparel", "/product/detail", "/cart", "/checkout"]


def generate_customers(num_customers: int = 50, reference_date: datetime | None = None) -> list[dict[str, Any]]:
    """Generate customer master records with tier and city attributes."""
    if reference_date is None:
        reference_date = datetime.utcnow()

    customers = []
    for i in range(1, num_customers + 1):
        cust_id = f"CUST-{i:04d}"
        customers.append({
            "customer_id": cust_id,
            "first_name": f"User{i}",
            "last_name": f"Smith{i}",
            "email": f"user{i}@example.com",
            "tier": random.choice(TIERS),
            "city": random.choice(CITIES),
            "updated_at": reference_date.isoformat(),
        })
    return customers


def generate_orders(customers: list[dict[str, Any]], target_date: datetime, num_orders: int = 150) -> list[dict[str, Any]]:
    """Generate orders for the specific partition date."""
    orders = []
    for _ in range(num_orders):
        customer = random.choice(customers)
        product = random.choice(PRODUCT_CATALOG)
        quantity = random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.1, 0.05])[0]
        unit_price = product["base_price"]
        total_amount = round(quantity * unit_price, 2)

        # Distribute orders evenly throughout the 24-hour target date
        random_seconds = random.randint(0, 86399)
        order_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=random_seconds)

        orders.append({
            "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "status": random.choice(ORDER_STATUSES),
            "order_timestamp": order_time.isoformat(),
        })
    return orders


def generate_clickstream(customers: list[dict[str, Any]], target_date: datetime, num_events: int = 500) -> list[dict[str, Any]]:
    """Generate web clickstream events for pageviews and conversions."""
    events = []
    sessions = [f"SESS-{uuid.uuid4().hex[:10]}" for _ in range(num_events // 5)]

    for _ in range(num_events):
        session_id = random.choice(sessions)
        # 80% authenticated users, 20% anonymous visitors
        customer_id = random.choice(customers)["customer_id"] if random.random() < 0.8 else None
        random_seconds = random.randint(0, 86399)
        event_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=random_seconds)

        events.append({
            "event_id": f"EVT-{uuid.uuid4().hex[:12]}",
            "session_id": session_id,
            "customer_id": customer_id,
            "url": random.choice(PAGES),
            "device": random.choice(DEVICES),
            "referral_source": random.choice(REFERRAL_SOURCES),
            "event_timestamp": event_time.isoformat(),
        })
    return events


def get_minio_client() -> Any | None:
    """Connect to MinIO instance using environment credentials."""
    try:
        from minio import Minio
    except ImportError:
        print("[INFO] 'minio' library not installed. Defaulting to local file output.")
        return None

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    try:
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        # Test connection
        client.list_buckets()
        return client
    except Exception as exc:
        print(f"[WARN] MinIO connection failed at {endpoint}: {exc}. Falling back to local storage.")
        return None


def upload_to_minio(client: Any, bucket: str, object_name: str, data_bytes: bytes, content_type: str = "application/octet-stream") -> None:
    """Upload byte buffer directly to S3-compatible MinIO object store."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type=content_type,
    )
    print(f"[SUCCESS] Uploaded {len(data_bytes)} bytes to s3://{bucket}/{object_name}")


def write_local_file(filepath: str, data_bytes: bytes) -> None:
    """Fallback to local filesystem if MinIO is not available."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "wb") as f:
        f.write(data_bytes)
    print(f"[SUCCESS] Saved local file: {filepath}")


def run_generator(target_date_str: str, output_mode: str = "auto") -> None:
    """Generate datasets for partition date and upload/save."""
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    year_str = target_date.strftime("%Y")
    month_str = target_date.strftime("%m")
    day_str = target_date.strftime("%d")
    partition_path = f"year={year_str}/month={month_str}/day={day_str}"

    print(f"--- Generating E-Commerce Data for Partition: {target_date_str} ---")
    customers = generate_customers(num_customers=50, reference_date=target_date)
    orders = generate_orders(customers=customers, target_date=target_date, num_orders=200)
    clickstream = generate_clickstream(customers=customers, target_date=target_date, num_events=600)

    # 1. Products CSV
    prod_buf = io.StringIO()
    prod_writer = csv.DictWriter(prod_buf, fieldnames=["product_id", "name", "category", "base_price"])
    prod_writer.writeheader()
    prod_writer.writerows(PRODUCT_CATALOG)
    prod_bytes = prod_buf.getvalue().encode("utf-8")

    # 2. Customers CSV
    cust_buf = io.StringIO()
    cust_writer = csv.DictWriter(cust_buf, fieldnames=["customer_id", "first_name", "last_name", "email", "tier", "city", "updated_at"])
    cust_writer.writeheader()
    cust_writer.writerows(customers)
    cust_bytes = cust_buf.getvalue().encode("utf-8")

    # 3. Orders CSV
    orders_buf = io.StringIO()
    orders_writer = csv.DictWriter(orders_buf, fieldnames=["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount", "status", "order_timestamp"])
    orders_writer.writeheader()
    orders_writer.writerows(orders)
    orders_bytes = orders_buf.getvalue().encode("utf-8")

    # 4. Clickstream JSON
    clickstream_bytes = "\n".join([json.dumps(event) for event in clickstream]).encode("utf-8")

    minio_client = get_minio_client() if output_mode in ("auto", "minio") else None

    files_to_store = [
        ("raw", "products/products.csv", prod_bytes, "text/csv"),
        ("raw", f"customers/{partition_path}/customers.csv", cust_bytes, "text/csv"),
        ("raw", f"orders/{partition_path}/orders.csv", orders_bytes, "text/csv"),
        ("raw", f"clickstream/{partition_path}/clickstream.json", clickstream_bytes, "application/json"),
    ]

    for bucket, rel_path, data_bytes, ctype in files_to_store:
        if minio_client:
            upload_to_minio(minio_client, bucket, rel_path, data_bytes, ctype)
        else:
            local_target = os.path.join("data", bucket, rel_path)
            write_local_file(local_target, data_bytes)

    print("--- Ingestion Completed Successfully ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic E-Commerce Lakehouse Data Generator")
    parser.add_argument("--date", type=str, default=datetime.utcnow().strftime("%Y-%m-%d"), help="Partition date in YYYY-MM-DD format")
    parser.add_argument("--output", type=str, choices=["auto", "minio", "local"], default="auto", help="Destination storage")
    args = parser.parse_args()

    run_generator(target_date_str=args.date, output_mode=args.output)
