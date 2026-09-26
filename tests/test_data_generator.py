"""Unit tests for synthetic e-commerce data generation."""

from datetime import datetime

from scripts.data_generator import (
    DEVICES,
    ORDER_STATUSES,
    PRODUCT_CATALOG,
    TIERS,
    generate_clickstream,
    generate_customers,
    generate_orders,
)


def test_generate_customers():
    """Verify customer generator schema and field integrity."""
    now = datetime(2026, 3, 20, 12, 0, 0)
    customers = generate_customers(num_customers=25, reference_date=now)

    assert len(customers) == 25
    for cust in customers:
        assert cust["customer_id"].startswith("CUST-")
        assert "@" in cust["email"]
        assert cust["tier"] in TIERS
        assert cust["city"] is not None
        assert cust["updated_at"] == now.isoformat()


def test_generate_orders():
    """Verify order calculations and constraints."""
    target_date = datetime(2026, 3, 20)
    customers = generate_customers(num_customers=10, reference_date=target_date)
    orders = generate_orders(customers=customers, target_date=target_date, num_orders=50)

    assert len(orders) == 50
    product_ids = {p["product_id"] for p in PRODUCT_CATALOG}

    for order in orders:
        assert order["order_id"].startswith("ORD-")
        assert order["product_id"] in product_ids
        assert order["quantity"] >= 1
        assert order["unit_price"] > 0
        # Business logic validation: total_amount must equal quantity * unit_price
        expected_total = round(order["quantity"] * order["unit_price"], 2)
        assert abs(order["total_amount"] - expected_total) < 0.01
        assert order["status"] in ORDER_STATUSES
        assert order["order_timestamp"].startswith("2026-03-20")


def test_generate_clickstream():
    """Verify clickstream event generator."""
    target_date = datetime(2026, 3, 20)
    customers = generate_customers(num_customers=5, reference_date=target_date)
    events = generate_clickstream(customers=customers, target_date=target_date, num_events=100)

    assert len(events) == 100
    for evt in events:
        assert evt["event_id"].startswith("EVT-")
        assert evt["session_id"].startswith("SESS-")
        assert evt["device"] in DEVICES
        assert evt["url"].startswith("/")
