import random
from datetime import datetime, timedelta

import psycopg
from faker import Faker

fake = Faker()

NUM_ORDERS = 100_000

DB_CONFIG = {
    "dbname": "retail_analytics",
    "user": "postgres",
    "password": "PostgreSQL",
    "host": "localhost",
    "port": 5432,
}

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Net Banking",
    "Cash",
]

ORDER_STATUSES = [
    "Completed",
    "Completed",
    "Completed",
    "Shipped",
    "Processing",
    "Cancelled",
]

PRODUCT_COUNT = 10_000
CUSTOMER_COUNT = 20_000
LOCATION_COUNT = 500

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 9, 30)


def random_date():
    days = (END_DATE - START_DATE).days
    return START_DATE + timedelta(days=random.randint(0, days))


def generate_orders():
    orders = []

    for i in range(NUM_ORDERS):
        order_id = 1_000_000_001 + i

        customer_id = f"C{random.randint(1, CUSTOMER_COUNT):06d}"
        product_id = f"P{random.randint(1, PRODUCT_COUNT):06d}"
        location_id = f"L{random.randint(1, LOCATION_COUNT):04d}"

        quantity = random.randint(1, 8)

        unit_price = round(random.uniform(10, 1500), 2)

        discount = round(
            random.choice([0, 0, 0, 5, 10, 15, 20, 25]),
            2
        )

        orders.append(
            (
                order_id,
                customer_id,
                product_id,
                location_id,
                random_date(),
                quantity,
                unit_price,
                discount,
                random.choice(PAYMENT_METHODS),
                random.choice(ORDER_STATUSES),
            )
        )

    return orders


def insert_orders(orders):
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            insert_query = """
                INSERT INTO orders (
                    order_id,
                    customer_id,
                    product_id,
                    location_id,
                    order_date,
                    quantity,
                    unit_price,
                    discount,
                    payment_method,
                    order_status
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """

            cur.executemany(insert_query, orders)

        conn.commit()


if __name__ == "__main__":
    print("Generating orders...")

    orders = generate_orders()

    print(f"Generated {len(orders):,} orders")
    print("Loading orders into PostgreSQL...")

    insert_orders(orders)

    print("Orders loaded successfully.")