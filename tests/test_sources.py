import csv

import psycopg
import requests


# --------------------------------------------------
# Configuration
# --------------------------------------------------

DB_CONFIG = {
    "dbname": "retail_analytics",
    "user": "postgres",
    "password": "PostgreSQL",
    "host": "localhost",
    "port": 5432,
}

API_BASE_URL = "http://127.0.0.1:8000"

PRODUCT_FILE = "data/products.csv"


# --------------------------------------------------
# Test Products CSV
# --------------------------------------------------

def test_products():

    print("=" * 50)
    print("TESTING PRODUCTS CSV")
    print("=" * 50)

    with open(
        PRODUCT_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        rows = []

        for row in reader:
            rows.append(row)

            if len(rows) == 3:
                break

    print(f"Successfully read {len(rows)} product records")

    for product in rows:
        print(
            product["product_id"],
            product["product_name"],
            product["category"]
        )

    print("Products CSV: PASS")


# --------------------------------------------------
# Test PostgreSQL
# --------------------------------------------------

def test_postgresql():

    print()
    print("=" * 50)
    print("TESTING POSTGRESQL")
    print("=" * 50)

    with psycopg.connect(**DB_CONFIG) as conn:

        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    order_id,
                    customer_id,
                    product_id,
                    quantity
                FROM orders
                LIMIT 3;
            """)

            rows = cur.fetchall()

    print(f"Successfully read {len(rows)} order records")

    for row in rows:
        print(row)

    print("PostgreSQL: PASS")


# --------------------------------------------------
# Test Customer API
# --------------------------------------------------

def test_customer_api():

    print()
    print("=" * 50)
    print("TESTING CUSTOMER API")
    print("=" * 50)

    response = requests.get(
        f"{API_BASE_URL}/customers/C000001",
        timeout=10
    )

    response.raise_for_status()

    customer = response.json()

    print(
        customer["customer_id"],
        customer["first_name"],
        customer["last_name"]
    )

    print("Customer API: PASS")


# --------------------------------------------------
# Test Location API
# --------------------------------------------------

def test_location_api():

    print()
    print("=" * 50)
    print("TESTING LOCATION API")
    print("=" * 50)

    response = requests.get(
        f"{API_BASE_URL}/locations/L0001",
        timeout=10
    )

    response.raise_for_status()

    location = response.json()

    print(
        location["location_id"],
        location["city"],
        location["country"]
    )

    print("Location API: PASS")


# --------------------------------------------------
# Run All Tests
# --------------------------------------------------

if __name__ == "__main__":

    test_products()
    test_postgresql()
    test_customer_api()
    test_location_api()

    print()
    print("=" * 50)
    print("ALL SOURCE TESTS PASSED")
    print("=" * 50)