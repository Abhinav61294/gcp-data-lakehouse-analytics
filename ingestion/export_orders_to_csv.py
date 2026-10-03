import csv
import psycopg


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "retail_analytics",
    "user": "postgres",
    "password": "PostgreSQL"
}

OUTPUT_FILE = "data/orders.csv"


def export_orders():
    query = """
        SELECT
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
        FROM orders
        ORDER BY order_id;
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    columns = [
        "order_id",
        "customer_id",
        "product_id",
        "location_id",
        "order_date",
        "quantity",
        "unit_price",
        "discount",
        "payment_method",
        "order_status"
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"Exported {len(rows):,} orders to {OUTPUT_FILE}")


if __name__ == "__main__":
    export_orders()