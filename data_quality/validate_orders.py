import psycopg


DB_CONFIG = {
    "dbname": "retail_analytics",
    "user": "postgres",
    "password": "PostgreSQL",
    "host": "localhost",
    "port": 5432,
}


def validate_orders():

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            print("=" * 50)
            print("ORDER DATA QUALITY REPORT")
            print("=" * 50)

            # Total records
            cur.execute("SELECT COUNT(*) FROM orders;")
            total_records = cur.fetchone()[0]

            print(f"Total records checked: {total_records:,}")

            # Duplicate order IDs
            cur.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT order_id
                    FROM orders
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                ) duplicates;
            """)

            duplicate_ids = cur.fetchone()[0]

            print(f"Duplicate order IDs: {duplicate_ids:,}")

            # Missing required fields
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE customer_id IS NULL
                   OR product_id IS NULL
                   OR location_id IS NULL
                   OR order_date IS NULL
                   OR quantity IS NULL
                   OR unit_price IS NULL
                   OR payment_method IS NULL
                   OR order_status IS NULL;
            """)

            missing_fields = cur.fetchone()[0]

            print(f"Records with missing fields: {missing_fields:,}")

            # Invalid quantity
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE quantity <= 0;
            """)

            invalid_quantity = cur.fetchone()[0]

            print(f"Invalid quantities: {invalid_quantity:,}")

            # Invalid unit price
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE unit_price <= 0;
            """)

            invalid_price = cur.fetchone()[0]

            print(f"Invalid unit prices: {invalid_price:,}")

            # Invalid discount
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE discount < 0
                   OR discount > 100;
            """)

            invalid_discount = cur.fetchone()[0]

            print(f"Invalid discounts: {invalid_discount:,}")

            # Invalid customer IDs
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE customer_id !~ '^C[0-9]{6}$';
            """)

            invalid_customer_ids = cur.fetchone()[0]

            print(f"Invalid customer IDs: {invalid_customer_ids:,}")

            # Invalid product IDs
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE product_id !~ '^P[0-9]{6}$';
            """)

            invalid_product_ids = cur.fetchone()[0]

            print(f"Invalid product IDs: {invalid_product_ids:,}")

            # Invalid location IDs
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE location_id !~ '^L[0-9]{4}$';
            """)

            invalid_location_ids = cur.fetchone()[0]

            print(f"Invalid location IDs: {invalid_location_ids:,}")

            # Invalid order status
            cur.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE order_status NOT IN (
                    'Completed',
                    'Shipped',
                    'Processing',
                    'Cancelled'
                );
            """)

            invalid_status = cur.fetchone()[0]

            print(f"Invalid order statuses: {invalid_status:,}")

            print("=" * 50)

            total_issues = (
                duplicate_ids
                + missing_fields
                + invalid_quantity
                + invalid_price
                + invalid_discount
                + invalid_customer_ids
                + invalid_product_ids
                + invalid_location_ids
                + invalid_status
            )

            print(f"Total issues found: {total_issues:,}")

            if total_issues == 0:
                print("Order data passed all validation checks.")
            else:
                print("Order data contains quality issues.")

            print("=" * 50)


if __name__ == "__main__":
    validate_orders()