import csv
from pathlib import Path


INPUT_FILE = Path("data/products.csv")


def validate_products():
    errors = []

    seen_ids = set()

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row_number, row in enumerate(reader, start=2):

            product_id = row["product_id"]

            # Check duplicate product IDs
            if product_id in seen_ids:
                errors.append(
                    f"Row {row_number}: Duplicate product_id {product_id}"
                )

            seen_ids.add(product_id)

            # Check required fields
            required_fields = [
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "unit_price",
                "cost_price",
                "supplier_id",
            ]

            for field in required_fields:
                if not row[field]:
                    errors.append(
                        f"Row {row_number}: Missing {field}"
                    )

            # Validate prices
            try:
                unit_price = float(row["unit_price"])
                cost_price = float(row["cost_price"])

                if unit_price <= 0:
                    errors.append(
                        f"Row {row_number}: Invalid unit_price"
                    )

                if cost_price <= 0:
                    errors.append(
                        f"Row {row_number}: Invalid cost_price"
                    )

                if cost_price >= unit_price:
                    errors.append(
                        f"Row {row_number}: cost_price >= unit_price"
                    )

            except ValueError:
                errors.append(
                    f"Row {row_number}: Invalid price format"
                )

    print("=" * 50)
    print("PRODUCT DATA QUALITY REPORT")
    print("=" * 50)

    print(f"Total records checked: {len(seen_ids):,}")
    print(f"Total issues found: {len(errors):,}")

    if errors:
        print("\nIssues:")
        for error in errors[:20]:
            print(error)

        if len(errors) > 20:
            print(
                f"\n... and {len(errors) - 20:,} more issues."
            )

    else:
        print("No data quality issues found.")

    print("=" * 50)


if __name__ == "__main__":
    validate_products()