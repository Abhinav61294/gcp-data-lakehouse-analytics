import json
import requests


API_URL = "http://127.0.0.1:8000/customers"
OUTPUT_FILE = "data/customers.json"


def export_customers():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    customers = response.json()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(customers, file, indent=2)

    print(f"Exported {len(customers):,} customers to {OUTPUT_FILE}")


if __name__ == "__main__":
    export_customers()