import csv
import random
from pathlib import Path

from faker import Faker

fake = Faker()

NUM_PRODUCTS = 10_000

OUTPUT_FILE = Path("data/products.csv")

CATEGORIES = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Monitor"],
    "Home": ["Furniture", "Kitchen", "Decor", "Lighting", "Appliances"],
    "Clothing": ["Shirts", "Trousers", "Shoes", "Jackets", "Accessories"],
    "Sports": ["Fitness", "Running", "Football", "Cricket", "Outdoor"],
    "Books": ["Fiction", "Technology", "Business", "Education", "Comics"],
}

BRANDS = [
    "NovaTech",
    "UrbanEdge",
    "PrimeGoods",
    "Vertex",
    "EverBuy",
    "GlobalMart",
    "NextGen",
    "Apex",
]

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

products = []

for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(list(CATEGORIES.keys()))
    subcategory = random.choice(CATEGORIES[category])

    cost_price = round(random.uniform(10, 1000), 2)
    markup = random.uniform(1.15, 1.80)
    unit_price = round(cost_price * markup, 2)

    products.append({
        "product_id": f"P{i:06d}",
        "product_name": f"{fake.word().title()} {subcategory}",
        "category": category,
        "subcategory": subcategory,
        "brand": random.choice(BRANDS),
        "unit_price": unit_price,
        "cost_price": cost_price,
        "supplier_id": f"S{random.randint(1, 500):04d}",
    })

with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=[
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
            "cost_price",
            "supplier_id",
        ],
    )

    writer.writeheader()
    writer.writerows(products)

print(f"Generated {len(products):,} products")
print(f"Output file: {OUTPUT_FILE}")