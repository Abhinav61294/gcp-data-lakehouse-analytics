import random

from fastapi import FastAPI, HTTPException
from faker import Faker


app = FastAPI(title="Retail Customer and Location API")

fake = Faker()

CUSTOMER_COUNT = 20_000
LOCATION_COUNT = 500

SEGMENTS = [
    "Standard",
    "Premium",
    "Enterprise",
]

COUNTRIES = [
    "India",
    "USA",
    "UK",
    "Germany",
    "Singapore",
    "Australia",
]

REGIONS = [
    "North",
    "South",
    "East",
    "West",
    "Central",
]


# --------------------------------------------------
# Generate Customers
# --------------------------------------------------

customers = []

for i in range(1, CUSTOMER_COUNT + 1):

    signup_date = fake.date_between(
        start_date="-2y",
        end_date="today"
    )

    customers.append({
        "customer_id": f"C{i:06d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.unique.email(),
        "phone": fake.phone_number(),
        "city": fake.city(),
        "country": random.choice(COUNTRIES),
        "signup_date": signup_date.isoformat(),
        "customer_segment": random.choice(SEGMENTS),
    })


# --------------------------------------------------
# Generate Locations
# --------------------------------------------------

locations = []

for i in range(1, LOCATION_COUNT + 1):

    locations.append({
        "location_id": f"L{i:04d}",
        "city": fake.city(),
        "state": fake.state(),
        "country": random.choice(COUNTRIES),
        "region": random.choice(REGIONS),
        "latitude": round(random.uniform(-35, 35), 6),
        "longitude": round(random.uniform(-120, 150), 6),
    })


# --------------------------------------------------
# Root Endpoint
# --------------------------------------------------

@app.get("/")
def root():

    return {
        "message": "Retail Customer and Location API",
        "customers": CUSTOMER_COUNT,
        "locations": LOCATION_COUNT,
    }


# --------------------------------------------------
# Customer Endpoints
# --------------------------------------------------

@app.get("/customers")
def get_customers():

    return customers


@app.get("/customers/{customer_id}")
def get_customer(customer_id: str):

    for customer in customers:

        if customer["customer_id"] == customer_id:
            return customer

    raise HTTPException(
        status_code=404,
        detail="Customer not found",
    )


# --------------------------------------------------
# Location Endpoints
# --------------------------------------------------

@app.get("/locations")
def get_locations():

    return locations


@app.get("/locations/{location_id}")
def get_location(location_id: str):

    for location in locations:

        if location["location_id"] == location_id:
            return location

    raise HTTPException(
        status_code=404,
        detail="Location not found",
    )
