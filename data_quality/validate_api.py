import re
import requests


BASE_URL = "http://127.0.0.1:8000"

EXPECTED_CUSTOMERS = 20_000
EXPECTED_LOCATIONS = 500


def validate_customers():
    print("=" * 50)
    print("CUSTOMER API DATA QUALITY REPORT")
    print("=" * 50)

    response = requests.get(
        f"{BASE_URL}/customers",
        timeout=30
    )

    response.raise_for_status()

    customers = response.json()

    print(f"Customers received: {len(customers):,}")

    issues = 0
    customer_ids = set()

    required_fields = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "country",
        "signup_date",
        "customer_segment",
    ]

    for customer in customers:

        customer_id = customer.get("customer_id")

        if customer_id in customer_ids:
            print(f"Duplicate customer ID: {customer_id}")
            issues += 1

        customer_ids.add(customer_id)

        # Required fields
        for field in required_fields:
            if not customer.get(field):
                print(
                    f"Missing {field} for {customer_id}"
                )
                issues += 1

        # Customer ID format
        if not re.match(r"^C\d{6}$", customer_id):
            print(f"Invalid customer ID: {customer_id}")
            issues += 1

        # Email format
        email = customer.get("email", "")

        if not re.match(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            email
        ):
            print(
                f"Invalid email for {customer_id}"
            )
            issues += 1

        # Customer segment
        if customer.get("customer_segment") not in [
            "Standard",
            "Premium",
            "Enterprise",
        ]:
            print(
                f"Invalid segment for {customer_id}"
            )
            issues += 1

    print(f"Expected customers: {EXPECTED_CUSTOMERS:,}")
    print(f"Total issues found: {issues:,}")

    return issues


def validate_locations():
    print()
    print("=" * 50)
    print("LOCATION API DATA QUALITY REPORT")
    print("=" * 50)

    response = requests.get(
        f"{BASE_URL}/locations",
        timeout=30
    )

    response.raise_for_status()

    locations = response.json()

    print(f"Locations received: {len(locations):,}")

    issues = 0
    location_ids = set()

    required_fields = [
        "location_id",
        "city",
        "state",
        "country",
        "region",
        "latitude",
        "longitude",
    ]

    for location in locations:

        location_id = location.get("location_id")

        if location_id in location_ids:
            print(
                f"Duplicate location ID: {location_id}"
            )
            issues += 1

        location_ids.add(location_id)

        # Required fields
        for field in required_fields:
            if location.get(field) is None:
                print(
                    f"Missing {field} for {location_id}"
                )
                issues += 1

        # Location ID format
        if not re.match(r"^L\d{4}$", location_id):
            print(
                f"Invalid location ID: {location_id}"
            )
            issues += 1

        # Latitude validation
        latitude = location.get("latitude")

        if not -90 <= latitude <= 90:
            print(
                f"Invalid latitude for {location_id}"
            )
            issues += 1

        # Longitude validation
        longitude = location.get("longitude")

        if not -180 <= longitude <= 180:
            print(
                f"Invalid longitude for {location_id}"
            )
            issues += 1

        # Region validation
        if location.get("region") not in [
            "North",
            "South",
            "East",
            "West",
            "Central",
        ]:
            print(
                f"Invalid region for {location_id}"
            )
            issues += 1

    print(f"Expected locations: {EXPECTED_LOCATIONS:,}")
    print(f"Total issues found: {issues:,}")

    return issues


if __name__ == "__main__":

    customer_issues = validate_customers()

    location_issues = validate_locations()

    total_issues = customer_issues + location_issues

    print()
    print("=" * 50)
    print("API VALIDATION SUMMARY")
    print("=" * 50)
    print(f"Total API issues: {total_issues:,}")

    if total_issues == 0:
        print("API data passed all validation checks.")
    else:
        print("API data contains quality issues.")

    print("=" * 50)