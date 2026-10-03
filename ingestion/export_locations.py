import json
import requests


API_URL = "http://127.0.0.1:8000/locations"
OUTPUT_FILE = "data/locations.json"


def export_locations():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    locations = response.json()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(locations, file, indent=2)

    print(f"Exported {len(locations):,} locations to {OUTPUT_FILE}")


if __name__ == "__main__":
    export_locations()