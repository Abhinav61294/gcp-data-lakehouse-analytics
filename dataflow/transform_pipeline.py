import json

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


# ============================================================
# GCP CONFIGURATION
# ============================================================

PROJECT_ID = "gcp-data-lakehouse-analytics"
REGION = "asia-south1"

DATASET_ID = "staging"

RAW_BUCKET = "gs://gcp-data-lakehouse-analytics-raw"

LOCATIONS_INPUT = f"{RAW_BUCKET}/locations/locations.json"

# Fully qualified BigQuery table reference:
# PROJECT:DATASET.TABLE
LOCATION_TABLE = f"{PROJECT_ID}:{DATASET_ID}.locations"

TEMP_LOCATION = f"{RAW_BUCKET}/dataflow-temp/"


# ============================================================
# BIGQUERY SCHEMA
# ============================================================

LOCATION_SCHEMA = {
    "fields": [
        {
            "name": "location_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "city",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "state",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "country",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "region",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "latitude",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "longitude",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
    ]
}


# ============================================================
# JSON PARSING
# ============================================================

class ParseLocationJson(beam.DoFn):

    def process(self, text):
        try:
            records = json.loads(text)

            if not isinstance(records, list):
                raise ValueError("Expected JSON array")

            for record in records:
                yield record

        except Exception as e:
            print(f"JSON parsing error: {e}")


# ============================================================
# DATA CLEANING AND VALIDATION
# ============================================================

class CleanLocation(beam.DoFn):

    def process(self, record):

        try:
            location_id = str(
                record.get("location_id", "")
            ).strip()

            city = str(
                record.get("city", "")
            ).strip()

            state = str(
                record.get("state", "")
            ).strip()

            country = str(
                record.get("country", "")
            ).strip()

            region = str(
                record.get("region", "")
            ).strip()

            latitude = float(
                record.get("latitude")
            )

            longitude = float(
                record.get("longitude")
            )

            # Required field validation
            if not location_id:
                return

            yield {
                "location_id": location_id,
                "city": city,
                "state": state,
                "country": country,
                "region": region,
                "latitude": latitude,
                "longitude": longitude,
            }

        except (TypeError, ValueError) as e:

            print(
                f"Invalid location record: "
                f"{record} | Error: {e}"
            )


# ============================================================
# PIPELINE
# ============================================================

def run():

    pipeline_options = PipelineOptions(
        save_main_session=True
    )

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        (
            pipeline

            # ------------------------------------------------
            # Read raw JSON from Cloud Storage
            # ------------------------------------------------
            | "Read Locations JSON"
            >> ReadFromText(
                LOCATIONS_INPUT
            )

            # ------------------------------------------------
            # Source JSON is a single JSON array.
            # Combine all lines before parsing.
            # ------------------------------------------------
            | "Combine JSON Lines"
            >> beam.CombineGlobally(
                lambda lines: "".join(lines)
            )

            # ------------------------------------------------
            # Parse JSON array into individual records
            # ------------------------------------------------
            | "Parse JSON Array"
            >> beam.ParDo(
                ParseLocationJson()
            )

            # ------------------------------------------------
            # Clean and validate records
            # ------------------------------------------------
            | "Clean and Validate Locations"
            >> beam.ParDo(
                CleanLocation()
            )

            # ------------------------------------------------
            # Load into BigQuery staging table
            # ------------------------------------------------
            | "Write Locations to BigQuery"
            >> WriteToBigQuery(
                table=LOCATION_TABLE,
                schema=LOCATION_SCHEMA,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_TRUNCATE
                ),
                custom_gcs_temp_location=TEMP_LOCATION,
            )
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run()