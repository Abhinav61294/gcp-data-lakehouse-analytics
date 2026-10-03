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

CUSTOMERS_INPUT = f"{RAW_BUCKET}/customers/customers.json"

# Fully qualified BigQuery table:
# PROJECT:DATASET.TABLE
CUSTOMER_TABLE = f"{PROJECT_ID}:{DATASET_ID}.customers"

TEMP_LOCATION = f"{RAW_BUCKET}/dataflow-temp/"


# ============================================================
# BIGQUERY SCHEMA
# ============================================================

CUSTOMER_SCHEMA = {
    "fields": [
        {
            "name": "customer_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "first_name",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "last_name",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "email",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "phone",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "city",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "country",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "signup_date",
            "type": "DATE",
            "mode": "NULLABLE",
        },
        {
            "name": "customer_segment",
            "type": "STRING",
            "mode": "NULLABLE",
        },
    ]
}


# ============================================================
# JSON PARSING
# ============================================================

class ParseCustomerJson(beam.DoFn):

    def process(self, text):

        try:
            records = json.loads(text)

            if not isinstance(records, list):
                raise ValueError("Expected JSON array")

            for record in records:
                yield record

        except Exception as e:

            print(
                f"JSON parsing error: {e}"
            )


# ============================================================
# DATA CLEANING AND VALIDATION
# ============================================================

class CleanCustomer(beam.DoFn):

    def process(self, record):

        try:
            # ------------------------------------------------
            # Required / string fields
            # ------------------------------------------------

            customer_id = str(
                record.get("customer_id", "")
            ).strip()

            first_name = str(
                record.get("first_name", "")
            ).strip()

            last_name = str(
                record.get("last_name", "")
            ).strip()

            email = str(
                record.get("email", "")
            ).strip()

            phone = str(
                record.get("phone", "")
            ).strip()

            city = str(
                record.get("city", "")
            ).strip()

            country = str(
                record.get("country", "")
            ).strip()

            customer_segment = str(
                record.get("customer_segment", "")
            ).strip()

            # ------------------------------------------------
            # Required field validation
            # ------------------------------------------------

            if not customer_id:
                return

            # ------------------------------------------------
            # Signup date
            #
            # Source format:
            # YYYY-MM-DD
            #
            # BigQuery target:
            # DATE
            # ------------------------------------------------

            signup_date = str(
                record.get("signup_date", "")
            ).strip()

            if signup_date:
                # Validate ISO date format
                from datetime import datetime

                signup_date = datetime.strptime(
                    signup_date,
                    "%Y-%m-%d",
                ).date().isoformat()

            else:
                signup_date = None

            # ------------------------------------------------
            # Output cleaned record
            # ------------------------------------------------

            yield {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "city": city,
                "country": country,
                "signup_date": signup_date,
                "customer_segment": customer_segment,
            }

        except (TypeError, ValueError) as e:

            print(
                f"Invalid customer record: "
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
            # Read raw Customers JSON from Cloud Storage
            # ------------------------------------------------
            | "Read Customers JSON"
            >> ReadFromText(
                CUSTOMERS_INPUT
            )

            # ------------------------------------------------
            # Source JSON is one complete JSON array.
            # Combine all lines before json.loads().
            # ------------------------------------------------
            | "Combine JSON Lines"
            >> beam.CombineGlobally(
                lambda lines: "".join(lines)
            )

            # ------------------------------------------------
            # Parse JSON array
            # ------------------------------------------------
            | "Parse Customer JSON"
            >> beam.ParDo(
                ParseCustomerJson()
            )

            # ------------------------------------------------
            # Clean and validate customers
            # ------------------------------------------------
            | "Clean and Validate Customers"
            >> beam.ParDo(
                CleanCustomer()
            )

            # ------------------------------------------------
            # Write to BigQuery staging
            # ------------------------------------------------
            | "Write Customers to BigQuery"
            >> WriteToBigQuery(
                table=CUSTOMER_TABLE,
                schema=CUSTOMER_SCHEMA,
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