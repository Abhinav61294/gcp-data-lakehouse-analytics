import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io
from datetime import datetime


# ============================================================
# GCP CONFIGURATION
# ============================================================

PROJECT_ID = "gcp-data-lakehouse-analytics"
REGION = "asia-south1"

DATASET_ID = "staging"

RAW_BUCKET = "gs://gcp-data-lakehouse-analytics-raw"

ORDERS_INPUT = f"{RAW_BUCKET}/orders/orders.csv"

# Fully qualified BigQuery table:
# PROJECT:DATASET.TABLE
ORDER_TABLE = f"{PROJECT_ID}:{DATASET_ID}.orders"

TEMP_LOCATION = f"{RAW_BUCKET}/dataflow-temp/"


# ============================================================
# BIGQUERY SCHEMA
# ============================================================

ORDER_SCHEMA = {
    "fields": [
        {
            "name": "order_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "customer_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "product_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "location_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "order_date",
            "type": "DATE",
            "mode": "REQUIRED",
        },
        {
            "name": "quantity",
            "type": "INTEGER",
            "mode": "NULLABLE",
        },
        {
            "name": "unit_price",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "discount",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "payment_method",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "order_status",
            "type": "STRING",
            "mode": "NULLABLE",
        },
    ]
}


# ============================================================
# CSV PARSING + CLEANING
# ============================================================

class ParseAndCleanOrder(beam.DoFn):

    def process(self, line):

        try:
            # Parse CSV safely rather than splitting manually.
            reader = csv.DictReader(
                io.StringIO(line),
                fieldnames=[
                    "order_id",
                    "customer_id",
                    "product_id",
                    "location_id",
                    "order_date",
                    "quantity",
                    "unit_price",
                    "discount",
                    "payment_method",
                    "order_status",
                ],
            )

            record = next(reader)

            # -----------------------------------------------
            # Required string fields
            # -----------------------------------------------

            order_id = str(
                record.get("order_id", "")
            ).strip()

            customer_id = str(
                record.get("customer_id", "")
            ).strip()

            product_id = str(
                record.get("product_id", "")
            ).strip()

            location_id = str(
                record.get("location_id", "")
            ).strip()

            if not order_id:
                return

            if not customer_id:
                return

            if not product_id:
                return

            if not location_id:
                return

            # -----------------------------------------------
            # Order date
            #
            # Source contains values such as:
            # 2026-09-22 00:00:00
            #
            # BigQuery staging uses DATE.
            # -----------------------------------------------

            raw_order_date = str(
                record.get("order_date", "")
            ).strip()

            order_date = datetime.strptime(
                raw_order_date,
                "%Y-%m-%d %H:%M:%S",
            ).date().isoformat()

            # -----------------------------------------------
            # Numeric fields
            # -----------------------------------------------

            quantity = int(
                record.get("quantity")
            )

            unit_price = float(
                record.get("unit_price")
            )

            discount = float(
                record.get("discount")
            )

            # -----------------------------------------------
            # Remaining fields
            # -----------------------------------------------

            payment_method = str(
                record.get("payment_method", "")
            ).strip()

            order_status = str(
                record.get("order_status", "")
            ).strip()

            # -----------------------------------------------
            # Output cleaned record
            # -----------------------------------------------

            yield {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "location_id": location_id,
                "order_date": order_date,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount,
                "payment_method": payment_method,
                "order_status": order_status,
            }

        except (TypeError, ValueError, StopIteration) as e:

            print(
                f"Invalid order record: "
                f"{line} | Error: {e}"
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
            # Read raw Orders CSV from Cloud Storage
            # ------------------------------------------------
            | "Read Orders CSV"
            >> ReadFromText(
                ORDERS_INPUT,
                skip_header_lines=1,
            )

            # ------------------------------------------------
            # Parse, validate and clean orders
            # ------------------------------------------------
            | "Parse and Clean Orders"
            >> beam.ParDo(
                ParseAndCleanOrder()
            )

            # ------------------------------------------------
            # Write cleaned orders to BigQuery
            # ------------------------------------------------
            | "Write Orders to BigQuery"
            >> WriteToBigQuery(
                table=ORDER_TABLE,
                schema=ORDER_SCHEMA,
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