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

PRODUCTS_INPUT = f"{RAW_BUCKET}/products/products.csv"

# Fully qualified BigQuery table:
# PROJECT:DATASET.TABLE
PRODUCT_TABLE = f"{PROJECT_ID}:{DATASET_ID}.products"

TEMP_LOCATION = f"{RAW_BUCKET}/dataflow-temp/"


# ============================================================
# BIGQUERY SCHEMA
# ============================================================

PRODUCT_SCHEMA = {
    "fields": [
        {
            "name": "product_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "product_name",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "category",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "subcategory",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "brand",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "unit_price",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "cost_price",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "supplier_id",
            "type": "STRING",
            "mode": "NULLABLE",
        },
    ]
}


# ============================================================
# CSV PARSING
# ============================================================

class ParseProductCsv(beam.DoFn):

    def process(self, row):

        try:
            product_id = str(
                row.get("product_id", "")
            ).strip()

            product_name = str(
                row.get("product_name", "")
            ).strip()

            category = str(
                row.get("category", "")
            ).strip()

            subcategory = str(
                row.get("subcategory", "")
            ).strip()

            brand = str(
                row.get("brand", "")
            ).strip()

            unit_price = float(
                row.get("unit_price")
            )

            cost_price = float(
                row.get("cost_price")
            )

            supplier_id = str(
                row.get("supplier_id", "")
            ).strip()

            # Required field validation
            if not product_id:
                return

            yield {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "unit_price": unit_price,
                "cost_price": cost_price,
                "supplier_id": supplier_id,
            }

        except (TypeError, ValueError) as e:

            print(
                f"Invalid product record: "
                f"{row} | Error: {e}"
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
            # Read products CSV from Cloud Storage
            # ------------------------------------------------
            | "Read Products CSV"
            >> ReadFromText(
                PRODUCTS_INPUT,
                skip_header_lines=1
            )

            # ------------------------------------------------
            # Convert CSV line into dictionary
            # ------------------------------------------------
            | "Parse CSV"
            >> beam.Map(
                lambda line: {
                    "product_id": line.split(",")[0].strip(),
                    "product_name": line.split(",")[1].strip(),
                    "category": line.split(",")[2].strip(),
                    "subcategory": line.split(",")[3].strip(),
                    "brand": line.split(",")[4].strip(),
                    "unit_price": line.split(",")[5].strip(),
                    "cost_price": line.split(",")[6].strip(),
                    "supplier_id": line.split(",")[7].strip(),
                }
            )

            # ------------------------------------------------
            # Clean and validate products
            # ------------------------------------------------
            | "Clean and Validate Products"
            >> beam.ParDo(
                ParseProductCsv()
            )

            # ------------------------------------------------
            # Write to BigQuery staging
            # ------------------------------------------------
            | "Write Products to BigQuery"
            >> WriteToBigQuery(
                table=PRODUCT_TABLE,
                schema=PRODUCT_SCHEMA,
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