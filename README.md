# GCP Data Lakehouse & Analytics Platform

An end-to-end GCP data engineering platform that ingests data from **CSV files, PostgreSQL, and REST APIs**, processes it using **Apache Beam and Google Cloud Dataflow**, transforms it with **Dataform**, and provides business analytics through **Looker Studio**.

---

## Architecture

    CSV Files ──────────────┐
                            │
    PostgreSQL ─────────────┼──> Python Ingestion
                            │
    REST APIs ──────────────┘
                                  │
                                  ▼
                         Google Cloud Storage
                              RAW / BRONZE
                                  │
                                  ▼
                         Apache Beam / Dataflow
                       ┌──────────┼──────────┐
                       │          │          │
                     Clean    Transform   Validate
                       │          │          │
                       └──────────┼──────────┘
                                  │
                                  ▼
                              BigQuery
                               STAGING
                                  │
                                  ▼
                               Dataform
                       ┌──────────┼──────────┐
                       │          │          │
                  Dimensions     Fact      Data Quality
                                Table        Checks
                       │          │
                       └──────────┼──────────┘
                                  │
                                  ▼
                           GOLD / MARTS
                       ┌──────────┼──────────┐
                       │          │          │
                    Sales      Product     Customer
                     Mart     Performance  Performance
                       │
                       └──────────┬──────────┘
                                  │
                                  ▼
                           Looker Studio
                        Interactive Dashboard

---

## Tech Stack

- **Google Cloud Storage** – Raw / Bronze data lake
- **Apache Beam** – Data processing
- **Google Cloud Dataflow** – Managed Beam execution
- **BigQuery** – Data warehouse
- **Dataform** – SQL transformation and data modeling
- **Looker Studio** – Business intelligence and visualization
- **Python** – Data generation, ingestion and validation
- **PostgreSQL** – Operational order source
- **REST APIs** – Customer and location sources
- **Git / GitHub** – Version control

---

## Data Sources

The platform integrates four datasets representing a global retail business.

| Source | Dataset | Records |
|---|---|---:|
| CSV | Products | 10,000 |
| PostgreSQL | Orders | 100,000 |
| REST API | Customers | 20,000 |
| REST API | Locations | 500 |
| **Total** | | **130,500** |

### Products

CSV-based product master data containing:

- Product ID
- Product name
- Category
- Subcategory
- Brand
- Unit price
- Cost price
- Supplier ID

### Orders

PostgreSQL transactional order data containing:

- Order ID
- Customer ID
- Product ID
- Location ID
- Order date
- Quantity
- Unit price
- Discount
- Payment method
- Order status

### Customers

REST API containing:

- Customer ID
- Name
- Email
- Phone
- City
- Country
- Signup date
- Customer segment

### Locations

REST API containing:

- Location ID
- City
- State
- Country
- Region
- Latitude
- Longitude

---

## Pipeline

### 1. Python Ingestion

Python is used to extract and validate data from the different source systems.

The ingestion layer performs:

- Source extraction
- Basic validation
- Schema validation
- Ingestion metadata handling

---

### 2. Cloud Storage — Raw / Bronze

Raw source data is stored in Google Cloud Storage before transformation.

    gs://gcp-data-lakehouse-analytics-raw/

    ├── customers/
    │   └── customers.json
    │
    ├── locations/
    │   └── locations.json
    │
    ├── orders/
    │   └── orders.csv
    │
    └── products/
        └── products.csv

The Raw layer preserves the source data before downstream processing.

---

### 3. Apache Beam / Dataflow

Apache Beam pipelines process the raw data from Cloud Storage.

Processing includes:

- Data cleaning
- Schema enforcement
- Data type conversion
- Validation
- Deduplication
- Transformation
- Enrichment

The pipelines were tested locally using Apache Beam and then executed on Google Cloud Dataflow.

All four pipelines successfully processed:

**130,500 records**

---

### 4. BigQuery Staging

Processed data is loaded into the BigQuery `staging` dataset.

| Table | Records |
|---|---:|
| products | 10,000 |
| customers | 20,000 |
| locations | 500 |
| orders | 100,000 |
| **Total** | **130,500** |

---

## Dataform Transformation

Dataform is used to transform the staging layer into an analytical dimensional model.

### Star Schema

    dim_product
         │
         │
    dim_customer ───── fact_orders ───── dim_location
                            │
                            │
                        dim_date

### Dimension Tables

- `dim_product`
- `dim_customer`
- `dim_location`
- `dim_date`

### Fact Table

- `fact_orders`

---

## Business Metrics

The fact model calculates important business metrics.

### Net Sales

    quantity × unit_price × (1 - discount / 100)

### Total Cost

    quantity × cost_price

### Gross Profit

    net_sales - total_cost

### Gross Margin

    gross_profit / net_sales × 100

---

## Incremental Processing

An incremental Dataform model was implemented for order processing.

The model checks existing `order_id` values and processes only new orders.

    New Orders
        │
        ▼
    Incremental Dataform Model
        │
        ├── Existing Order → Skip
        │
        └── New Order → Process

Verified:

- 100,000 orders
- 100,000 unique order IDs

---

## SCD Type 2

A customer dimension was designed using an SCD Type 2 structure.

The model contains:

- `customer_id`
- `valid_from`
- `valid_to`
- `is_current`

This provides the structure required to maintain historical versions of customer records.

> The current implementation establishes the SCD Type 2 structure and current-state snapshot.

---

## Data Quality

Data quality checks were implemented at multiple stages.

### Source Validation

Validation checks include:

- Missing IDs
- Invalid emails
- Invalid dates
- Required fields
- Invalid records

All source validation tests passed.

### Dataform Assertions

Dataform assertions check for:

- Null order IDs
- Invalid order IDs
- Invalid quantities
- Negative net sales
- Duplicate order IDs

---

## Gold / Business Marts

Dataform creates business-ready marts for analytics.

### Daily Sales

`mart_daily_sales`

Contains:

- Completed orders
- Units sold
- Total sales
- Total cost
- Gross profit
- Gross margin %

Only completed orders are treated as realized sales.

### Product Performance

`mart_product_performance`

Contains:

- Product performance
- Category performance
- Subcategory performance
- Brand performance
- Total sales
- Gross profit
- Gross margin
- Average selling price

### Customer Performance

`mart_customer_performance`

Contains:

- Completed orders
- Units purchased
- Total spend
- Total cost
- Gross profit
- Gross margin
- First order date
- Last order date

### Geographic Performance

`mart_geographic_performance`

Contains:

- Country
- Region
- City
- Completed orders
- Units sold
- Total sales
- Total cost
- Gross profit
- Gross margin

---

## Looker Studio Dashboard

The Gold marts are connected to Looker Studio to create an interactive business dashboard.

### KPI Scorecards

| KPI | Visualization | Metric |
|---|---|---|
| Total Sales | KPI Scorecard | `SUM(total_sales)` |
| Gross Profit | KPI Scorecard | `SUM(gross_profit)` |
| Gross Margin % | KPI Scorecard | `SUM(gross_profit) / SUM(total_sales) × 100` |
| Completed Orders | KPI Scorecard | `SUM(completed_orders)` |
| Units Sold | KPI Scorecard | `SUM(units_sold)` |

### Sales Trend

**Visualization:** Time Series / Line Chart

**Dimension:**

- `order_date`

**Metrics:**

- `SUM(total_sales)`
- `SUM(gross_profit)`

### Category Performance

**Visualization:** Bar Chart

**Dimension:**

- `category`

**Metrics:**

- `SUM(total_sales)`
- `SUM(gross_profit)`

### Customer Performance

**Visualization:** Table

**Dimensions:**

- `customer_id`
- `first_name`
- `last_name`
- `customer_segment`

**Metrics:**

- `completed_orders`
- `total_spend`
- `gross_profit`
- `gross_margin_pct`

### Geographic Performance

**Visualization:** Geo Chart / Map

**Dimension:**

- `country`

**Metric:**

- `SUM(total_sales)`

**Supporting Table:**

- Country
- Region
- Completed Orders
- Units Sold
- Total Sales
- Gross Profit
- Gross Margin %

### Dashboard Filters

Interactive controls include:

- Date Range Control using `order_date`
- Category Drop-down Filter using `category`

---

## Data Quality Results

| Dataset | Records | Validation |
|---|---:|---|
| Products | 10,000 | Passed |
| Customers | 20,000 | Passed |
| Locations | 500 | Passed |
| Orders | 100,000 | Passed |
| **Total** | **130,500** | **Passed** |

---

## Key Engineering Challenges

### Dataflow Worker Availability

Some Dataflow jobs failed when specific worker zones were selected because of regional resource availability.

The pipelines were adjusted to use regional worker placement instead of forcing a specific zone.

### Windows Beam Environment

The local Windows environment encountered a `grpcio` / `cygrpc` Application Control issue while submitting Dataflow jobs.

A clean Python environment was created in Google Cloud Shell to successfully submit the affected pipeline.

### JSON Array Processing

Customer and location data was stored as JSON arrays rather than newline-delimited JSON.

The Beam pipelines therefore combined the input content before parsing the JSON arrays.

### Dataform External References

BigQuery staging tables were created outside the Dataform repository.

Explicit BigQuery table references were therefore used for staging sources, while Dataform `ref()` was used for Dataform-managed models.

---

## Project Structure

    gcp-data-lakehouse-analytics/
    │
    ├── airflow/
    ├── config/
    ├── data/
    ├── data_quality/
    ├── dataflow/
    ├── dataform/
    ├── docs/
    ├── ingestion/
    ├── sql/
    ├── tests/
    │
    ├── .gitignore
    └── README.md

> Note: The `airflow/` directory is retained as part of the project structure, but Airflow/Composer orchestration is not part of the implemented Project 3 pipeline.

---

## End-to-End Flow

    CSV
    PostgreSQL
    REST APIs
        │
        ▼
    Python Ingestion
        │
        ▼
    Cloud Storage
    RAW / BRONZE
        │
        ▼
    Apache Beam / Dataflow
        │
        ▼
    BigQuery STAGING
        │
        ▼
    Dataform
        │
        ├── Dimensions
        ├── Fact Tables
        ├── Incremental Processing
        ├── SCD Type 2
        └── Data Quality
        │
        ▼
    Gold / Business Marts
        │
        ▼
    Looker Studio

---

## Key Concepts Demonstrated

- Multi-source data ingestion
- REST API integration
- PostgreSQL integration
- CSV ingestion
- Cloud Storage Raw / Bronze layer
- Apache Beam
- Google Cloud Dataflow
- BigQuery
- Dimensional modeling
- Star schema
- Fact and dimension tables
- Incremental processing
- SCD Type 2 structure
- Data quality assertions
- Gold / business marts
- Business KPI modeling
- Looker Studio dashboards
- Git / GitHub

---

## Final Outcome

Built an end-to-end **GCP Data Lakehouse & Analytics Platform** processing **130,500 records** from CSV, PostgreSQL, and REST API sources.

The platform demonstrates a complete data engineering workflow from raw data ingestion through cloud processing, dimensional modeling, incremental transformations, data quality validation, business marts, and interactive BI analytics.

    130,500 Records
           │
           ▼
    Cloud Storage
           │
           ▼
    Apache Beam / Dataflow
           │
           ▼
    BigQuery
           │
           ▼
    Dataform
           │
           ▼
    Gold Business Marts
           │
           ▼
    Looker Studio
