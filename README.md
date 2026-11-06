# AtliQ Commerce – End-to-End Data Engineering Project

## Project Overview
This project implements an end-to-end data engineering solution for AtliQ Commerce, covering data ingestion, transformation, data quality, and analytics.

## Technology Stack
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Databricks
- PySpark
- Delta Lake
- dbt
- Microsoft Fabric
- Power BI
- GitHub Actions

## Architecture

The solution follows a layered data architecture:

1. **Bronze:** Ingest raw data from source systems into ADLS Gen2.
2. **Silver:** Clean, standardize, deduplicate, and validate data using PySpark and Databricks.
3. **Gold:** Build analytical models using dbt, including dimensions and fact tables.
4. **Reporting:** Connect the Gold layer to Power BI for analytics and reporting.

## Key Data Sources
- Customers
- Products
- Orders
- Order Items
- Payments
- Supplier Price List
- Clickstream Events

## Gold Models
- `dim_date`
- `dim_customer`
- `dim_product`
- `fact_sales`
- `fact_web_sessions`

## Data Quality and Reliability
- dbt tests and build validation
- Data quality logging
- Pipeline execution audit tracking
- GitHub Actions CI for pull requests

## Repository Structure
- `adf/` – Azure Data Factory pipeline definitions
- `notebooks/` – Databricks PySpark notebooks
- `dbt/` – dbt project, models, tests, and configuration
- `sql/` – SQL scripts
- `.github/workflows/` – GitHub Actions workflows

## Execution
Refer to the project implementation guide and individual folder documentation for setup and execution instructions.
