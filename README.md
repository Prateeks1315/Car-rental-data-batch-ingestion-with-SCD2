# Car Rental Data Batch Ingestion with SCD Type 2 Implementation

This project demonstrates a batch data ingestion pipeline for car rental data using Slowly Changing Dimension Type 2 (SCD2) methodology. The pipeline is designed to efficiently capture historical changes in data while preserving data integrity for accurate analysis.

## 🎯 Objective

- To build a robust ETL pipeline that ingests batch car rental data.
- Implement SCD Type 2 logic to track historical changes in rental records.
- Load processed data into a data warehouse (e.g., Snowflake, Redshift, or any RDBMS).
- Enable analytics on historical car rental trends and customer behavior.

## 🧰 Tools & Technologies Used

- Python (or SQL-based ETL tools)
- Snowflake / Redshift / PostgreSQL (or your target data warehouse)
- Apache Airflow (optional for orchestration)
- SQL for SCD2 logic and data transformations
- Pandas (for data manipulation, if applicable)

## 🧩 Project Structure
Car-Rental-Data-Batch-Ingestion/
│
├── etl_script.py                  # Python script for batch ingestion and SCD2 logic
├── scd2_sql_queries.sql           # SQL scripts to implement SCD2 on the target table
├── sample_data/                   # Folder containing sample CSV/JSON input files
├── README.md                     # Project documentation
└── requirements.txt              # Python dependencies (if any)

Workflow Overview
Batch Data Ingestion:
Load the latest batch car rental data from CSV/JSON or source system.

SCD Type 2 Implementation:
Detect changes between the incoming batch and existing data.
Insert new records for new rentals.
Close out old records that have changed (set end date, update current flag).
Insert updated records with new information while preserving history.

Data Loading:
Load transformed data into the data warehouse table.
Ensure data consistency and audit logging.

Understanding and implementing Slowly Changing Dimensions Type 2 (SCD2).
Batch ingestion best practices.
Data quality and incremental update handling.
Use of SQL for complex data transformations.

