# Solution Explanation

This document explains the design decisions and implementation of the data pipeline.

---

# Pipeline Overview

The pipeline follows a simple ETL architecture:

1. Extract raw data from CSV and JSON files
2. Transform and clean the data using pandas
3. Load the cleaned data into PostgreSQL
4. Expose analytics through SQL views
5. Generate a markdown report

---

# Data Sources

The pipeline processes three input datasets:

### customers.csv

Customer information including:

- email
- signup date
- country code
- active status

### orders.jsonl

Order transaction records.

Each line represents one JSON object.

### order_items.csv

Product line items belonging to orders.

---

# Data Cleaning

Data cleaning is implemented in dedicated functions.

## Customers

The following cleaning steps are applied:

- Emails are normalized to lowercase
- Invalid email addresses are removed
- Signup dates are parsed and validated
- Country codes are standardized to uppercase
- Duplicate emails are removed

---

## Orders

Orders are cleaned using the following rules:

- Timestamps are converted to UTC
- Invalid order statuses are removed
- Orders referencing missing customers are filtered
- Total amounts are validated as numeric values
- Currency codes are normalized

---

## Order Items

Cleaning includes:

- Removing rows with invalid quantity or price
- Removing items referencing missing orders
- Removing duplicate order lines

---

# Data Loading

Data is loaded using the PostgreSQL `COPY` command.

COPY is significantly faster than inserting rows individually and is commonly used in production pipelines.

---

# Database Schema

The database contains three tables:

### customers
Stores customer data.

### orders
Stores order transactions and references customers.

### order_items
Stores individual products within orders.

Primary keys, foreign keys, and check constraints enforce data integrity.

---

# Analytics Views

SQL views are created to provide analytics:

### daily_metrics

Shows daily revenue, order counts, and average order value.

### top_customers

Lists the highest spending customers.

### top_skus

Identifies the best selling products by revenue.

---

# Report Generation

The pipeline generates a markdown report (`REPORT.md`) summarizing:

- daily metrics
- top customers
- top SKUs

The report is generated using pandas and SQL queries.

---

# Assumptions

The following assumptions were made:

- Emails must contain '@'
- Valid order statuses are: placed, shipped, cancelled, refunded
- Quantities and prices must be positive
- Orders must reference valid customers
- Order items must reference valid orders

---

# Possible Improvements

In a production environment the pipeline could be extended with:

- workflow orchestration (Airflow)
- automated data validation
- incremental loading
- monitoring and alerting
- partitioned database tables

---

# Conclusion

This pipeline demonstrates a clean and maintainable ETL workflow using Python and PostgreSQL, with a focus on data quality, performance, and analytical usability.
