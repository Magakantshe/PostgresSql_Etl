# Orders Data Pipeline

This project implements a simple data pipeline that ingests messy order data, cleans and validates it, loads it into PostgreSQL, and exposes analytics through SQL views.

The pipeline is implemented in **Python using pandas and psycopg**.

---

## Features

The pipeline performs the following tasks:

- Data ingestion from multiple file formats
- Data cleaning and validation
- Loading data into PostgreSQL using COPY
- Creating analytics SQL views
- Generating a markdown report

---

## Project Structure
orders-data-pipeline/
│
├── main.py
├── config.yaml
├── requirements.txt
│
├── data/
│   ├── customers.csv
│   ├── orders.jsonl
│   └── order_items.csv
│
│
├── README.md
└── SOLUTION.md


---

## Requirements

Install dependencies:


Dependencies include:

- pandas
- psycopg
- python-dotenv
- tabulate

---

## Database Configuration

Create a `.env` file in the project root:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=orders_db
DB_USER=postgres
DB_PASSWORD=postgres

DATA_PATH=data


---

## Running the Pipeline

Run the full pipeline:

python main.py


The pipeline performs the following steps:

1. Create database tables and SQL views
2. Extract raw data files
3. Clean and validate the data
4. Load data into PostgreSQL
5. Generate an analytics report

---

## Database Tables

### customers

Stores customer information.

### orders

Stores order transactions.

### order_items

Stores product line items for each order.

---

## Analytics Views

The pipeline creates the following views:

### daily_metrics
Daily order metrics including revenue and order count.

### top_customers
Top customers ranked by lifetime spend.

### top_skus
Best selling products by revenue.

---

## Report

After the pipeline runs, a `REPORT.md` file is generated summarizing analytics results.

---

## Author

Magakantshe