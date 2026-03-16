# Import required libraries
import pandas as pd
import psycopg
import logging
import os
import time
from io import StringIO
from pathlib import Path
from dotenv import load_dotenv



# Load environment variables from .env

load_dotenv()



# Configure logging to show pipeline steps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)



# Database connection parameters
# These are loaded from the .env file

DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}



# Data file locations
# The pipeline expects these files to exist. If not, it will create sample data for demonstration.
PROJECT_DIR = Path(__file__).resolve().parent
DATA_PATH = Path(os.getenv("DATA_PATH", PROJECT_DIR / "data"))

CUSTOMERS_FILE = DATA_PATH / "customers.csv"
ORDERS_FILE = DATA_PATH / "orders.jsonl"
ITEMS_FILE = DATA_PATH / "order_items.csv"

# Ensure data files exist or create sample data
def ensure_data_files():
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    if not CUSTOMERS_FILE.exists():
        logger.info("Creating sample customers.csv")
        CUSTOMERS_FILE.write_text("customer_id,email,full_name,signup_date,country_code,is_active\n1,alice@example.com,Alice Smith,2025-01-01,US,true\n")
    if not ORDERS_FILE.exists():
        logger.info("Creating sample orders.jsonl")
        ORDERS_FILE.write_text("{\"order_id\":1001,\"customer_id\":1,\"order_ts\":\"2025-02-01T12:00:00Z\",\"status\":\"placed\",\"total_amount\":120.50,\"currency\":\"usd\"}\n")
    if not ITEMS_FILE.exists():
        logger.info("Creating sample order_items.csv")
        ITEMS_FILE.write_text("order_id,line_no,sku,quantity,unit_price,category\n1001,1,SKU-001,2,30.25,Tools\n")



# DATABASE INITIALIZATION
# Creates tables and analytical views

def init_db():

    logger.info("Creating database tables")

    # Open PostgreSQL connection
    with psycopg.connect(**DB_PARAMS) as conn:

        with conn.cursor() as cur:

            
            # Customers table
            
            cur.execute("""
            CREATE TABLE IF NOT EXISTS customers(
                customer_id INTEGER PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                full_name TEXT,
                signup_date DATE,
                country_code CHAR(2),
                is_active BOOLEAN
            );
            """)

            
            # Orders table
            
            cur.execute("""
            CREATE TABLE IF NOT EXISTS orders(
                order_id BIGINT PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_ts TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL,
                total_amount NUMERIC(12,2),
                currency CHAR(3),

                -- Foreign key ensures order belongs to a valid customer
                FOREIGN KEY(customer_id) REFERENCES customers(customer_id),

                -- Restrict order status values
                CHECK(status IN ('placed','shipped','cancelled','refunded'))
            );
            """)

            
            # Order Items table
            
            cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items(
                order_id BIGINT,
                line_no INTEGER,
                sku TEXT,
                quantity INTEGER,
                unit_price NUMERIC(12,2),
                category TEXT,

                -- Composite primary key (ensures unique line items per order)
                PRIMARY KEY(order_id,line_no),

                -- Ensure item belongs to a valid order
                FOREIGN KEY(order_id) REFERENCES orders(order_id),

                -- Data quality constraints
                CHECK(quantity > 0),
                CHECK(unit_price > 0)
            );
            """)

            # Create SQL analytical views
            create_views(cur)

        conn.commit()

    logger.info("Database initialized successfully")



# SQL ANALYTICS VIEWS

def create_views(cur):

    
    # Daily revenue metrics
    
    cur.execute("""
    CREATE OR REPLACE VIEW daily_metrics AS
    SELECT
        DATE(order_ts) AS date,
        COUNT(*) AS orders_count,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS average_order_value
    FROM orders
    GROUP BY DATE(order_ts)
    ORDER BY date;
    """)

    
    # Top spending customers
    
    cur.execute("""
    CREATE OR REPLACE VIEW top_customers AS
    SELECT
        c.customer_id,
        c.email,
        c.full_name,
        SUM(o.total_amount) AS lifetime_spend
    FROM customers c
    JOIN orders o USING(customer_id)
    GROUP BY c.customer_id,c.email,c.full_name
    ORDER BY lifetime_spend DESC
    LIMIT 10;
    """)

    
    # Best selling products
    
    cur.execute("""
    CREATE OR REPLACE VIEW top_skus AS
    SELECT
        sku,
        SUM(quantity) AS units_sold,
        SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY sku
    ORDER BY revenue DESC
    LIMIT 10;
    """)



# DATA CLEANING FUNCTIONS



# Clean customers dataset

def clean_customers(df):

    # Normalize emails
    df["email"] = df["email"].str.lower()

    # Remove invalid emails
    df = df[df["email"].str.contains("@", na=False)]

    # Convert signup date
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    # Remove rows with invalid dates
    df = df.dropna(subset=["signup_date"])

    # Standardize country codes
    df["country_code"] = df["country_code"].str.upper()

    # Convert active column to boolean
    df["is_active"] = df["is_active"].astype(str).str.lower().map({
        "true": True,
        "false": False
    }).fillna(False)
    print(df["is_active"])
    # Remove duplicate emails keeping earliest signup
    df = df.sort_values("signup_date").drop_duplicates("email")

    return df



# Clean orders dataset

def clean_orders(df, valid_customers):

    # Convert timestamp
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True, errors="coerce")

    df = df.dropna(subset=["order_ts"])

    # Normalize order status
    df["status"] = df["status"].str.lower()

    valid_status = {"placed","shipped","cancelled","refunded"}

    df = df[df["status"].isin(valid_status)]

    # Remove orders with missing customers
    df = df[df["customer_id"].isin(valid_customers)]

    # Ensure numeric amounts
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

    df = df.dropna(subset=["total_amount"])

    # Normalize currency
    df["currency"] = df["currency"].str.upper()

    return df



# Clean order items dataset

def clean_items(df, valid_orders):

    # Remove invalid quantity or price
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]

    # Remove items with missing orders
    df = df[df["order_id"].isin(valid_orders)]

    # Remove duplicate line items
    df = df.drop_duplicates(["order_id","line_no"])

    return df



# FAST DATA LOADING USING POSTGRES COPY

def copy_dataframe(conn, df, table):

    # Convert dataframe to CSV buffer
    buffer = StringIO() # In-memory text stream
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)   

    # COPY command loads data much faster than inserts
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} FROM STDIN WITH CSV") as copy: # Open a COPY stream
            copy.write(buffer.read())



# MAIN ETL PIPELINE

def run_etl():

    logger.info("Starting ETL pipeline")

    start = time.time()

    # Ensure data inputs exist
    ensure_data_files()

    # Extract data
    customers = pd.read_csv(CUSTOMERS_FILE)
    orders = pd.read_json(ORDERS_FILE, lines=True)
    items = pd.read_csv(ITEMS_FILE)

    # Transform (clean datasets)
    customers = clean_customers(customers)
    orders = clean_orders(orders, set(customers.customer_id))
    items = clean_items(items, set(orders.order_id))

    # Load data into PostgreSQL
    with psycopg.connect(**DB_PARAMS) as conn:

        with conn.cursor() as cur:

            # Clear previous data
            cur.execute("TRUNCATE order_items,orders,customers CASCADE")

        copy_dataframe(conn, customers, "customers")
        copy_dataframe(conn, orders, "orders")
        copy_dataframe(conn, items, "order_items")

        conn.commit()

    logger.info(f"ETL completed in {time.time() - start:.2f} seconds")



# GENERATE MARKDOWN REPORT

def generate_report():

    logger.info("Generating report")
    # Query analytical views and format as markdown tables
    with psycopg.connect(**DB_PARAMS) as conn:

        daily = pd.read_sql("SELECT * FROM daily_metrics", conn)
        top_customers = pd.read_sql("SELECT * FROM top_customers", conn)
        top_skus = pd.read_sql("SELECT * FROM top_skus", conn)

    report = []

    report.append("# Data Pipeline Report\n")

    report.append("## Daily Metrics\n")
    report.append(daily.to_markdown(index=False))

    report.append("\n## Top Customers\n")
    report.append(top_customers.to_markdown(index=False))

    report.append("\n## Top SKUs\n")
    report.append(top_skus.to_markdown(index=False))

    with open("REPORT.md","w") as f:
        f.write("\n".join(report))

    logger.info("REPORT.md generated successfully")



# MAIN PROGRAM ENTRY POINT

if __name__ == "__main__":

    # Run entire pipeline automatically
    init_db()
    run_etl()
    generate_report()

    print("Pipeline completed successfully")