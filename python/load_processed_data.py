import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import traceback
import psycopg2


# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Paths
# ----------------------------
RAW_DATA_DIR = Path("data_base/raw_data")
PROCESSED_DATA_DIR = Path("data_base/processed_data")

# ----------------------------
# Environment + DB
# ----------------------------


def load_env():
    load_dotenv()
    logging.info("Environment variables loaded.")


def get_db_url():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env file")
    logging.info("Database URL retrieved.")
    return db_url

# ----------------------------
# Data Loading
# ----------------------------


def create_data_dir():
    if not PROCESSED_DATA_DIR.exists():
        PROCESSED_DATA_DIR.mkdir()
        logging.info("Data directory created.")
    else:
        logging.info("Data directory already exists.")


def load_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path.resolve()}")
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {file_name} with shape {df.shape}")
    return df

# ----------------------------
# Cleaning Functions
# ----------------------------


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customer dataset."""
    logging.info(f"Cleaning customers: {df.shape[0]:,} rows")

    clean_customers = df.drop_duplicates(subset="customer_id").copy()
    clean_customers["country"] = "Brazil"
    clean_customers.columns = ["customer_id", "unique_id", "zip_code",
                               "city", "state", "country"]
    logging.info(f"Cleaned customers: {clean_customers.shape[0]:,} rows")
    return clean_customers


def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sellers dataset."""
    logging.info(f"Cleaning sellers: {df.shape[0]:,} rows")

    clean_merchants = df.drop_duplicates(subset="seller_id").copy()
    clean_merchants["country"] = "Brazil"
    clean_merchants.columns = ["merchant_id", "zip_code",
                               "city", "state", "country"]
    logging.info(f"Cleaned merchants: {clean_merchants.shape[0]:,} rows")
    return clean_merchants


def merge_transactions(orders: pd.DataFrame, items: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    """Cleaning Txn Data: Merge orders, items, and payments into transactions."""
    logging.info(
        f"Merging transactions: orders={orders.shape[0]:,}, items={items.shape[0]:,}, payments={payments.shape[0]:,}")

    clean_orders = orders.drop_duplicates(subset="order_id").copy()
    transactions = (
        clean_orders
        .merge(items[["order_id", "seller_id", "price", "product_id"]], on="order_id", how="left")
        .merge(payments[["order_id", "payment_type", "payment_value"]], on="order_id", how="left")
    )
    logging.info(f"Merged transactions: {transactions.shape[0]:,} rows")
    return transactions


def convert_date_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Convert columns to datetime, ignoring missing columns."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            logging.info(f"Converted column to datetime: {col}")
        else:
            logging.warning(
                f"Column not found, skipping datetime conversion: {col}")
    return df


# ----------------------------
# Save Data
# ----------------------------
def save_dataframe(df: pd.DataFrame, path: Path, format: str = "csv"):
    """Save DataFrame to CSV or Parquet."""
    if format == "csv":
        df.to_csv(path.with_suffix(".csv"), index=False)
        logging.info(f"Saved CSV: {path.with_suffix('.csv')}")
    elif format == "parquet":
        df.to_parquet(path.with_suffix(".parquet"), index=False)
        logging.info(f"Saved Parquet: {path.with_suffix('.parquet')}")
    else:
        logging.error(f"Unsupported format: {format}")
        raise ValueError(f"Unsupported format: {format}")


# ----------------------------
# Load to PostgreSQL
# ----------------------------
def load_to_postgres(df, table_name, engine):
    df.to_sql(table_name, engine, schema="operationals",
              if_exists="replace", index=False)
    logging.info(f"Loaded {table_name} into PostgreSQL.")

# ----------------------------
# ETL Pipeline
# ----------------------------


def run_etl():

    try:
        logging.info("Starting ETL pipeline...")

        create_data_dir()

        load_env()
        db_url = get_db_url()
        engine = create_engine(db_url)

        # Load raw data
        customers = load_raw_data("olist_customers_dataset.csv")
        merchants = load_raw_data("olist_sellers_dataset.csv")
        items = load_raw_data("olist_order_items_dataset.csv")
        orders = load_raw_data("olist_orders_dataset.csv")
        payments = load_raw_data("olist_order_payments_dataset.csv")

        # Clean
        clean_cust = clean_customers(customers)
        clean_merchants = clean_sellers(merchants)

        # Merge
        transactions = merge_transactions(orders, items, payments)

        # Convert dates
        date_cols = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        transactions = convert_date_columns(transactions, date_cols)

        # Save processed data
        save_dataframe(clean_cust, PROCESSED_DATA_DIR / "clean_customers")
        save_dataframe(clean_merchants, PROCESSED_DATA_DIR / "clean_merchants")
        save_dataframe(transactions, PROCESSED_DATA_DIR / "transactions")

        # Load into PostgreSQL
        load_to_postgres(clean_cust, "customers_raw", engine)
        load_to_postgres(clean_merchants, "merchants_raw", engine)
        load_to_postgres(transactions, "transactions_raw", engine)

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())


# ----------------------------
# Entry Point
# ----------------------------
if __name__ == "__main__":
    run_etl()
